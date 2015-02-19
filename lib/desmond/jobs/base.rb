module Desmond
  ##
  # base class for queueable jobs.
  # override 'run' method in subclasses to specify behavior.
  # template available in 'template.rb'.
  #
  class BaseJob < ::Que::Job
    include JobRunFinders
    attr_accessor :run_id

    ##
    # queue this job for execution
    #
    # see `run` for parameter documentation
    #
    # returns a JobRun instance
    #
    def self.enqueue(job_id, user_id, options={})
      job_run_id = create_job_run(job_id, user_id)
      # the run_id is passed in as an option, because in the synchronous job execution mode, the created job instance
      # is returned after the job was executed, so no JobRun instance would be accessible during execution of the job
      super(job_id, user_id, options.merge(_run_id: job_run_id))
      # requery from database, since it was already updated in sync mode
      Desmond::JobRun.find(job_run_id)
    end

    ##
    # run job synchronously
    #
    # see `run` for parameter documentation
    #
    # returns a JobRun instance
    #
    def self.run(job_id, user_id, options={})
      # self.enqueue will call this function in sync mode, so if the job run was already created, don't do it again
      job_run_id = options[:_run_id]
      job_run_id = create_job_run(job_id, user_id) if job_run_id.nil?
      # the run_id is passed in as an option, because in the synchronous job execution mode, the created job instance
      # is returned after the job was executed, so no JobRun instance would be accessible during execution of the job
      super(job_id, user_id, options.merge(_run_id: job_run_id))
      # requery from database, since it was already updated in sync mode
      Desmond::JobRun.find(job_run_id)
    end

    ##
    # this method will be called to execute this job.
    # needs to be overriden by specific job, making sure to call
    # this `super` method as the first statement.
    #
    # +job_id+: unique identifier for this type of job from calling application for later identification
    # +user_id+: unique identifier of the application's user running this export for later identification
    # +options+: depends on the implementation of the job
    #
    def run(job_id, user_id, options={})
      @censored_options = censor_hash_keys(options, CENSORED_KEYS)
      self.run_id = options[:_run_id] # retrieve run_id from options and safe it in the instance
      job_run.update(status: 'running', executed_at: Time.now)
      run_hook(:before)
      log_job_event(:info, "Starting to execute job")
      self.send :execute, job_id, user_id, options if self.respond_to?(:execute)
      self.done if job_run.running?
    rescue => e
      log_job_event(:error, "Error executing job")
      Que.log level: :error, exception: e.message
      Que.log level: :error, backtrace: e.backtrace.join("\n ")
      self.failed(error: e.message)
    ensure
      log_job_event(:info, "Finished executing job")
      # we always want to execute the after hook
      run_hook(:after)
    end

    ##
    # job is completed, but failed.
    # +details+ will be saved for this run in the database.
    #
    def failed(details={})
      details ||= {}
      delete_job(false, details)
      run_hook(:error) # special error hook when job fails
    end

    ##
    # job is completed and succeeded.
    # +details+ will be saved for this run in the database.
    #
    def done(details={})
      details ||= {}
      delete_job(true, details)
      run_hook(:success)
    end

    private

    ##
    # runs the hook with the given +name+
    # swallows all exceptions, only logging them
    #
    def run_hook(name)
      self.send name.to_sym, job_run, *self.attrs[:args] if self.respond_to?(name.to_sym)
    rescue Exception => e
      log_job_event(:error, "Error executing hook '#{name}' for job")
      Que.log level: :error, exception: e.message
      Que.log level: :error, backtrace: e.backtrace.join("\n ")
    end

    ##
    # create a job run with the given parameters,
    # returning its id.
    #
    def self.create_job_run(job_id, user_id)
      e = Desmond::JobRun.create(job_id: job_id, job_class: self.name, user_id: user_id, status: 'queued', queued_at: Time.now)
      e.id
    end

    ##
    # returns the JobRun for this instance of the job
    #
    def job_run
      Desmond::JobRun.find(self.run_id)
    end

    ##
    # deletes the job marking it as a success if parameter +success+ is true, a failure otherwise.
    # +details+ are saved with the job run as well.
    #
    def delete_job(success, details={})
      status = 'done'
      status = 'failed' unless success
      destroy if Que.mode != :sync # Que doesn't in the database in sync mode
      job_run.update(status: status, details: details, completed_at: Time.now)
    end

    def log_job_event(level, prefix_str)
      job_args = self.attrs[:args]
      msg = "#{prefix_str} #{self.class.name} with (#{job_args[0, job_args.size - 1].join(', ')}, ...)"
      Que.log level: level, msg: msg, options: @censored_options
    end
  end
end
