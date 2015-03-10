module Desmond
  ##
  # base class for queueable jobs.
  # implement 'execute' instance method in subclasses to specify behavior.
  # templates available in 'template_*.rb'.
  #
  # schedule for background processing using class method `enqueue`.
  # run immediately blocking using class method `run`.
  #
  class BaseJob < ::Que::Job
    include JobRunFinders
    attr_accessor :run_id
    attr_reader :job_id, :user_id

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
      # is returned after the job was executed, so no JobRun instance would be accessible during execution of the job.
      # the async option differentiates between enqueue in sync mode mode and self.run in sync mode
      super(job_id, user_id, options.merge(_run_id: job_run_id))
      # requery from database, since it was already updated in sync mode
      Desmond::JobRun.find(job_run_id)
    end

    ##
    # run job synchronously
    #
    # see instance method `run` for parameter documentation
    #
    # returns the return value of the job's execute method
    #
    # TODO shouldn't create a job run? polizei is gonna create a lot of them, ability to delete old one?
    def self.run(job_id, user_id, options={})
      # self.enqueue will call this function in sync mode, so if the job run was already created, don't do it again
      job_run_id = options[:_run_id]
      called_by_enqueue = true # enqueue can call us, in which case the behavior of this method will be slightly different
      if job_run_id.nil?
        # not already create by `enqueue`
        called_by_enqueue = false
        job_run_id = create_job_run(job_id, user_id)
      end
      # the run_id is passed in as an option, because in the synchronous job execution mode, the created job instance
      # is returned after the job was executed, so no JobRun instance would be accessible during execution of the job
      super(job_id, user_id, options.merge(_run_id: job_run_id))
      # requery from database, since it was already updated in sync mode
      jr = Desmond::JobRun.find(job_run_id)
      if jr.done?
        jr.result
      else
        # in sync mode, enqueue directly runs this method, where we don't want to throw an exception,
        # as this will not happen in async mode
        fail jr.error unless called_by_enqueue # only raise exception if not called by 'enqueue'
      end
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
      self.run_id = options.delete(:_run_id) # retrieve run_id from options and safe it in the instance
      @job_id = job_id
      @user_id = user_id
      @symbolized_options = options.deep_symbolize_keys
      @censored_options = censor_hash_keys(@symbolized_options, CENSORED_KEYS)
      jr = job_run # cache job run
      jr.update(status: 'running', executed_at: Time.now)
      run_hook(:before)
      log_job_event(:info, "Starting to execute job")
      result = self.send :execute, job_id, user_id, @symbolized_options if self.respond_to?(:execute)
      # check that we can actually persist the result
      check_result_type(result)
      jr = job_run # update cache (might have been changed by execute)
      # save result in job_run
      jr.update(details: { _job_result: result })
      self.done if jr.running? # mark as succeded if not done by the job
    rescue => e
      log_job_event(:error, "Error executing job")
      Que.log level: :error, exception: e.message
      Que.log level: :error, backtrace: e.backtrace.join("\n ")
      # requery job_run (might have been changed by execute) and svae error message
      self.failed(e.message)
    ensure
      log_job_event(:info, "Finished executing job")
      # we always want to execute the after hook
      jr = job_run
      run_hook(:success) if jr.done?
      run_hook(:error) if jr.failed?
      run_hook(:after)
      PGUtil.notify(ActiveRecord::Base.connection, "job_run_#{jr.id}")
    end

    ##
    # job is completed, but failed.
    #
    def failed(error)
      delete_job(false)
      job_run.update(details: { error: error })
    end

    ##
    # job is completed and succeeded.
    #
    def done
      delete_job(true)
    end

    ##
    # return the symbolized options the job was run with
    #
    def options
      @symbolized_options
    end

    private

    ##
    # runs the hook with the given +name+
    # swallows all exceptions, only logging them
    #
    def run_hook(name)
      self.send name.to_sym, job_run, @job_id, @user_id, @symbolized_options if self.respond_to?(name.to_sym)
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
      e = Desmond::JobRun.create!(job_id: job_id, job_class: self.name, user_id: user_id, status: 'queued', queued_at: Time.now)
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
    #
    def delete_job(success)
      status = 'done'
      status = 'failed' unless success
      destroy if Que.mode != :sync # Que doesn't in the database in sync mode
      jr = job_run
      jr.update(status: status, completed_at: Time.now)
    end

    ##
    # check that the types contained in +result+ are persistable in json
    #
    def check_result_type(result, strict=false)
      if !strict && result.is_a?(Array)
        result.each { |e| check_result_type(e) }
      elsif !strict && result.is_a?(Hash)
        result.keys.each { |k| check_result_type(k, strict=true) }
        result.values.each { |v| check_result_type(v) }
      else
        unless result.nil? || result.is_a?(Numeric) || result.is_a?(Symbol) || result.is_a?(String) || result.is_a?(TrueClass) || result.is_a?(FalseClass)
          Que.log level: :error, msg: 'Invalid result type', result: result
          fail 'Invalid result type'
        end
      end
    end

    ##
    # log a message with level +level+ and +prefix_str+ appended by job options
    #
    def log_job_event(level, prefix_str)
      job_args = self.attrs[:args]
      msg = "#{prefix_str} #{self.class.name} with (#{job_args[0, job_args.size - 1].join(', ')}, ...)"
      Que.log level: level, msg: msg, options: @censored_options
    end
  end
end
