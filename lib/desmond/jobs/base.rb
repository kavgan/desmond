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
      e = Desmond::JobRun.create(job_id: job_id, job_class: self.name, user_id: user_id, status: 'queued', queued_at: Time.now)
      # the run_id is passed in as an option, because in the synchronous job execution mode, the created job instance
      # is returned after the job was executed, so no JobRun instance would be accessible during execution of the job
      super(job_id, user_id, options.merge(_run_id: e.id))
      # requery from database, since it was already updated in sync mode
      Desmond::JobRun.find(e.id)
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
      self.run_id = options[:_run_id] # retrieve run_id from options and safe it in the instance
      job_run.update(status: 'running', executed_at: Time.now)
      run_hook(:before)
      self.send :execute, job_id, user_id, options if self.respond_to?(:execute)
      self.done if job_run.running?
      run_hook(:after)
    rescue => e
      Que.log level: :error, message: "Error executing job #{self.class.name}(#{job_id}, #{user_id}, #{options}):"
      Que.log level: :error, exception: e.message
      Que.log level: :error, backtrace: e.backtrace.join("\n ")
      self.failed(error: e.message)
    end

    ##
    # job is completed, but failed.
    # +details+ will be saved for this run in the database.
    #
    def failed(details={})
      details ||= {}
      delete_job(false, details)
    end

    ##
    # job is completed and succeeded.
    # +details+ will be saved for this run in the database.
    #
    def done(details={})
      details ||= {}
      delete_job(true, details)
    end

    private

    def run_hook(name)
      self.send name.to_sym, job_run, *self.attrs[:args] if self.respond_to?(name.to_sym)
    rescue Exception => e
      Que.log level: :error, message: 'Error executing hook:'
      Que.log level: :error, exception: e.message
      Que.log level: :error, backtrace: e.backtrace.join("\n ")
    end

    def job_run
      Desmond::JobRun.find(self.run_id)
    end

    def delete_job(success, details={})
      status = 'done'
      status = 'failed' unless success
      destroy if Que.mode != :sync # Que doesn't in the database in sync mode
      job_run.update(status: status, details: details, completed_at: Time.now)
    end
  end
end
