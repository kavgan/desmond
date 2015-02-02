# configure log censoring, so that password and AWS secret keys don't end up in the logs
CENSORED_KEYS = [ 'password', 'secret_access_key' ]
Que.log_formatter = proc do |data|
  tmp = ActiveSupport::HashWithIndifferentAccess.new(data)
  if tmp.include?(:job)
    tmp[:job][:args] = tmp[:job][:args].map do |arg|
      censor_hash_keys(arg, CENSORED_KEYS) if arg.is_a?(Hash)
    end
  end
  tmp.to_json
end


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
    # test the job
    #
    # +user_id+: unique identifier of the application's user running this test
    # +options+: depends on the implementation of the job
    #
    def self.test(user_id, options={})
      raise NotImplementedError
    end

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
      super(job_id, user_id, options.merge({ _run_id: e.id }))
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
      Desmond::JobRun.find(self.run_id).update(status: 'running', executed_at: Time.now)
    end

    ##
    # job is completed, but failed.
    # +mail+ will be passed to Pony.mail.
    # +details+ will be saved for this run in the database.
    #
    def failed(mail={}, details={})
      mail ||= {}
      details ||= {}
      delete_job(false, details)
      mail_failure(mail)
    end

    ##
    # job is completed and succeeded.
    # +mail+ will be passed to Pony.mail.
    # +details+ will be saved for this run in the database.
    #
    def done(mail={}, details={})
      mail ||= {}
      details ||= {}
      delete_job(true, details)
      mail_success(mail)
    end

    private
      def job_run
        Desmond::JobRun.find(self.run_id)
      end

      def delete_job(success, details={})
        status = 'done'
        status = 'failed' if not(success)
        destroy if Que.mode != :sync # Que doesn't in the database in sync mode
        Desmond::JobRun.find(self.run_id).update(status: status, details: details, completed_at: Time.now)
      end

      def mail_success(options={})
        options['to'] = options['mail_success']
        mail(options)
      end

      def mail_failure(options={})
        options['to'] = options['mail_failure']
        mail(options)
      end

      def mail(options)
        return if options['to'].nil? || options['subject'].nil? || options['body'].nil?
        options['subject'] = Erubis::Eruby.new(options['subject']).evaluate(options)
        options['body'] = Erubis::Eruby.new(options['body']).evaluate(options)
        Pony.mail(options.symbolize_keys.select{ |k,v|
          k == :to || k == :from || k == :subject || k == :body
        })
      end
  end
end
