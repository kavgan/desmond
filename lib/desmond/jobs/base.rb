module Que
  class Job
    class << self
      def run(*args)
        # monkey patching Que so it doesn't discard what `run` returns
        new(:args => args).run(*args)
      end
    end
  end
end

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
    attr_reader :run_id
    attr_reader :job_id, :user_id

    ##
    # queue this job for execution
    #
    # +job_id+: unique identifier for this type of job from calling application for later identification
    # +user_id+: unique identifier of the application's user running this export for later identification
    # +options+: depends on the implementation of the job
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
    # see instance method `enqueue` for parameter documentation
    #
    # returns the return value of the job's execute method
    #
    def self.run(job_id, user_id, options={})
      # self.enqueue will call this function in sync mode, so if the job run was already created, don't do it again
      job_run_id = options[:_run_id]
      called_by_enqueue = true # enqueue can call us, in which case the behavior of this method will be slightly different
      called_by_enqueue = false if job_run_id.nil? # not already create by `enqueue`
      # we are not gonna create a job run for synchronous executions

      # actually run the job
      result = super(job_id, user_id, options)

      # return return value of job if finished successful
      # otherwise re-raise the exception it threw
      # if it just called failed, use that message to raise an exception
      if !result.is_a?(Exception)
        result
      else
        # in sync mode, enqueue directly runs this method, where we don't want to throw an exception,
        # as this will not happen in async mode
        fail result unless called_by_enqueue # only raise exception if not called by 'enqueue'
      end
    end

    ##
    # job is completed, but failed.
    # called from within the job, to declare that it failed.
    #
    def failed(error)
      delete_job(false)
      @error = error
      job_run.update(details: { error: error }) unless @sync
    end

    ##
    # job is completed and succeeded.
    # called from within the job, to declare that it succeeded.
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

    ##
    # implements the job & hook logic
    # should be considered internal!
    # use class methods `run` and `enqueue` to execute the job!
    #
    # The call orders in the different modes of Que are:
    # - `enqueue` in async mode:
    #   - enqueue: class method
    #   - ... waiting to be executed
    #   - initialize: instance method
    #   - _run: instance method
    #   - run: instance method
    # - `enqueue` in sync mode:
    #   - enqueue: class method
    #   - run: class method
    #   - initialize: instance method
    #   - run: instance method
    # - `run` in all modes:
    #   - run: class method
    #   - initialize: instance method
    #   - run: instance method
    #
    def run(job_id, user_id, options={})
      # this is the first time we are in the job instance, so we'll save some important stuff we need
      @run_id = options.delete(:_run_id) # retrieve run_id from options and safe it in the instance
      @sync    = self.run_id.nil?
      @job_id  = job_id
      @user_id = user_id
      @symbolized_options = options.deep_symbolize_keys || {}
      @censored_options   = censor_hash_keys(@symbolized_options, CENSORED_KEYS)
      @error = nil
      @done = nil

      # start the logic
      unless @sync # in sync mode there is no job run, so we'll skip that
        jr = job_run # cache job run
        jr.update(status: 'running', executed_at: Time.now)
      end
      run_hook(:before)
      log_job_event(:info, "Starting to execute job")
      @result = self.send :execute, @job_id, user_id, @symbolized_options if self.respond_to?(:execute)
      # check that we can actually persist the result
      check_result_type(@result)
      unless @sync
        jr = job_run # update cache (might have been changed by execute)
        # save result in job_run
        jr.update(details: { _job_result: @result })
      end
      self.done if @done.nil? # mark as succeded if not done by the job
      if @done
        # return result of job for synchronous mode
        return @result
      else
        # when the job marked itself as failed return an exception
        return JobExecutionError.new(@error)
      end
    rescue => e
      log_job_event(:error, "Error executing job")
      Que.log level: :error, exception: e.message
      Que.log level: :error, backtrace: e.backtrace.join("\n ")
      # requery job_run (might have been changed by execute) and save error message
      self.failed(e.message)
      return e # return the exception for synchronous mode
    ensure
      log_job_event(:info, "Finished executing job")
      # we always want to execute the after hook
      run_hook(:success) if @done
      run_hook(:error) unless @done
      run_hook(:after)
      PGUtil.notify(Desmond::JobRun.connection, "job_run_#{self.run_id}") unless @sync
    end

    private

    ##
    # runs the hook with the given +name+
    # swallows all exceptions, only logging them
    #
    def run_hook(name)
      if self.respond_to?(name.to_sym)
        if @sync
          # create unpersisted fake job run in synchronous mode
          jr = self.class.create_job_run(@job_id, @user_id, persist: false, result: @result)
        else
          jr = job_run
        end
        # actually call hook method
        self.send name.to_sym, jr, @job_id, @user_id, @symbolized_options
      end
    rescue Exception => e
      log_job_event(:error, "Error executing hook '#{name}' for job")
      Que.log level: :error, exception: e.message
      Que.log level: :error, backtrace: e.backtrace.join("\n ")
    end

    ##
    # create a job run with the given parameters,
    # returning its id.
    # if +persist+ is false, the instance is returned.
    #
    def self.create_job_run(job_id, user_id, persist: true, result: nil)
      attributes = {
        job_id: job_id,
        job_class: self.name,
        user_id: user_id,
        status: 'queued',
        queued_at: Time.now,
        details: {
          _job_result: result
        }
      }
      if persist
        e = Desmond::JobRun.create!(attributes)
        e.id
      else
        Desmond::JobRun.new(attributes)
      end
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
      if success
        @done  = true
        status = 'done'
      else
        @done  = false
        status = 'failed'
      end
      destroy if Que.mode != :sync # Que doesn't persist in the database in sync mode
      unless @sync
        jr = job_run
        jr.update(status: status, completed_at: Time.now)
      end
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
