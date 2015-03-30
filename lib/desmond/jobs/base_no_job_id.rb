module Desmond
  ##
  # base class for queueable jobs which do not need a job_id.
  # implement 'execute' instance method in subclasses to specify behavior.
  # templates available in 'template_*.rb'.
  #
  # schedule for background processing using class method `enqueue`.
  # run immediately blocking using class method `run`.
  #
  class BaseJobNoJobId < BaseJob
    #
    # methods for setting the default job_id which will be used in the background
    #
    class << self
      attr_accessor :default_job_id
    end
    # since every subclass has its own sets of class instance variables,
    # we need to define it on every subclass
    def self.inherited(subclass)
      subclass.instance_variable_set(:@default_job_id, 1)
    end

    ##
    # see `Desmond::BaseJob.enqueue`
    #
    def self.enqueue(user_id, options={})
      super(self.default_job_id, user_id, options)
    end

    ##
    # see `Desmond::BaseJob.run`
    #
    def self.run(*args)
      if args.size == 3 # `enqueue` will call us in sync mode with three arguments -.-
        job_id, user_id, options = *args
      elsif args.size == 2
        job_id, user_id, options = self.default_job_id, *args
      elsif args.size == 1
        job_id, user_id, options = self.default_job_id, *args, {}
      else
        fail ArgumentError, "wrong number of arguments (#{args.size} for 1..2)"
      end
      super(job_id, user_id, options)
    end

    ##
    # see `Desmond::JobRunFinders.last_run`
    #
    def self.last_run(user_id=nil)
      super(self.default_job_id, user_id)
    end

    ##
    # see `Desmond::JobRunFinders.last_runs`
    #
    def self.last_runs(n, user_id=nil)
      super(self.default_job_id, n, user_id)
    end

    ##
    # see `Desmond::JobRunFinders.runs_unfinished`
    #
    def self.runs_unfinished(user_id=nil)
      super(self.default_job_id, user_id)
    end
  end
end
