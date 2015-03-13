module Desmond
  ##
  # Model representing a run of any job
  #
  class JobRun < ActiveRecord::Base
    STATUS_QUEUED = 'queued'
    STATUS_RUNNING = 'running'
    STATUS_SUCCESS = 'done'
    STATUS_FAILURE = 'failed'

    self.table_name = :desmond_job_runs
    # id                     :integer          not null, primary key
    # job_id                 :string           not null
    # job_class              :string           not null
    # user_id                :string           not null
    # status                 :string           not null
    # queued_at              :timestamp        not null
    # executed_at            :timestamp        null
    # completed_at           :timestamp        null
    # details                :json             not null

    after_initialize :init

    ##
    # default values for model
    #
    def init
      self.details ||= {}
    end

    ##
    # filename of the export file
    #
    def filename
      time = self.queued_at.utc.strftime('%Y_%m_%dT%H_%M_%S_%LZ')
      "#{DesmondConfig.app_id}_#{self.job_class.sub('::', '_')}_#{self.job_id}_#{self.user_id}_#{time}.csv"
    end

    ##
    # waits until job run finished executing
    # waits a max of +timeout+ secs and then returns.
    # if +timeout+ is nil, wait indefinitly.
    # +timeout+ can be a decimal to get under 1sec resolution
    # returns self if job is completed, nil otherwise
    #
    def wait_until_finished(timeout=nil)
      conn = ActiveRecord::Base.connection_pool.checkout
      # trying to use only finished?, in case implementations
      # of unfinished? and finished? diverge accidently
      return self if self.finished?
      # use postgres as a conditional variable across processes/machines
      PGUtil.listen(conn, "job_run_#{self.id}", timeout)
      self.reload # reload object from database to check if something changed
      return nil unless self.finished?
      self
    ensure
      ActiveRecord::Base.connection_pool.checkin(conn)
    end

    ##
    # is this run still queued?
    #
    def queued?
      self['status'] == STATUS_QUEUED
    end

    ##
    # is this run already being executed?
    #
    def running?
      self['status'] == STATUS_RUNNING
    end

    ##
    # is this run queued or executing?
    #
    def unfinished?
      (self.queued? || self.running?)
    end

    ##
    # is this run finished executing?
    # doesn't matter if it failed or suceeded
    #
    def finished?
      (self.done? || self.failed?)
    end

    ##
    # did this run fail?
    #
    def failed?
      self['status'] == STATUS_FAILURE
    end

    ##
    # did this run complete successfully?
    #
    def done?
      self['status'] == STATUS_SUCCESS
    end

    ##
    # return value of the job
    #
    def result
      self['details']['_job_result']
    end

    ##
    # error saved by the job
    #
    def error
      self['details']['error']
    end
  end
end
