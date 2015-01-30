module Desmond
  ##
  # Model representing a run of any job
  #
  class JobRun < ActiveRecord::Base
    # TODO put into Desmond module directly
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
  end
end