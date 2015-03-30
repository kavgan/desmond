module Desmond
  ##
  # module implementing finders for the JobRun model.
  #
  module JobRunFinders
    def self.included(base)
      base.send :extend, ClassMethods
    end

    ##
    # class methods helping find a particular job run
    #
    module ClassMethods
      ##
      # returns the last run for the given +job_id+.
      # optionally filter by +user_id+.
      # returns nil if none available.
      #
      def last_run(job_id, user_id=nil)
        job_runs(job_id, user_id).order(queued_at: :desc).take(1).first
      end

      ##
      # get the last +n+ runs for the given +job_id+.
      # optionally filter by +user_id+.
      # returns an array of the found runs, which is empty if none are available
      #
      def last_runs(job_id, n, user_id=nil)
        job_runs(job_id, user_id).order(queued_at: :desc).take(n)
      end

      ##
      # get all runs queued or still executing for the given +job_id+.
      # optionally filter by +user_id+.
      #
      def runs_unfinished(job_id, user_id=nil)
        job_runs(job_id, user_id, [JobRun::STATUS_QUEUED, JobRun::STATUS_RUNNING])
      end

      private

      def job_runs(job_id, user_id=nil, status=nil)
        q = JobRun.where(job_id: job_id, job_class: self.name)
        q = q.where(status: status) unless status.nil?
        q = q.where(user_id: user_id) unless user_id.nil?
        q
      end
    end
  end
end
