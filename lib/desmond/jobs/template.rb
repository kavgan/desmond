# rubocop:disable Lint/UnreachableCode

module Desmond
  ##
  # template for custom jobs
  #
  class TemplateJob < BaseJob
    ##
    # method specifying what should be done at execution
    #
    def execute(job_id, user_id, options={})
      begin
        ActiveRecord::Base.transaction do
          # do the job
          fail 'Nothing'

          # everything is done, remove the job
          done({}, {})
        end
      rescue => e
        # mark job as failed and remove it
        failed({}, error: e.message, backtrace: e.backtrace.join("\n "))
      end
    end
  end
end
