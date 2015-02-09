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
      ActiveRecord::Base.transaction do
        # do the job
        fail 'Nothing'

        # everything is done, remove the job
        done(success_message: 'how did this happen?')
      end
    end
  end
end
