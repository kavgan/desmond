# rubocop:disable Lint/UnreachableCode

module Desmond
  ##
  # template for custom jobs
  #
  class TemplateFailureJob < BaseJob
    ##
    # method specifying what should be done at execution
    # +options+ will always be passed symbolized
    #
    def execute(job_id, user_id, options={})
      ActiveRecord::Base.transaction do
        # do the job
        sleep(10)
        fail 'Nothing'

        # everything is done
        { success_message: 'how did this happen?' }
      end
    end
  end
end
