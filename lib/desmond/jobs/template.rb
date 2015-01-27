module Desmond
  ##
  # template for custom jobs
  #
  class TemplateJob < BaseJob
    ##
    # method specifying what should be done at execution
    #
    def run(job_id, user_id, options={})
      # make sure to call super before running your job
      super(job_id, user_id, options)

      begin
        ActiveRecord::Base.transaction do
          # do the job
          raise 'Nothing'

          # everything is done, remove the job
          done({})
        end
      rescue => e
        # mark job as failed and remove it
        failed({ error: e.message, backtrace: e.backtrace.join("\n ") })
        raise e
      end
    end
  end
end
