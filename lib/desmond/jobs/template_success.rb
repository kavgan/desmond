require_relative '../../desmond'

module Desmond
  ##
  # template for custom jobs
  #
  class TemplateSuccessJob < BaseJob
    ##
    # method specifying what should be done at execution
    # +options+ will always be passed symbolized
    #
    def execute(job_id, user_id, options={})
      sleep(10)
      42
    end
  end
end

if __FILE__ == $PROGRAM_NAME
  p Desmond::TemplateSuccessJob.run(1, 1)
end
