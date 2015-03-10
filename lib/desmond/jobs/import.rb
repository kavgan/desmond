module Desmond
  ##
  # job importing data into AWS RedShift or Postgres from S3.
  #
  # Please see `BaseJob` class documentation on how to run
  # any job using its general interface.
  #
  class ImportJob < BaseJob
    ##
    # intelligently chooses between `ImportPgJob` and `ImportRsJob`.
    # see their respective documentation for options and stuff
    #
    def self.enqueue(job_id, user_id, options={})
      fail 'No database options!' if options[:db].nil?
      config_name = options[:db][:connection_id]
      fail 'No connection id!' if config_name.nil? || config_name.empty?
      conf = ActiveRecord::Base.configurations[config_name.to_s]
      fail "Connection configuration '#{config_name.to_s}' not found" if conf.nil? || conf.empty?
      if conf['adapter'] == 'postgresql'
        ImportPgJob.enqueue(job_id, user_id, options)
      elsif conf['adapter'] == 'redshift'
        ImportRsJob.enqueue(job_id, user_id, options)
      else
        fail "Unknown database adapter '#{conf['adapter']}'"
      end
    end
  end
end
