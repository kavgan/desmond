module Desmond
  ##
  # job importing data into AWS RedShift or Postgres from S3.
  #
  # intelligently chooses between `ImportPgJob` and `ImportRsJob`.
  # see their respective documentation for options and stuff
  #
  # Please see `BaseJob` class documentation on how to run
  # any job using its general interface.
  #
  class ImportJob < BaseJob
    def self.enqueue(job_id, user_id, options={})
      adapter = get_database_adapter(options)
      if adapter == 'postgresql'
        ImportPgJob.enqueue(job_id, user_id, options)
      elsif adapter == 'redshift'
        ImportRsJob.enqueue(job_id, user_id, options)
      else
        fail "Unknown database adapter '#{adapter}'"
      end
    end

    def self.run(job_id, user_id, options={})
      adapter = get_database_adapter(options)
      if adapter == 'postgresql'
        ImportPgJob.run(job_id, user_id, options)
      elsif adapter == 'redshift'
        ImportRsJob.run(job_id, user_id, options)
      else
        fail "Unknown database adapter '#{adapter}'"
      end
    end

    def self.get_database_adapter(options)
      fail 'No database options!' if options[:db].nil?
      config_name = options[:db][:connection_id]
      fail 'No connection id!' if config_name.nil? || config_name.empty?
      conf = ActiveRecord::Base.configurations[config_name.to_s]
      fail "Connection configuration '#{config_name.to_s}' not found" if conf.nil? || conf.empty?
      conf['adapter']
    end
    private_class_method :get_database_adapter
  end
end
