require 'que'
require 'active_record'
require 'yaml'
require 'aws-sdk-v1'
require 'active_support/hash_with_indifferent_access'

require_relative 'desmond/monkey_patches'
require_relative 'desmond/utils/pg_util'
require_relative 'desmond/utils/log_censor'

require_relative 'desmond/execution_error'

require_relative 'desmond/streams/base'
require_relative 'desmond/streams/csv'
require_relative 'desmond/streams/database'
require_relative 'desmond/streams/s3'

require_relative 'desmond/job_run_finders'
require_relative 'desmond/jobs/base'
require_relative 'desmond/jobs/base_no_job_id'
require_relative 'desmond/jobs/export'
require_relative 'desmond/jobs/import'
require_relative 'desmond/jobs/unload'

require_relative 'desmond/models/job_run'

##
# manages the gem's configuration
#
class DesmondConfig
  @is_daemon = false
  @logger = Logger.new(STDOUT)
  @exception_notifier = []
  class << self
    attr_accessor :logger
    attr_reader :is_daemon
  end

  ##
  # determins the environment we are running in:
  # - development
  # - staging
  # - production
  #
  def self.environment
    (ENV['RACK_ENV'] || 'development').to_sym
  end
  ##
  # set where the desmond configuration file is located
  # and reload the configration
  #
  def self.config_file=(file)
    @config = load_config_file(file)
  end
  ##
  # retrieve the desmond configuration
  #
  def self.config
    self.config_file = 'config/desmond.yml' if @config.nil?
    @config
  end

  ##
  # retrieves the app_id from the config, defaults to 'desmond'
  #
  def self.app_id
    config['app_id'] || 'desmond'
  end

  ##
  # adds the given argument to the list of block to be called when
  # an uncaught exception occurs during job execution
  #
  # blocks will be called with the arguments (exception, job_class, job_run)
  #
  def self.add_exception_notifier(&block)
    @exception_notifier << block
  end
  def self.clear_exception_notifier
    @exception_notifier = []
  end
  def self.register_with_exception_notifier(options={})
    options.each do |notifier_name, options|
      ExceptionNotifier.register_exception_notifier(notifier_name, options)
    end
    DesmondConfig.add_exception_notifier do |exception, job_class, job_run|
      ExceptionNotifier.notify_exception(exception, :data => { :class => job_class, run: job_run })
    end
  end

  ##
  # change 'app_id' to +value+
  # only use this in the 'test' environment otherwise the change
  # will not be shared with the worker processes, use the desmond.yml
  # configuration file instead
  #
  def self.app_id=(value)
    fail 'Do not use this DesmondConfig.app_id= outside of "test"' if self.environment != :test
    config['app_id'] = value
  end
  ##
  # retrieve the location of the ActiveRecord database configuration file.
  # should really only be used when developing desmond, otherwise the using app
  # should establish the ActiveRecord connections
  #
  def self.database
    load_config_file(config['database'] || 'config/database.yml')
  end
  ##
  # determines whether the tasks are allowed to use the configured
  # connection instead of using the users credentials
  #
  def self.system_connection_allowed?
    config['system_connection_allowed'] || false
  end
  ##
  # change 'system_connection_allowed' to +value+
  # only use this in the 'test' environment otherwise the change
  # will not be shared with the worker processes, use the desmond.yml
  # configuration file instead
  #
  def self.system_connection_allowed=(value)
    fail 'Do not use this DesmondConfig.system_connection_allowed= outside of "test"' if self.environment != :test
    config['system_connection_allowed'] = value
  end

  def self.load_config_file(file)
    return {} unless File.exist?(file)
    ActiveSupport::HashWithIndifferentAccess.new(YAML.load_file(file))
  end
  private_class_method :load_config_file

  def self.set_daemon
    @is_daemon = true
  end
  private_class_method :set_daemon

  def self.exception_notification(exception, job_class, job_run)
    @exception_notifier.each do |thing|
      thing.call(exception, job_class, job_run) rescue nil
    end
  end
  private_class_method :exception_notification
end

# configure ActiveRecord, but the app using us should really do this,
# mostly included for development purposes on desmond itself
if ActiveRecord::Base.configurations.empty?
  ActiveRecord::Base.configurations = DesmondConfig.database
  ActiveRecord::Base.establish_connection DesmondConfig.environment
end

# configure que
Que.connection = ActiveRecord
ActiveRecord::Base.schema_format = :sql # otherwise the que_jobs table gets missed
Que.mode = :sync if DesmondConfig.environment == :test
Que.mode = :off if DesmondConfig.environment != :test

# configure log censoring, so that password and AWS secret keys don't end up in the logs
CENSORED_KEYS = %w(password secret_access_key)
Que.log_formatter = proc do |data|
  tmp = ActiveSupport::HashWithIndifferentAccess.new(data)
  if tmp.include?(:job) && !tmp[:job].nil?
    tmp[:job][:args] = tmp[:job][:args].map do |arg|
      censor_hash_keys(arg, CENSORED_KEYS) if arg.is_a?(Hash)
    end
  end
  tmp.to_json
end

#
# overwriting the get logger method of Que, to always return
# Desmond's configured logger. Que tends to overwrite the logger
# in background situations, which we'll ignore by this.
#
module Que
  class << self
    def logger
      DesmondConfig.logger
    end
  end
end
