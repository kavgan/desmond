require 'que'
require 'active_record'
require 'yaml'
require 'aws-sdk-v1'
require 'erubis'
require 'pony'
# not loaded automatically
require 'active_support/hash_with_indifferent_access'

require_relative 'desmond/csv'
require_relative 'desmond/log_censor'
require_relative 'desmond/job_run_finders'
require_relative 'desmond/jobs/base'
require_relative 'desmond/jobs/export'
require_relative 'desmond/models/job_run'

class DesmondConfig
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
    self.mail
  end
  ##
  # retrieve the desmond configuration
  #
  def self.config
    self.config_file = 'config/desmond.yml' if @config.nil?
    @config
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
  # loads the mail configuration
  #
  def self.mail
    mail_config = load_config_file(config['mail'] || 'config/mail.yml')
    Pony.options = {
      :from => mail_config['username'],
      :via => :smtp,
      :via_options => mail_config.symbolize_keys
    }
  end
  ##
  # retrieves the mail template for successful exports
  #
  def self.mail_export_success(options={})
    file = config['mail_export_success'] || 'mailers/export_success.yml'
    @mail_export_success = load_config_file(file) if @mail_export_success.nil?
    @mail_export_success.merge(options)
  end
  ##
  # retrieves the mail template for failed exports
  #
  def self.mail_export_failure(options={})
    file = config['mail_export_failure'] || 'mailers/export_failure.yml'
    @mail_export_failure = load_config_file(file) if @mail_export_failure.nil?
    @mail_export_failure.merge(options)
  end

  private
    def self.load_config_file(file)
      return {} if not(File.exists?(file))
      return ActiveSupport::HashWithIndifferentAccess.new(YAML.load_file(file))
    end
end

# configure ActiveRecord, but the app using us should really do this,
# mostly included for development purposes on desmond itself
if ActiveRecord::Base.configurations.empty?
  ActiveRecord::Base.configurations = DesmondConfig.database
  ActiveRecord::Base.establish_connection DesmondConfig.environment
end

# configure pony
DesmondConfig.mail

# configure que
Que.connection = ActiveRecord
Que.mode = :off
