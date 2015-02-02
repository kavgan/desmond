require 'simplecov'
SimpleCov.start
require 'rspec'
require 'rake'

# root path of project
root_path = File.join File.expand_path(File.dirname(__FILE__)), '..'

# set to 'test' environment
ENV['RACK_ENV'] = ENV['RAILS_ENV'] = 'test'

require 'desmond'
require 'sinatra/activerecord/rake'

# recreate test database from migrations
ActiveRecord::Schema.verbose = false # no output for migrations
Rake::Task['db:drop'].invoke
Rake::Task['db:reset'].invoke

RSpec.configure do |config|
  # only allowing new syntax, no mixing
  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before(:all) do
    @config = YAML.load_file(File.join root_path, 'config', 'tests.yml').symbolize_keys
    AWS.config({
      access_key_id: @config[:access_key_id],
      secret_access_key: @config[:secret_access_key]
    })
  end
end
