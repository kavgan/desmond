source 'https://rubygems.org'

gem 'activerecord' # used together with 'pg' to store information about jobs
gem 'pg' # used to connect to RedShift directly
gem 'que' # background jobs
gem 'daemons' # background processes
gem 'aws-sdk-v1' # s3 access (v2 is still in preview release)
gem 'pony' # sending emails
gem 'erubis' # mail templates

group :development, :test do
  gem 'rspec'
  gem 'simplecov', :require => false
  gem 'sinatra-activerecord' # ActiveRecord initialization and automatic rake tasks
end
