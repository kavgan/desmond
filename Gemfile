source 'https://rubygems.org'

gem 'activerecord' # used together with 'pg' to store information about jobs
gem 'pg' # used to connect to RedShift directly
gem 'que' # background jobs
gem 'daemons' # background processes
gem 'aws-sdk-v1' # s3 access (v2 is still in preview release)

group :development, :test do
  gem 'rspec'
  gem 'simplecov', require: false # code coverage
  gem 'sinatra-activerecord' # ActiveRecord rake tasks

  # code style stuff
  gem 'rubocop', require: false
end
