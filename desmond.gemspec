Gem::Specification.new do |s|
  s.name        = 'desmond'
  s.version     = '0.0.0'
  s.date        = '2015-01-14'
  s.summary     = 'CSV Export/Import gem'
  s.description = 'Exporting and importing a lot of data out of Amazon RedShift'
  s.authors     = ['Tobias Thiel']
  s.email       = 'tobi@amg.tv'
  s.files       = [
    'lib/desmond.rb',
    'lib/desmond/rake.rb',
    'lib/desmond/capistrano.rb'
  ]
  s.homepage    = 'http://amg.tv'
  s.license     = 'MIT'

  s.require_paths = ['lib']
  s.executables << 'desmond'

  s.add_runtime_dependency 'activerecord', '~> 4.2' # used together with 'pg' to store information about jobs
  s.add_runtime_dependency 'activesupport', '~> 4.2' # hash symbolize extensions
  s.add_runtime_dependency 'pg', '>= 0.17', '< 0.19' # used to connect to RedShift directly
  s.add_runtime_dependency 'que', '~> 0.9' # background jobs
  s.add_runtime_dependency 'daemons', '~> 1.1' # background processes
  s.add_runtime_dependency 'aws-sdk-v1', '~> 1' # s3 access (v2 is still in preview release)
  s.add_runtime_dependency 'rake', '~> 10.4' # ability to run background processes

  s.add_development_dependency 'rspec'
  s.add_development_dependency 'simplecov'
  s.add_development_dependency 'rubocop'
  s.add_development_dependency 'sinatra-activerecord' # ActiveRecord rake tasks
end
