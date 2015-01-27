require_relative '../desmond'
require 'fileutils'

#
# returns migration files in the given directory +dirname+
#
def migrations(dirname)
  Dir.foreach(dirname).select do |entry|
    not(entry.start_with?('.'))
  end
end

#
# returns the next migration number to use from the given directory +dirname+
#
def next_migration_number(dirname)
  current_migration_number_str = migrations(dirname).map do |file|
    File.basename(file).split('_').first
  end.max || '0000'
  current_migration_number = current_migration_number_str.to_i

  num_digits = current_migration_number_str.size
  if num_digits > 5
    # timestamp format
    [Time.now.utc.strftime("%Y%m%d%H%M%S"), format("%.14d", current_migration_number + 1)].max
  else
    # counter format
    format("%.#{num_digits}d", current_migration_number + 1)
  end
end

#
# copy migration +source_file+ to migrations folder +dest_folder+
# if it doesn't exist ther yet
#
def copy_migration(source_file, dest_folder)
  # check if migration already exists
  migration_exists = migrations(dest_folder).map do |file|
    File.basename(file).split('_')[1..-1].join('_').eql?(File.basename(source_file))
  end.any?
  return if migration_exists

  # copy migration file
  migration_number = next_migration_number(dest_folder)
  base_migration_name = File.basename source_file
  migration_name = "#{migration_number}_#{base_migration_name}"
  dest_file = File.join dest_folder, migration_name
  FileUtils.cp(source_file, dest_file)
end

namespace :desmond do
  desc 'Setup database for desmond'
  task :migrate do
    Que.migrate! :version => 3

    migration = File.join File.dirname(__FILE__), 'migrations', 'add_desmond_job_runs.rb'
    copy_migration(migration, 'db/migrate')
  end

  desc 'Start daemon for desmond'
  task :run do
    require 'que/rake_tasks' # so that que's tasks don't get directly included in using apps
    Rake::Task['que:work'].invoke
  end

  desc 'Clear job runs and queues'
  task :clear do
    require 'que/rake_tasks' # so that que's tasks don't get directly included in using apps
    Rake::Task['que:clear'].invoke
    ActiveRecord::Base.connection.execute("TRUNCATE desmond_job_runs")
  end
end
