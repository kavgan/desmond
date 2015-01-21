# Capistrano Recipes for managing que
#
# Add these callbacks to have the que process restart when the server
# is restarted:
#
#   after "deploy:stop",    "desmond:stop"
#   after "deploy:start",   "desmond:start"
#   after "deploy:restart", "desmond:restart"
#
# To change the number of workers define a Capistrano variable que_num_workers:
#
#   set :que_num_workers, 4
#
# If you've got desmond workers running on specific servers, you can also specify
# which servers have desmond running and should be restarted after deploy.
#
#   set :que_server_role, :worker
#

Capistrano::Configuration.instance.load do
  namespace :desmond do
    def env
      fetch(:rails_env, false) ? "RACK_ENV=#{fetch(:rails_env)}" : ''
    end

    def workers
      fetch(:que_num_workers, false) ? "QUE_WORKER_COUNT=#{fetch(:que_num_workers)}" : ''
    end

    def roles
      fetch(:que_server_role, :app)
    end

    def que_command
      fetch(:que_command, "bundle exec ./scripts/que")
    end

    def pid_dir
      fetch(:pid_dir, "#{fetch(:current_path)}/tmp/pids")
    end

    desc 'Stop the que process'
    task :stop, :roles => lambda { roles } do
      run "cd #{current_path};#{env} #{workers} #{que_command} stop #{pid_dir}"
    end

    desc 'Start the que process'
    task :start, :roles => lambda { roles } do
      run "cd #{current_path};#{env} #{workers} #{que_command} start #{pid_dir}"
    end

    desc 'Restart the que process'
    task :restart, :roles => lambda { roles } do
      run "cd #{current_path};#{env} #{workers} #{que_command} restart #{pid_dir}"
    end
  end
end
