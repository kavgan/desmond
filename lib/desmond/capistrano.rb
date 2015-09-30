namespace :desmond do
  def pid_dir
    fetch(:pid_dir, "#{fetch(:shared_path)}/pids")
  end

  def cmd(cmd)
    "cd #{current_path};#{env} #{workers} #{que_command} #{cmd} #{pid_dir}"
  end

  desc 'Stop the desmond process'
  task :stop do
    on roles(:app) do
      within release_path do
        with rack_env: fetch(:rack_env), que_worker_count: fetch(:que_num_workers, 1) do
          execute :bundle, :exec, :desmond, 'stop', pid_dir
        end
      end
    end
  end

  desc 'Start the desmond process'
  task :start do
    on roles(:app) do
      within release_path do
        with rack_env: fetch(:rack_env), que_worker_count: fetch(:que_num_workers, 1) do
          execute :bundle, :exec, :desmond, 'start', pid_dir
        end
      end
    end
  end

  desc 'Restart the desmond process'
  task :restart do
    on roles(:app) do
      within release_path do
        with rack_env: fetch(:rack_env), que_worker_count: fetch(:que_num_workers, 1) do
          execute :bundle, :exec, :desmond, 'restart', pid_dir
        end
      end
    end
  end

  after 'deploy:finished' , 'desmond:restart'
end
