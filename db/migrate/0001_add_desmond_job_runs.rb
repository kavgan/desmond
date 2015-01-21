class AddDesmondJobRuns < ActiveRecord::Migration
  def change
    create_table(:desmond_job_runs) do |t|
      t.string    :job_id, null: false
      t.string    :job_class, null: false
      t.string    :user_id, null: false
      t.string    :status, null: false
      t.datetime  :queued_at, null: false
      t.datetime  :executed_at, null: true
      t.datetime  :completed_at, null: true
      t.json      :details, null: false
    end
    add_index :desmond_job_runs, :job_id
    add_index :desmond_job_runs, :user_id
  end
end
