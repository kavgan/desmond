require_relative '../spec_helper'

describe Desmond::ImportPgJob do
  CONN_ID = 'test'

  #
  # runs an import and returns the job run
  #
  def __run_import(file, options={})
    unique_name = "desmond_test_#{rand(1024)}"
    if options.has_key?(:db) && options[:db].has_key?(:table) && !options[:db][:table].nil?
      unique_name = options[:db][:table]
    end
    s3_obj = nil
    run = nil
    begin
      s3_obj = AWS::S3.new.buckets[@config[:import_bucket]].objects.create(unique_name, File.read(file))

      run = Desmond::ImportPgJob.enqueue('JobId', 'UserId', {
          db: {
            connection_id: CONN_ID,
            table: unique_name
          },
          s3: {
            bucket: @config[:import_bucket],
            key: unique_name
          },
          csv: {
            headers: :first_row
          }
        }.deep_merge(options)
      )
    ensure
      s3_obj.delete unless s3_obj.nil?
    end
    return run, unique_name
  end

  def run_import(file, options={})
    run, table = __run_import(file, options)
    run
  ensure
    begin
      ActiveRecord::Base.connection.execute("DROP TABLE IF EXISTS #{table}") unless table.nil?
    rescue => e
      # ignore failed query, tested invalid credentials
    end
  end
  #
  # runs an import and returns the database rows
  #
  def run_import_and_return_rows(file, options={})
    run, table = __run_import(file, options)
    ActiveRecord::Base.connection.execute("SELECT * FROM #{table}").to_a
  ensure
    ActiveRecord::Base.connection.execute("DROP TABLE IF EXISTS #{table}") unless table.nil? || options[:donotdeletetable]
  end

  it 'should import a pipe-delimited csv' do
    expect(run_import_and_return_rows('spec/import_pipe.csv')).to match_array([
          { "id" => "0", "txt" => "null" },
          { "id" => "1", "txt" => "eins" }
        ])
  end

  it 'should import a comma-delimited csv' do
    expect(run_import_and_return_rows('spec/import_comma.csv')).to match_array([
          { "id" => "0", "txt" => "null" },
          { "id" => "1", "txt" => "eins" }
        ])
  end

  it 'should import a quoted csv' do
    expect(run_import_and_return_rows('spec/import_quoted.csv')).to match_array([
          { "id" => "0", "txt" => "null" },
          { "id" => "1", "txt" => "eins" }
        ])
  end

  it 'should drop existing tables on import' do
    fixed_table_name = "desmond_test_#{rand(1024)}"
    run_import_and_return_rows('spec/import_comma.csv', db: { table: fixed_table_name }, donotdeletetable: true)
    expect(run_import_and_return_rows('spec/import_comma.csv', db: { table: fixed_table_name, dropifexists: true })).to match_array([
          { "id" => "0", "txt" => "null" },
          { "id" => "1", "txt" => "eins" }
        ])
  end

  it 'should save the destination table' do
    fixed_table_name = "desmond_test_#{rand(1024)}"
    expect(run_import('spec/import_comma.csv', db: {
      table: fixed_table_name
    }).result).to have_key('table')
  end

  it 'should complain about missing table name' do
    run = run_import('spec/import_comma.csv', db: { table: nil })
    expect(run.failed?).to eq(true)
    expect(run.error).to eq('Empty table name!')
  end

  it 'should complain about missing headers' do
    r = run_import('spec/import_pipe.csv', csv: { headers: nil })
    expect(r.failed?).to eq(true)
    expect(r.error).to eq('No CSV headers!')
  end

  it 'should complain about invalid database configuration' do
    r = run_import('spec/import_pipe.csv', db: { connection_id: nil })
    expect(r.failed?).to eq(true)
    expect(r.error).to eq('No connection id!')
  end

  it 'should complain about invalid database credentials' do
    prev_value = DesmondConfig.system_connection_allowed?
    begin
      DesmondConfig.system_connection_allowed = false
      r = run_import('spec/import_pipe.csv', db: { username: nil })
      expect(r.failed?).to eq(true)
      expect(r.error).to eq('No db connection username!')
      r = run_import('spec/import_pipe.csv', db: { username: 'test', password: nil })
      expect(r.failed?).to eq(true)
      expect(r.error).to eq('No db connection password!')
    ensure
      DesmondConfig.system_connection_allowed = prev_value
    end
  end
end
