require_relative '../spec_helper'

describe Desmond::ImportJob do
  IMPORT_RS_CONN_ID = 'redshift_test'
  let (:conn) { Desmond::PGUtil.dedicated_connection(connection_id: IMPORT_RS_CONN_ID) }

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
      s3_obj = Aws::S3::Bucket.new(@config[:import_bucket]).object(unique_name)
      s3_obj.put(body: File.read(file))

      run = Desmond::ImportJob.enqueue('JobId', 'UserId', {
          db: {
            connection_id: IMPORT_RS_CONN_ID,
            schema: @config[:import_schema],
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
      #fail run.error if run.failed?
    ensure
      s3_obj.delete unless s3_obj.nil?
      rewrite_s3_obj = Aws::S3::Bucket.new(@config[:import_bucket]).object(unique_name + 'rewrite')
      rewrite_s3_obj.delete if rewrite_s3_obj.exists?
    end
    return run, unique_name
  end

  def run_import(file, options={})
    run, table = __run_import(file, options)
    run
  ensure
    begin
      conn.exec("DROP TABLE IF EXISTS #{@config[:import_schema]}.#{table}") unless table.nil?
    rescue => e
      # ignore failed query, tested invalid credentials
    end
  end

  #
  # runs an import and returns the database rows
  #
  def run_import_and_return_rows(file, options={})
    run, table = __run_import(file, options)
    conn.exec("SELECT * FROM #{@config[:import_schema]}.#{table}").to_a
  ensure
    conn.exec("DROP TABLE IF EXISTS #{@config[:import_schema]}.#{table}") unless table.nil? || options[:donotdeletetable]
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

  it 'should import carriage return newlines' do
    expect(run_import_and_return_rows('spec/import_comma_r_newlines.csv')).to match_array([
          { "id" => "0", "txt" => "null" },
          { "id" => "1", "txt" => "eins" }
        ])
  end

  it 'should import with null as option' do
    expect(run_import_and_return_rows('spec/import_pipe.csv', csv: { null_as: 'null' })).to match_array([
          { "id" => "0", "txt" => nil },
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
