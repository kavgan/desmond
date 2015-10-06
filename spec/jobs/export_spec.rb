require_relative '../spec_helper'

describe Desmond::ExportJob do

  #
  # run an export test
  #
  def run_export_test(options={})
    Desmond::ExportJob.test('UserId', {
        connection_id: 'redshift_test',
        query: "SELECT * FROM exportdata;"
      }.deep_merge(options)
    )
  end

  #
  # runs an export and returns the job run
  #
  def run_export(options={})
    run = Desmond::ExportJob.enqueue('JobId', 'UserId', {
        db: {
          connection_id: 'redshift_test',
          query: "SELECT * FROM exportdata;"
        },
        s3: {
          bucket: @config[:export_bucket]
        }
      }.deep_merge(options)
    )
    AWS::S3.new.buckets[@config[:export_bucket]].objects[run.result['key']].delete unless run.failed? || options[:donotdelete]
    run
  end

  #
  # runs an export and returns the csv string from S3
  #
  def run_export_and_return_string(options={})
    run = run_export(options.merge(donotdelete: true))
    fail run.error if run.failed?
    s3_obj = nil
    csv = nil
    begin
      s3_obj = AWS::S3.new.buckets[@config[:export_bucket]].objects[run.result['key']]
      csv = s3_obj.read
    ensure
      s3_obj.delete
    end
    csv.split("\n")
  end

  before(:context) do
    c = Desmond::PGUtil.dedicated_connection(connection_id: 'redshift_test')
    c.exec("DROP TABLE IF EXISTS exportdata")
    c.exec("CREATE TABLE exportdata(id INT, txt VARCHAR)")
    c.exec("INSERT INTO exportdata VALUES(0, 'null')")
    c.exec("INSERT INTO exportdata VALUES(1, 'eins')")
  end

  it 'should export to pipe-delimited csv' do
    expect(run_export_and_return_string(csv: {
        col_sep: '|',
        return_headers: false
    })).to match_array(['0|null', '1|eins'])
  end

  it 'should export to comma-delimited csv' do
    expect(run_export_and_return_string(csv: {
        col_sep: ',',
        return_headers: false
    })).to match_array(['0,null', '1,eins'])
  end

  it 'should export to comma-delimited csv with header row' do
    expect(run_export_and_return_string(csv: {
        col_sep: ',',
        return_headers: true
    })).to match_array(['id,txt', '0,null', '1,eins'])
  end

  it 'should save the s3 details of the exported file' do
    run = run_export(csv: {
        col_sep: '|',
        return_headers: false
    })
    expect(run.done?).to eq(true)
    expect(run.result).to have_key('bucket')
    expect(run.result).to have_key('key')
    expect(run.result).to have_key('access_key')
  end

  it 'should allow custom s3 names' do
    custom_s3_key = "custom_desmond_test_#{rand(1024)}"
    run = run_export(csv: {
        col_sep: '|',
        return_headers: false
    },
    s3: {
      key: custom_s3_key
    })
    expect(run.done?).to eq(true)
    expect(run.result).to have_key('bucket')
    expect(run.result).to have_key('key')
    expect(run.result['key']).to eq(custom_s3_key)
    expect(run.result).to have_key('access_key')
  end

  it 'should complain about invalid database configuration' do
    r = run_export(db: { connection_id: nil })
    expect(r.failed?).to eq(true)
    expect(r.error).to eq('No connection id!')
  end

  it 'should complain about invalid database credentials' do
    prev_value = DesmondConfig.system_connection_allowed?
    begin
      DesmondConfig.system_connection_allowed = false
      r = run_export(db: { username: nil })
      expect(r.failed?).to eq(true)
      expect(r.error).to eq('No db connection username!')
      r = run_export(db: { username: 'test', password: nil })
      expect(r.failed?).to eq(true)
      expect(r.error).to eq('No db connection password!')
    ensure
      DesmondConfig.system_connection_allowed = prev_value
    end
  end

  it 'should complain about multiple queries/injection' do
    expect(run_export_test({ query: "SELECT 1 AS one; INSERT INTO" })).to eq({error: 'Query separator detected'})
  end

  it 'should be able to return test data' do
    expect(run_export_test[:columns]).to eq(['id', 'txt'])
    expect(run_export_test[:rows]).to match_array([['0', 'null'], ['1', 'eins']])
  end

  it 'should complain if query is missing' do
    expect(run_export_test({ query: nil })).to eq({error: 'Arguments cannot be nil'})
  end

  it 'should complain about invalid test database configuration' do
    expect(run_export_test({ connection_id: nil })).to eq({error: 'No connection id!'})
  end

  it 'should complain about invalid test database credentials' do
    prev_value = DesmondConfig.system_connection_allowed?
    begin
      DesmondConfig.system_connection_allowed = false
      expect(run_export_test({ username: nil })).to eq({error: 'No db connection username!'})
      expect(run_export_test({ username: 'test', password: nil })).to eq({error: 'No db connection password!'})
    ensure
      DesmondConfig.system_connection_allowed = prev_value
    end
  end

  it 'should use the app id in the export filename' do
    prev_value = DesmondConfig.app_id
    begin
      DesmondConfig.app_id = 'testapp1234567'
      expect(run_export.filename).to start_with(DesmondConfig.app_id)
    ensure
      DesmondConfig.app_id = prev_value
    end
  end
end
