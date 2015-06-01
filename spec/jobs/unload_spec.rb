require_relative '../spec_helper'

describe Desmond::UnloadJob do
  RS_CONN_ID = 'redshift_test'
  let (:conn) { Desmond::PGUtil.dedicated_connection(connection_id: RS_CONN_ID) }

  #
  # Runs an UnloadJob with the specified options and returns the job run.
  #
  def run_unload(options)
    return Desmond::UnloadJob.enqueue('UserId', options)
  end

  #
  # Checks if the result of an UnloadJob fully succeeded.
  #
  def check_success(result, options)
    manifest_s3_key = "#{options[:s3][:prefix]}manifest"
    expect(result.failed?).to(eq(false), "Error: #{result.error}")
    expect(result.result['manifest_file']).to eq("s3://#{options[:s3][:bucket]}/#{manifest_s3_key}")

    # Ensure manifest file exists.
    bucket = AWS::S3.new.buckets[options[:s3][:bucket]]
    expect(bucket.objects[manifest_s3_key].exists?).to eq(true)
    # Ensure S3 files hold the table data.
    data = []
    bucket.objects.each do |obj|
      if obj.key =~ /#{Regexp.quote(@full_table_name)}[0-9]*_part_[0-9]*/
        obj.read.split("\n").each do |line|
          d = line.split('|')
          data << line.split('|') unless d.empty?
        end
      end
    end
    expect(data.sort_by! { |obj| obj[0] }).to eq([%w{"0" "hello"}, %w{"1" "privyet"}, %w{"2" "NULL"}])
  end

  #
  # helper function to keep track of what options where used
  #
  def merge_options(options={})
    return {
        db: {
            connection_id: @connection_id,
            username: @config[:unload_username],
            password: @config[:unload_password],
            schema: @config[:unload_schema],
            table: nil
        },
        s3: {
            access_key_id: @config[:access_key_id],
            secret_access_key: @config[:secret_access_key],
            bucket: @config[:unload_bucket],
            prefix: nil
        },
        unload: {
            allowoverwrite: true,
            gzip: false,
            addquotes: true,
            escape: true,
            null_as: 'NULL'
        }
    }.deep_merge(options)
  end

  it 'should succeed with basic options' do
    options = merge_options({db: {table: @table},
               s3: {prefix: @full_table_name}})
    check_success(run_unload(options), options)
  end

  before(:each) do
    @connection_id = 'redshift_test'
    @schema = @config[:unload_schema]
    @table = "unload_test_#{Time.now.to_i}_#{rand(1024)}"
    @full_table_name = "#{@schema}.#{@table}"
    create_sql = <<-SQL
        CREATE TABLE #{@full_table_name}(id INT, txt VARCHAR);
        INSERT INTO #{@full_table_name} VALUES (0, 'hello'), (1, 'privyet'), (2, null);
    SQL
    conn.exec(create_sql)
    @bucket = AWS::S3.new.buckets[@config[:unload_bucket]]
  end

  after(:each) do
    # Drop test redshift table.
    conn.exec("DROP TABLE IF EXISTS #{@full_table_name}")
    # Clean up S3 files.
    @bucket.objects.with_prefix(@full_table_name).delete_all
  end

end
