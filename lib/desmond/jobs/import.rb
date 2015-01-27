module Desmond
  ##
  # job importing data into PostgreSQL compatible databases (e.g. AWS RedShift) from S3
  #
  class ImportJob < BaseJob
    def run(job_id, user_id, options={})
      begin
        Que.log level: :info, msg: "Starting to execute import job #{job_id} for user #{user_id}"
        super(job_id, user_id, options)

        raise 'No database options!' if options[:db].nil?
        raise 'No s3 options!' if options[:s3].nil?
        bucket = options[:s3][:bucket_name]
        s3_key = options[:s3][:key]
        table_name = options[:db][:table]

        # open S3 CSV file
        s = Desmond::Streams::S3::S3Reader.new(
          bucket,
          s3_key,
          options[:s3])
        r = Desmond::Streams::CSV::CSVArrayReader.guess_and_create(s, options.fetch(:csv, {}))
        headers = r.headers
        raise 'No CSV headers!' if headers.empty?

        # construct create table query out of CSV headers
        create_table_sql  = "CREATE TABLE \"#{table_name}\"("
        create_table_sql += headers.map { |header| "\"#{header}\" VARCHAR" }.join(',')
        create_table_sql += ");"

        # create table in database
        conn = self.class.dedicated_connection(options[:db])
        if options[:db][:dropifexists]
          conn.exec("DROP TABLE IF EXISTS \"#{table_name}\";")
        end
        conn.exec(create_table_sql)
        copy_sql = "COPY \"#{table_name}\" FROM 's3://#{bucket}/#{s3_key}' WITH CREDENTIALS AS '#{s.credentials}'#{(options[:csv][:headers] == :first_row) ? ' IGNOREHEADER 1' : ''};"
        conn.exec(copy_sql)
      ensure
        Que.log level: :info, msg: "Done executing import job #{job_id} for user #{user_id}"
      end
    end

    private
      def self.dedicated_connection(options)
        ar_config = options[:connection_id]
        username = options[:username]
        password = options[:password]
        raise 'No connection id!' if ar_config.nil? || ar_config.empty?
        raise 'No db connection username!' if username.nil? || username.empty?
        raise 'No db connection password!' if password.nil? || password.empty?
        # construct connection config with the provided credentials
        conf = ActiveRecord::Base.configurations[ar_config.to_s]
        raise "Connection configuration '#{ar_config}' not found" if conf.nil?
        PG.connect(host: conf['host'],
          port: conf['port'],
          user: username,
          password: password,
          dbname: conf['database'],
          connect_timeout: options['timeout'])
      end
  end
end
