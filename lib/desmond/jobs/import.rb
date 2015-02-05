module Desmond
  ##
  # job importing data into AWS RedShift from S3
  #
  class ImportJob < BaseJob
    ##
    # runs an import
    # see `BaseJob` for information on arguments except +options+.
    #
    # the following +options+ are required:
    # - db
    #   - connection_id: ActiveRecord connection id used to connect to database
    #   - schema: schema of import table
    #   - table: name of import table
    # - s3
    #   - bucket: bucket of csv file to be imported
    #   - key: key of csv file to be imported
    #   - access_key_id, if not configured globally
    #   - secret_access_key, if not configured globally
    #
    # the following options are required when `DesmondConfig.system_connection_allowed?` is false:
    # - db
    #   - username: database username
    #   - password: database password
    #
    # the following +options+ are additionally supported:
    # - db
    #   - dropifexists: drop import table if it exists
    #   - timeout: connection timeout to database
    # - s3
    #   - everything supported by AWS::S3.new
    # - csv (see ruby's CSV documentation)
    #   - col_sep
    #   - row_sep
    #   - headers
    #   - return_headers
    #   - quote_char
    #
    def run(job_id, user_id, options={})
      Que.log level: :info, msg: "Starting to execute import job #{job_id} for user #{user_id}"
      super(job_id, user_id, options)

      fail 'No database options!' if options[:db].nil?
      fail 'No s3 options!' if options[:s3].nil?
      bucket = PGUtil.escape_string(options[:s3][:bucket])
      s3_key = PGUtil.escape_string(options[:s3][:key])
      schema_name = options[:db][:schema] || ''
      table_name = options[:db][:table]
      fail 'Empty table name!' if table_name.nil? || table_name.empty?
      schema_name = PGUtil.escape_identifier(schema_name) unless schema_name.empty?
      table_name = PGUtil.escape_identifier(table_name)
      full_table_name = "#{schema_name}.#{table_name}" unless schema_name.empty?
      full_table_name = table_name if schema_name.empty?

      # open S3 CSV file
      s = Desmond::Streams::S3::S3Reader.new(
        bucket,
        s3_key,
        options[:s3])
      r = Desmond::Streams::CSV::CSVArrayReader.guess_and_create(s, options.fetch(:csv, {}))
      headers = r.headers
      fail 'No CSV headers!' if headers.empty?

      # construct create table query out of CSV headers
      create_table_sql  = "CREATE TABLE #{full_table_name} ("
      create_table_sql += headers.map { |header| "#{PGUtil.escape_identifier(header)} VARCHAR" }.join(',')
      create_table_sql += ');'

      # create table in database
      conn = PGUtil.dedicated_connection(options[:db])
      if options[:db][:dropifexists]
        conn.exec("DROP TABLE IF EXISTS #{full_table_name};")
      end
      conn.exec(create_table_sql)
      copy_columns = headers.map { |header| PGUtil.escape_identifier(header) }.join(', ')
      s3_credentials = PGUtil.escape_string(s.credentials)
      csv_col_sep = PGUtil.escape_string(r.options[:col_sep])
      csv_quote_char = PGUtil.escape_string(r.options[:quote_char])
      copy_sql = "COPY #{full_table_name}(#{copy_columns}) FROM 's3://#{bucket}/#{s3_key}' WITH CREDENTIALS AS '#{s3_credentials}' TRIMBLANKS CSV QUOTE '#{csv_quote_char}' DELIMITER '#{csv_col_sep}'#{(options[:csv][:headers] == :first_row) ? ' IGNOREHEADER 1' : ''};"
      conn.exec(copy_sql)

      self.done
    rescue => e
      # error occurred
      details = { error: e.message }
      Que.log level: :error, exception: details[:error]
      Que.log level: :error, backtrace: e.backtrace.join("\n ")
      self.failed(nil, details)
    ensure
      Que.log level: :info, msg: "Done executing import job #{job_id} for user #{user_id}"
    end
  end
end
