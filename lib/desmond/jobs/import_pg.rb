module Desmond
  ##
  # job importing data into Postgres from S3
  # only used for faster testing, can probably be optimized (inserts one row at a time)
  #
  # Please see `BaseJob` class documentation on how to run
  # any job using its general interface.
  #
  class ImportPgJob < BaseJob
    ##
    # runs an import
    # see `BaseJob` for information on arguments except +options+.
    #
    # the following +options+ are required:
    # - db
    #   - connection_id: ActiveRecord connection id used to connect to database
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
    #   - skip_header_row (additional option to skip header row regardless of headers option)
    #
    def execute(job_id, user_id, options={})
      fail 'No database options!' if options[:db].nil?
      fail 'No s3 options!' if options[:s3].nil?
      bucket = PGUtil.escape_string(options[:s3][:bucket])
      s3_key = PGUtil.escape_string(options[:s3][:key])
      table_name = options[:db][:table]
      fail 'Empty table name!' if table_name.nil? || table_name.empty?
      table_name = PGUtil.escape_identifier(table_name)

      # create table in database
      conn = PGUtil.dedicated_connection(options[:db])
      r = nil
      conn.transaction do
        # open S3 CSV file
        s = Desmond::Streams::S3::S3Reader.new(
          bucket,
          s3_key,
          options[:s3])
        r = Desmond::Streams::CSV::CSVArrayReader.guess_and_create(s, options.fetch(:csv, {}))
        headers = r.headers
        fail 'No CSV headers!' if headers.empty?

        # construct create table query out of CSV headers
        create_table_sql  = "CREATE TABLE #{table_name} ("
        create_table_sql += headers.map { |header| "#{PGUtil.escape_identifier(header)} VARCHAR" }.join(',')
        create_table_sql += ');'

        if options[:db][:dropifexists]
          conn.exec("DROP TABLE IF EXISTS #{table_name};")
        end
        conn.exec(create_table_sql)

        insert_columns = headers.map { |header| PGUtil.escape_identifier(header) }.join(', ')
        while !r.eof?
          row = r.read
          insert_sql  = "INSERT INTO #{table_name} ("
          insert_sql += insert_columns
          insert_sql += ') VALUES('
          insert_sql += row.map { |column| "'" + PGUtil.escape_string(column) + "'" } .join(', ')
          insert_sql += ');'
          conn.exec(insert_sql)
        end

        { table: table_name }
      end
    ensure
      conn.close unless conn.nil?
      r.close unless r.nil?
    end
  end
end
