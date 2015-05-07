module Desmond
  ##
  # job importing data into AWS RedShift or Postgres from S3.
  #
  # Please see `BaseJob` class documentation on how to run
  # any job using its general interface.
  #
  class ImportJob < BaseJob
    SAFE_ROW_SEP = "\n"
    ##
    # runs an import
    # see `BaseJob` for information on arguments except +options+.
    #
    # the following +options+ are required:
    # - db
    #   - connection_id: ActiveRecord connection id used to connect to database
    #   - schema: schema of import table (not supported if using PostgreSQL)
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

      # get s3 file location
      bucket = PGUtil.escape_string(options[:s3][:bucket])
      s3_key = PGUtil.escape_string(options[:s3][:key])

      # retrieve escaped table name
      schema_name = options[:db][:schema] || ''
      table_name = options[:db][:table]
      fail 'Empty table name!' if table_name.nil? || table_name.empty?
      full_table_name = PGUtil.get_escaped_table_name(options[:db], schema_name, table_name)

      # connect to database
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
        column_types = options[:db][:types] || []
        create_table_sql  = "CREATE TABLE #{full_table_name} ("
        create_table_sql += headers.zip(column_types).map do |header, type|
          column_type = 'VARCHAR'
          column_type = type unless type.nil?
          "#{PGUtil.escape_identifier(header)} #{column_type}"
        end.join(',')
        create_table_sql += ');'

        # drop and then create table
        conn.exec("DROP TABLE IF EXISTS #{full_table_name};") if options[:db][:dropifexists]
        conn.exec(create_table_sql)

        # actually start inserting
        adapter = PGUtil.get_database_adapter(options[:db])
        if adapter == 'postgresql'
          import_general(conn, table_name, full_table_name, r, s, options)
        elsif adapter == 'redshift'
          import_redshift(conn, full_table_name, r, s, options)
        else
          fail "Unknown database adapter '#{adapter}'"
        end

        # done return table we used
        { table: full_table_name }
      end
    ensure
      conn.close unless conn.nil?
      r.close unless r.nil?
    end

    private

    def import_redshift(conn, table_name, array_reader, s3_reader, options={})
      s3_bucket = s3_reader.bucket
      s3_key    = s3_reader.key
      if array_reader.options[:row_sep] != SAFE_ROW_SEP
        csv_options = array_reader.options
        skip_rows = options.fetch(:csv, {})[:skip_rows]
        hdrs      = options.fetch(:csv, {})[:headers]
        if (!skip_rows.nil? && skip_rows > 0) || (!hdrs.nil? && hdrs == :first_row)
          csv_options = csv_options.merge(return_headers: true)
        end
        s3_key = rewrite_weird_newlines(s3_bucket, s3_key, array_reader,
          s3: options[:s3], csv: csv_options)
      end
      copy_columns = array_reader.headers.map { |header| PGUtil.escape_identifier(header) }.join(', ')
      s3_credentials = PGUtil.escape_string(s3_reader.credentials)
      csv_col_sep = PGUtil.escape_string(array_reader.options[:col_sep])
      csv_quote_char = PGUtil.escape_string(array_reader.options[:quote_char])
      skip_rows = 1 if options.fetch(:csv, {})[:headers] == :first_row
      skip_rows = options.fetch(:csv, {})[:skip_rows].to_i if options.fetch(:csv, {}).has_key?(:skip_rows)
      copy_sql = <<-SQL
        COPY #{table_name}(#{copy_columns})
        FROM 's3://#{s3_bucket}/#{s3_key}'
        WITH CREDENTIALS AS '#{s3_credentials}'
        TRIMBLANKS
        CSV
        QUOTE '#{csv_quote_char}'
        DELIMITER '#{csv_col_sep}'
        #{(skip_rows) ? " IGNOREHEADER #{skip_rows}" : ''}
        ;
      SQL
      conn.exec(copy_sql)
    end

    def import_general(conn, raw_table_name, table_name, array_reader, s3_reader, options={})
      statement_name = "desmond_#{raw_table_name}"
      placeholders = []
      i = 1
      headers = array_reader.headers
      insert_columns = headers.map do |header|
        placeholders << "$#{i.to_i}"
        i += 1
        PGUtil.escape_identifier(header)
      end.join(', ')
      placeholders = placeholders.join(', ')
      conn.prepare(statement_name, "insert into #{table_name} (#{insert_columns}) values (#{placeholders})")
      while !array_reader.eof?
        row = array_reader.read
        fail ArgumentError, "wrong number of columns. found #{row.size}, expected: #{headers.size}" if row.size != headers.size
        conn.exec_prepared(statement_name, row)
      end
    ensure
      # we don't need the prepared statement anymore
      conn.exec("deallocate #{statement_name}") unless conn.transaction_status == PG::Constants::PQTRANS_INERROR
    end

    ##
    # rewrites csv file which use weird newline separator's to use \n
    # returns s3 key of rewritten file in same folder
    #
    def rewrite_weird_newlines(s3_bucket, s3_key, csv_reader, options={})
      s3_key_ext = File.extname(s3_key)
      new_s3_key = File.join(File.dirname(s3_key), File.basename(s3_key, s3_key_ext) + 'rewrite' + s3_key_ext)
      s3writer = Streams::S3::S3Writer.new(s3_bucket, new_s3_key, options[:s3])
      s3writer.write_from(Streams::CSV::CSVStringWriter.new(csv_reader, options.fetch(:csv).merge(row_sep: SAFE_ROW_SEP)))
      return new_s3_key
    end
  end
end
