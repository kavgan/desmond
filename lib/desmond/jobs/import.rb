module Desmond
  ##
  # job importing data into AWS RedShift or Postgres from S3.
  #
  # Please see `BaseJob` class documentation on how to run
  # any job using its general interface.
  #
  class ImportJob < BaseJob
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

      # redshift supports schemas, postgres doesn't
      schema_name = options[:db][:schema] || ''
      schema_name = PGUtil.escape_identifier(schema_name) unless schema_name.empty?
      schema_name = '' if self.class.get_database_adapter(options) != 'redshift'

      # retrieve table name
      table_name = options[:db][:table]
      fail 'Empty table name!' if table_name.nil? || table_name.empty?
      table_name = PGUtil.escape_identifier(table_name)

      # construct full escaped table name
      full_table_name = "#{schema_name}.#{table_name}" unless schema_name.empty?
      full_table_name = table_name if schema_name.empty?

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
        adapter = self.class.get_database_adapter(options)
        if adapter == 'postgresql'
          import_general(conn, full_table_name, r, s, options)
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
      copy_columns = array_reader.headers.map { |header| PGUtil.escape_identifier(header) }.join(', ')
      s3_credentials = PGUtil.escape_string(s3_reader.credentials)
      csv_col_sep = PGUtil.escape_string(array_reader.options[:col_sep])
      csv_quote_char = PGUtil.escape_string(array_reader.options[:quote_char])
      skip_rows = options.fetch(:csv, {})[:skip_rows].to_i
      copy_sql = <<-SQL
        COPY #{table_name}(#{copy_columns})
        FROM 's3://#{s3_reader.bucket}/#{s3_reader.key}'
        WITH CREDENTIALS AS '#{s3_credentials}'
        TRIMBLANKS
        CSV
        QUOTE '#{csv_quote_char}'
        DELIMITER '#{csv_col_sep}'
        #{(skip_first_row) ? ' IGNOREHEADER 1' : ''}
        ;
      SQL
      conn.exec(copy_sql)
    end

    def import_general(conn, table_name, array_reader, s3_reader, options={})
      insert_columns = array_reader.headers.map do |header|
        PGUtil.escape_identifier(header)
      end.join(', ')
      while !array_reader.eof?
        row = array_reader.read
        insert_sql  = "INSERT INTO #{table_name} ("
        insert_sql += insert_columns
        insert_sql += ') VALUES('
        insert_sql += row.map { |column| "'" + PGUtil.escape_string(column) + "'" } .join(', ')
        insert_sql += ');'
        conn.exec(insert_sql)
      end
    end

    def self.get_database_adapter(options)
      fail 'No database options!' if options[:db].nil?
      config_name = options[:db][:connection_id]
      fail 'No connection id!' if config_name.nil? || config_name.empty?
      conf = ActiveRecord::Base.configurations[config_name.to_s]
      fail "Connection configuration '#{config_name.to_s}' not found" if conf.nil? || conf.empty?
      conf['adapter']
    end
  end
end
