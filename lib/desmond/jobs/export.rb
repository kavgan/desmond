FETCH_SIZE = 10000 # default num of rows to be fetched by cursor
TEST_SIZE = 100 # default num of rows retuned by test
TIMEOUT = 5 # default connection timeout to database

module Desmond
  ##
  # job exporting data out of PostgreSQL compatible databases (e.g. AWS RedShift) into S3
  #
  # Please see `BaseJob` class documentation on how to run
  # any job using its general interface.
  #
  class ExportJob < BaseJob
    ##
    # tests an export, only returning one patch of database rows directly
    # see `BaseJob` for information on arguments except +options+.
    #
    # the following +options+ are required:
    # - connection_id: ActiveRecord connection id to be used
    # - query: query to be exported (only SELECT or VALUES queries, supported)
    #
    # the following options are required when `DesmondConfig.system_connection_allowed?` is false:
    # - username: database username
    # - password: database password
    #
    # the following +options+ are additionally supported:
    # - fetch_size: how many rows are retrieved in one iteration
    # - timeout: connection timeout to database
    #
    def self.test(user_id, options={})
      options = ActiveSupport::HashWithIndifferentAccess.new(options)
      results = nil
      begin
        Que.log level: :info, msg: "Starting to test job for user #{user_id}"
        time = Time.now.utc.strftime('%Y_%m_%dT%H_%M_%S_%LZ')
        export_id = "#{DesmondConfig.app_id}_validate_#{user_id}_#{time}"

        # read the test rows
        db_reader = database_reader(export_id, options[:query], { fetch_size: TEST_SIZE }.merge(options))
        begin
          results = { columns: db_reader.columns, rows: db_reader.read }
        ensure
          db_reader.close
        end
      rescue => e
        Que.log level: :error, msg: e.message
        Que.log level: :error, msg: e.backtrace.join("\n ")
        results = { error: e.message }
      ensure
        Que.log level: :info, msg: "Done testing job for user #{user_id}"
      end
      results
    end

    ##
    # runs an export
    # see `BaseJob` for information on arguments except +options+.
    #
    # the following +options+ are required:
    # - db
    #   - connection_id: ActiveRecord connection id to be used
    #   - query: query to be exported
    # - s3
    #   - bucket: bucket where exported csv filfe should be stored
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
    #   - fetch_size: how many rows are retrieved in one iteration
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
    def execute(job_id, user_id, options={})
      job_run_filename = job_run.filename
      time = job_run.queued_at.utc.strftime('%Y_%m_%dT%H_%M_%S_%LZ')
      export_id = "#{DesmondConfig.app_id}_export_#{job_id}_#{user_id}_#{time}"
      # check options
      fail 'No database options!' if options[:db].nil?
      fail 'No s3 options!' if options[:s3].nil?
      s3_bucket = options[:s3][:bucket]
      s3_key = options[:s3][:key] || job_run_filename
      fail 'No S3 export bucket!' if s3_bucket.nil? || s3_bucket.empty?

      csv_reader = nil
      begin
        # csv reader, transforms database rows to csv
        db_reader = Streams::Database::PGCursorReader.new(
          export_id,
          options[:db][:query],
          {
            fetch_size: FETCH_SIZE,
            timeout: TIMEOUT
          }.deep_merge(options.fetch(:db, {}))
        )

        # stream write to S3 from csv writer

        csv_writer = Streams::CSV::CSVWriter.new(
          Streams::S3::S3Writer.new(s3_bucket, s3_key, options[:s3]),
          { headers: db_reader.columns }.merge(options.fetch(:csv, {}))
        )
        Streams::Utils.pipe(db_reader, csv_writer)
      ensure
        db_reader.close unless db_reader.nil?
        csv_writer.close unless csv_writer.nil?
      end

      # everything is done
      { bucket: s3_bucket, key: s3_key, access_key: options[:s3][:access_key_id] }
    end

    def self.database_reader(id, query, options)
      fail 'Arguments cannot be nil' if id.nil? || query.nil? || options.nil?
      Streams::Database::PGCursorReader.new(
        id,
        query,
        {
          fetch_size: FETCH_SIZE,
          timeout: TIMEOUT
        }.merge(options))
    end
    private_class_method :database_reader
  end
end
