FETCH_SIZE = 10000 # default num of rows to be fetched by cursor
TEST_SIZE = 100 # default num of rows retuned by test
TIMEOUT = 5 # default connection timeout to database

module Desmond
  ##
  # job exporting data out of PostgreSQL compatible databases (e.g. AWS RedShift) into S3
  #
  class ExportJob < BaseJob
    ##
    # tests an export, only returning one patch of database rows directly
    # see `BaseJob` for information on arguments except +options+.
    #
    # the following +options+ are required:
    # - connection_id: ActiveRecord connection id used for everything except username & password
    # - username
    # - password
    # - query
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
        db_reader = database_reader(export_id, options[:query], {fetch_size: TEST_SIZE}.merge(options))
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
    end

    ##
    # runs an export
    # see `BaseJob` for information on arguments except +options+.
    #
    # the following +options+ are required:
    # - db
    #   - connection_id: ActiveRecord connection id used for everything except username & password
    #   - username
    #   - password
    #   - query
    # - s3
    #   - bucket
    #   - access_key_id, if not configured globally
    #   - secret_access_key, if not configured globally
    #
    # the following +options+ are additionally supported:
    # - db
    #   - fetch_size: how many rows are retrieved in one iteration
    #   - timeout: connection timeout to database
    # - s3
    #   - everything supported by AWS::S3.new
    # - job
    #   - mail_success: comma-separated emails to notify on success
    #   - mail_failure: comma-separated emails to notify on failure
    #   - everything else is passed to the email template as variables
    # - csv (see ruby's CSV documentation)
    #   - col_sep
    #   - row_sep
    #   - headers
    #   - return_headers
    #   - quote_char
    #
    def run(job_id, user_id, options={})
      begin
        Que.log level: :info, msg: "Starting to execute export job #{job_id} for user #{user_id}"
        options = options.deep_symbolize_keys
        super(job_id, user_id, options)
        time = Time.now.utc.strftime('%Y_%m_%dT%H_%M_%S_%LZ')
        export_id = "#{DesmondConfig.app_id}_export_#{job_id}_#{user_id}_#{time}"
        # check options
        raise 'No database options!' if options[:db].nil?
        raise 'No s3 options!' if options[:s3].nil?
        s3_bucket = options[:s3][:bucket]
        s3_key = "#{export_id}.csv"
        raise 'No S3 export bucket!' if s3_bucket.nil? || s3_bucket.empty?

        csv_reader = nil
        begin
          # csv reader, transforms database rows to csv
          csv_reader = Streams::Database::PGCursorReader.create_csv_reader(
            export_id,
            options[:db][:query],
            {
              db: {
                fetch_size: FETCH_SIZE,
                timeout: TIMEOUT
              }
            }.deep_merge(options)
          )

          # stream write to S3 from csv reader
          s3writer = Streams::S3::S3Writer.new(s3_bucket, s3_key, options[:s3])
          s3writer.write_from(csv_reader)
        ensure
          csv_reader.close if not(csv_reader.nil?)
        end

        # everything is done, send emails and remove the job
        details = { bucket: s3_bucket, key: s3_key, aceess_key: options[:s3][:access_key_id] }
        self.done(DesmondConfig.mail_export_success(options.fetch(:job, {}).merge(url: s3writer.public_url)), details)
      rescue => e
        # error occurred
        details = { error: e.message }
        Que.log level: :error, exception: details[:error]
        Que.log level: :error, backtrace: e.backtrace.join("\n ")
        self.failed(DesmondConfig.mail_export_failure(options.fetch(:job, {})).merge(details), details)
      ensure
        Que.log level: :info, msg: "Done executing export job #{job_id} for user #{user_id}"
      end
    end

    private
      def self.database_reader(id, query, options)
        raise 'Arguments cannot be nil' if id.nil? || query.nil? || options.nil?
        Streams::Database::PGCursorReader.new(
          id,
          query,
          {
            fetch_size: FETCH_SIZE,
            timeout: TIMEOUT
          }.merge(options))
      end
  end
end
