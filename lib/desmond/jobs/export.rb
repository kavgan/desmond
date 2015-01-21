FETCH_SIZE = 10000 # default num of rows to be fetched by cursor
TEST_SIZE = 100 # default num of rows retuned by test
TIMEOUT = 5 # default connection timeout to database

module Desmond
  ##
  # job exporting data out of PostgreSQL compatible databases (e.g. AWS RedShift) into S3
  #
  class ExportJob < BaseJob
    def self.test(user_id, query, options={})
      options = ActiveSupport::HashWithIndifferentAccess.new(options)
      results = nil
      begin
        Que.log level: :info, msg: "Starting to test job for user #{user_id}"
        time = Time.now.utc.strftime('%Y_%m_%dT%H_%M_%S_%LZ')
        export_id = "desmond_validate_#{user_id}_#{time}"
        raise 'No database options!' if options['db'].nil?

        # read the test rows
        db_reader = database_reader(export_id, query, options.merge(fetch_size: TEST_SIZE))
        begin
          results = { values: db_reader.read }
        ensure
          db_reader.close
        end
      rescue => e
        results = { error: e.message }
      ensure
        Que.log level: :info, msg: "Done testing job for user #{user_id}"
      end
    end

    def run(job_id, user_id, query, options={})
      begin
        Que.log level: :info, msg: "Starting to execute export job #{job_id} for user #{user_id}"
        super(job_id, user_id, query, options)
        time = Time.now.utc.strftime('%Y_%m_%dT%H_%M_%S_%LZ')
        export_id = "desmond_export_#{job_id}_#{user_id}_#{time}"
        # check options
        raise 'No database options!' if options['db'].nil?
        raise 'No s3 options!' if options['s3'].nil?
        s3_bucket = options['s3']['bucket_name']
        s3_key = "#{export_id}.csv"
        raise 'No S3 export bucket!' if s3_bucket.nil? || s3_bucket.empty?

        # database reader, streams rows without loading everything in memory
        db_reader = self.class.database_reader(export_id, query, options)

        begin
          # csv reader, transforms database rows to csv
          csv_reader = CSVStreams::CSVRecordHashReader.new(db_reader,
            delimiter: options['delimiter'],
            include_headers: options['include_header'])

          # stream write to S3 from csv reader
          s3writer = CSVStreams::S3Writer.new(s3_bucket, s3_key, options['s3'])
          s3writer.write_from(csv_reader)
        ensure
          db_reader.close
        end

        # everything is done, send emails and remove the job
        details = { bucket: s3_bucket, key: s3_key, aceess_key: options['s3']['access_key_id'] }
        self.done(DesmondConfig.mail_export_success(options['job'].merge(url: s3writer.public_url)), details)
      rescue => e
        # error occurred
        details = { error: e.message }
        Que.log level: :error, exception: details[:error]
        Que.log level: :error, backtrace: e.backtrace.join("\n ")
        self.failed(DesmondConfig.mail_export_failure(options['job']).merge(details), details)
      ensure
        Que.log level: :info, msg: "Done executing export job #{job_id} for user #{user_id}"
      end
    end

    private
      def self.database_reader(id, query, options)
        raise 'Arguments cannot be nil' if id.nil? || query.nil? || options.nil?
        raise 'No database options!' if options['db'].nil?
        CSVStreams::PGCursorReader.new(
          id,
          query,
          options['db'].merge(
            fetch_size: FETCH_SIZE,
            timeout: TIMEOUT
        ))
      end
  end
end
