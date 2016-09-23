module Desmond
  ##
  # job exporting data out of AWS RedShift into S3
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
      PGExportJob.test(user_id, options)
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
    # - csv (see ruby's CSV documentation)
    #   - col_sep
    #   - return_headers
    #   - quote_char
    #
    def execute
      # check options
      fail ArgumentError, 'No database options!' if options[:db].nil?
      if PGUtil.get_database_adapter(options[:db]).to_sym == :postgresql
        return PGExportJob.run(self.job_id, self.user_id, self.options)
      end
      fail ArgumentError, 'No s3 options!' if options[:s3].nil?
      aws_credentials = options[:s3].select { |key| [:access_key_id, :secret_access_key].include?(key) }
      s3_bucket = options[:s3][:bucket]
      s3_key = options[:s3][:key] || job_run.filename
      s3_key_parallel_unload = "export_tmp_#{s3_key}/"
      fail ArgumentError, 'No S3 export bucket!' if s3_bucket.nil? || s3_bucket.empty?

      raw_query = self.options[:db][:query].strip
      raw_query = raw_query[0..-2] if raw_query.end_with?(';')
      fail ArgumentError, "Can't use query separator!" unless raw_query.index(';').nil?
      unload_query  = "select * from (#{raw_query})"
      # this is valid in PG & Redshift and won't use any resources compared to limit 1
      headers_query = "select * from (#{raw_query}) limit 0"
      s3_bucket_obj = Aws::S3::Bucket.new(s3_bucket)
      col_sep = self.options.fetch(:csv, {})[:col_sep] || '|'

      # do a parallel unload of all the data
      UnloadJob.run(self.job_id, self.user_id, {
        db: self.options[:db].merge({ query: unload_query }),
        s3: self.options[:s3].merge({ prefix: s3_key_parallel_unload }),
        unload: {
          nomanifest: true,
          gzip: false,
          delimiter: col_sep,
          null_as: ''
        }
      })


      # deal with headers separatly if requested
      if options.fetch(:csv, {})[:return_headers]
        # get the headers of the query
        rs_conn = PGUtil.dedicated_connection(self.options[:db])
        headers = rs_conn.exec(headers_query).try(:fields)
        # write headers to S3 file to be merged
        s3_bucket_obj.object(s3_key_parallel_unload + '00000__headers').put(
          body: headers.join(col_sep) + "\n"
        )
      end

      # merge the parallel unloaded files on the S3 server
      S3Util.merge_objects(s3_bucket, s3_key_parallel_unload, s3_bucket, s3_key, aws_credentials)

      # everything is done
      { bucket: s3_bucket, key: s3_key, access_key: options[:s3][:access_key_id] }
    ensure
      rs_conn.close unless rs_conn.nil?
      # delete the parallel unloaded files
      s3_bucket_obj.objects(prefix: s3_key_parallel_unload).each(&:delete) unless s3_bucket_obj.nil?
    end
  end
end
