module Desmond
  ##
  # job unloading RedShift data into S3
  #
  # Please see `BaseJob` class documentation on how to run
  # any job using its general interface.
  #
  class UnloadJob < BaseJob
    ##
    # runs an unload of a Redshift table into S3
    # see `BaseJob` for information on arguments except +options+.
    #
    # the following +options+ are required:
    # - db
    #   - connection_id: ActiveRecord connection id used to connect to database
    #   - username: database username
    #   - password: database password
    #   - schema: schema of table to unload
    #   - table: name of table to unload
    # - s3
    #   - access_key_id: s3 access key
    #   - secret_access_key: s3 secret key
    #   - bucket: bucket to place unloaded data into
    #   - prefix: prefix to append to s3 data stored
    #
    # the following +options+ are additionally supported:
    # - db
    #   - timeout: connection timeout to database
    # - unload: options for the Redshift UNLOAD command
    #   - allowoverwrite: if true, will use the ALLOWOVERWRITE unload option
    #   - gzip: if true, will use the GZIP unload option
    #   - addquotes: if true, will use the REMOVEQUOTES unload option
    #   - escape: if true, will use the ESCAPE unload option
    #   - null_as: string to use for the NULL AS unload option
    #
    def execute(job_id, user_id, options={})
      fail 'No database options!' if options[:db].nil?
      fail 'No s3 options!' if options[:s3].nil?

      # S3 location to store unloaded data
      bucket = options[:s3][:bucket]
      fail 'Empty bucket name!' if bucket.nil? || bucket.empty?
      prefix = options[:s3][:prefix]
      fail 'Empty prefix name!' if prefix.nil? || prefix.empty?

      # s3 credentials for the bucket to unload to
      access_key = options[:s3][:access_key_id]
      fail 'Empty access key!' if access_key.nil? || access_key.empty?
      secret_key = options[:s3][:secret_access_key]
      fail 'Empty secret key!' if secret_key.nil? || secret_key.empty?

      # construct full escaped table name
      schema_name = options[:db][:schema]
      fail 'Empty schema name!' if schema_name.nil? || schema_name.empty?
      table_name = options[:db][:table]
      fail 'Empty table name!' if table_name.nil? || table_name.empty?

      # execute UNLOAD sql
      bucket = PGUtil.escape_string(bucket)
      prefix = PGUtil.escape_string(prefix)
      access_key = PGUtil.escape_string(access_key)
      secret_key = PGUtil.escape_string(secret_key)
      full_table_name = PGUtil.get_escaped_table_name(options[:db], schema_name, table_name)
      unload_options = ''
      unless options[:unload].nil? || options[:unload].empty?
        unload_options += 'ALLOWOVERWRITE' if options[:unload][:allowoverwrite]
        unload_options += ' GZIP' if options[:unload][:gzip]
        unload_options += ' ADDQUOTES' if options[:unload][:addquotes]
        unload_options += ' ESCAPE' if options[:unload][:escape]
        unless options[:unload][:null_as].nil?
          unload_options += " NULL AS '#{PGUtil.escape_string(options[:unload][:null_as])}'"
        end
      end
      unload_sql = <<-SQL
          UNLOAD ('SELECT * FROM #{full_table_name}')
          TO 's3://#{bucket}/#{prefix}'
          CREDENTIALS 'aws_access_key_id=#{access_key};aws_secret_access_key=#{secret_key}'
          MANIFEST #{unload_options};
      SQL
      conn = PGUtil.dedicated_connection(options[:db])
      conn.exec(unload_sql)

      # done. return the location of the manifest file
      {manifest_file: "s3://#{bucket}/#{prefix}manifest"}
    end
  end
end
