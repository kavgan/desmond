module Desmond
  ##
  # 'pg' gem utility functions
  #
  class PGUtil
    ##
    # returns a dedicted pg connection
    # required +options+:
    # - connection_id: ActiveRecord connection id configuration to clone
    # required +options+ if `DesmondConfig.system_connection_allowed?` is false:
    # - username: custom username to use for connection
    # - password: custom password to use for connection
    # optional +options+
    # - username: custom username to use for connection
    # - password: custom password to use for connection
    # - timeout: connection timeout to use
    #
    def self.dedicated_connection(options={})
      ar_config = options[:connection_id]
      username = options[:username]
      password = options[:password]
      fail 'No connection id!' if ar_config.nil? || ar_config.empty?
      conf = ActiveRecord::Base.configurations[ar_config.to_s]
      fail ArgumentError, "Connection configuration '#{ar_config.to_s}' not found" if conf.nil? || conf.empty?
      if !DesmondConfig.system_connection_allowed? && !options[:system_connection_allowed]
        fail ArgumentError, 'No db connection username!' if username.nil? || username.empty?
        fail ArgumentError, 'No db connection password!' if password.nil? || password.empty?
      else
        username ||= conf['username']
        password ||= conf['password']
      end
      # construct connection config with the provided credentials
      fail ArgumentError, "Connection configuration '#{ar_config}' not found" if conf.nil?
      PG.connect(
        host: conf['host'],
        port: conf['port'],
        user: username,
        password: password,
        dbname: conf['database'],
        connect_timeout: options['timeout']
      )
    end

    ##
    # escapes aregular string for SQL
    #
    def self.escape_string(str)
      PG::Connection.escape_string(str)
    end

    ##
    # escapes a SQL identifier
    #
    def self.escape_identifier(str)
      PG::Connection.quote_ident(str)
    end

    ##
    # listens to notification +channel+ in postgres.
    # waits a maximum of +timeout+ seconds (can be a decimal)
    # +connection+ must be a postgres connection either from
    # the pg gem or ActiveRecord.
    # returns true/false depending if notification was received or
    # timeout reached.
    #
    def self.listen(connection, channel, timeout=nil)
      pg_conn = get_pg_connection(connection)
      escaped_channel = self.escape_identifier(channel)
      pg_conn.exec("LISTEN #{escaped_channel}")
      return !pg_conn.wait_for_notify(timeout).nil?
    ensure
      pg_conn.exec("UNLISTEN *") unless pg_conn.nil?
    end

    ##
    # send a postgres notification +channel+.
    # +connection+ must be a postgres connection either from
    # the pg gem or ActiveRecord.
    #
    def self.notify(connection, channel)
      escaped_channel = self.escape_identifier(channel)
      get_pg_connection(connection).exec("NOTIFY #{escaped_channel}")
    end

    ##
    # do a 'copy from stdin' through the postgresql connection.
    # send the given COPY query +copy_query+ through +connection+ and
    # afterwards calls the supplied block until it returns nil or an
    # emptry string, any other return value is send through the
    # +connection+ for the COPY command
    #
    def self.copy_from(connection, copy_query, &block)
      data = nil
      connection = get_pg_connection(connection)
      connection.copy_data(copy_query) do
        loop do
          data = block.call unless block.nil?
          connection.put_copy_data(data) unless data.blank?
          break if data.blank?
        end
      end
    end

    ##
    # returns the database adapter used with the given database +options+
    #
    def self.get_database_adapter(options)
      config_name = options[:connection_id]
      fail 'No connection id!' if config_name.nil? || config_name.empty?
      conf = ActiveRecord::Base.configurations[config_name.to_s]
      fail "Connection configuration '#{config_name.to_s}' not found" if conf.nil? || conf.empty?
      conf['adapter']
    end

    ##
    # returns an escaped version of the given table name
    # dependent on the database adapter used.
    # - pg doesn't have schema's etc
    #
    def self.get_escaped_table_name(db_options, schema_name, table_name)
      adapter = self.get_database_adapter(db_options)
      if adapter == 'postgresql'
        get_escaped_table_name_pg(table_name)
      elsif adapter == 'redshift'
        get_escaped_table_name_rs(schema_name, table_name)
      else
        fail ArgumentError, "Unknown database adapter '#{adapter}'"
      end
    end

    ##
    # returns an escaped table name for a postgresql database
    #
    def self.get_escaped_table_name_pg(table_name)
      Desmond::PGUtil.escape_identifier(table_name)
    end
    private_class_method :get_escaped_table_name_pg

    ##
    # returns an escaped table name for a redshift database
    #
    def self.get_escaped_table_name_rs(schema_name, table_name)
      schema_name = Desmond::PGUtil.escape_identifier(schema_name)
      table_name  = Desmond::PGUtil.escape_identifier(table_name)
      "#{schema_name}.#{table_name}"
    end
    private_class_method :get_escaped_table_name_rs

    ##
    # extracts PG::Connection out of ActiveRecord +connection+,
    # or returns straigt if already a PG::Connection.
    # raises exception otherwise.
    #
    def self.get_pg_connection(connection)
      if connection.is_a?(ActiveRecord::ConnectionAdapters::AbstractAdapter)
        get_pg_connection(connection.instance_variable_get(:@connection))
      elsif connection.is_a?(PG::Connection)
        connection
      else
        fail ArgumentError, 'Unsupported connection!'
      end
    end
    private_class_method :get_pg_connection
  end
end
