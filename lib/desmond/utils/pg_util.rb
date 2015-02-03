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
    def self.dedicated_connection(options)
      ar_config = options[:connection_id]
      username = options[:username]
      password = options[:password]
      conf = ActiveRecord::Base.configurations[ar_config.to_s]
      fail 'No connection id!' if ar_config.nil? || ar_config.empty?
      if !DesmondConfig.system_connection_allowed?
        fail 'No db connection username!' if username.nil? || username.empty?
        fail 'No db connection password!' if password.nil? || password.empty?
      else
        username ||= conf['username']
        password ||= conf['password']
      end
      # construct connection config with the provided credentials
      fail "Connection configuration '#{ar_config}' not found" if conf.nil?
      PG.connect(
        host: conf['host'],
        port: conf['port'],
        user: username,
        password: password,
        dbname: conf['database'],
        connect_timeout: options['timeout']
      )
    end
  end
end
