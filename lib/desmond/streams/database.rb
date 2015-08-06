require 'active_record'
require 'pg'

module Desmond
  module Streams
    module Database
      ##
      # abstract base class for reading a lot of data out of databases using SQL cursors
      #
      class DatabaseCursorReader < Streams::Reader
        ##
        # +name+: unique name for the cursor
        # +query+: query to use with cursor
        # supported +options+:
        # - fetch_size: how many rows should be read at once
        #
        def initialize(name, query, options={})
          super()
          fail 'No name parameter' if name.nil?
          fail 'No query parameter' if query.nil?
          @name = PGUtil.escape_identifier(name)
          @query = query
          # only one query is allowed, mitigating sql injection
          fail 'Query separator detected' unless query.index(';').nil? || query.index(';') == query.size - 1
          @options = {
            fetch_size: 1000
          }.merge((options || {}).symbolize_keys)
          @options[:fetch_size] = @options[:fetch_size].to_i
          fail '"fetch_size" needs to be greater than 0' if @options[:fetch_size] <= 0
          # prepare queries
          @initq = "BEGIN; DECLARE #{@name} CURSOR FOR #{@query};"
          @fetchq = "FETCH FORWARD #{@options[:fetch_size]} FROM #{@name};"
          @closeq = "CLOSE #{@name}; END;"
          @init_cursor = false
          @buff = nil
          @keys = nil
          @dbtime = 0.0
          @dbcalls = 0
        end

        ##
        # returns the columns the query produces
        #
        def columns
          @buff = read if @keys.nil?
          @keys
        end

        ##
        # returns array of rows containing column values:
        # [ [value1, value2], ... ]
        #
        def read(*args) # ignoring any argument for now
          self.init_cursor unless @init_cursor

          unless @buff.nil?
            tmp = @buff
            @buff = nil
            return tmp
          end

          start_time = Time.now
          tmp = self.execute(@fetchq).map do |h|
            @keys = h.keys
            h.values
          end.to_a
          @dbtime += Time.now - start_time
          @dbcalls += 1

          @eof = true if tmp.empty?
          yield(tmp) if block_given?
          tmp
        end

        def close
          self.execute(@closeq) if @init_cursor
          DesmondConfig.logger.info "database time: #{@dbtime}, #{@dbcalls}" unless DesmondConfig.logger.nil?
        end

        def eof?
          @eof
        end

        protected

        def init_cursor
          @init_cursor = true
          start_time = Time.now
          self.execute(@initq)
          @dbtime += Time.now - start_time
        end

        def execute(_sql)
          fail NotImplementedError
        end
      end

      ##
      # implementation of DatabaseCursorReader for Postgres compatible databases
      #
      # see DatabaseCursorReader for additional details about arguments.
      # required +options+:
      # - connection_id: ActiveRecord connection_id to clone
      # - username: custom credentials to use
      # - password: custom credentials to use
      # additionally supported +options+:
      # - timeout: connection timeout to database
      #
      class PGCursorReader < DatabaseCursorReader
        def initialize(name, query, options={})
          super(name, query, options)
          @conn = PGUtil.dedicated_connection(self.options)
        end

        def close
          super() unless @conn.transaction_status == PG::Constants::PQTRANS_INERROR # transaction aborted, no need to close cursor
          @conn.close
        end

        protected

        def execute(sql)
          Que.logger.debug sql unless Que.logger.nil?
          @conn.exec(sql)
        end
      end
    end
  end
end
