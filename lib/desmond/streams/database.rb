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
        def read
          self.init_cursor unless @init_cursor

          unless @buff.nil?
            tmp = @buff
            @buff = nil
            return tmp
          end

          tmp = self.execute(@fetchq).map do |h|
            @keys = h.keys
            h.values
          end.to_a

          @eof = true if tmp.empty?
          yield(tmp) if block_given?
          tmp
        end

        def close
          self.execute(@closeq)
        end

        def eof?
          @eof
        end

        ##
        # creates an instance of Streams::CSV:CSVStringWriter wrapped around this class,
        # letting you read csv from the database
        #
        # the 'db' key of +options+ is passed to the Postgres database reader.
        # key 'csv' in +options+ is passed to the CSV reader. See respective classes
        # for supported and required options.
        #
        def self.create_csv_reader(name, query, options={})
          Streams::CSV::CSVStringWriter.new(self.new(name, query, options[:db]), options[:csv])
        end

        protected

        def init_cursor
          @init_cursor = true
          self.execute(@initq)
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
          Que.log level: :debug, sql: sql
          @conn.exec(sql)
        end
      end
    end
  end
end
