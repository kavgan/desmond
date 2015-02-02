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
          raise 'No name parameter' if name.nil?
          raise 'No query parameter' if query.nil?
          @name = name
          @query = query
          @options = {
            fetch_size: 1000,
          }.merge((options || {}).symbolize_keys)
          raise '"fetch_size" needs to be greater than 0' if @options[:fetch_size] <= 0
          # prepare queries
          @initq = "BEGIN; DECLARE #{@name} CURSOR FOR #{@query};"
          @fetchq = "FETCH FORWARD #{@options[:fetch_size].to_i} FROM #{@name};"
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
          self.init_cursor if not(@init_cursor)

          if not(@buff.nil?)
            tmp = @buff
            @buff = nil
            return tmp
          end
          tmp = self.execute(@fetchq).map { |h| @keys = h.keys; h.values }.to_a
          @eof = true if tmp.empty?
          yield(tmp) if block_given?
          return tmp
        end

        def close
          self.execute(@closeq)
        end

        def eof?
          @eof
        end

        ##
        # creates an instance of Streams::CSV:CSVStringReader wrapped around this class,
        # letting you read csv from the database
        #
        # the 'db' key of +options+ is passed to the Postgres database reader.
        # key 'csv' in +options+ is passed to the CSV reader. See respective classes
        # for supported and required options.
        #
        def self.create_csv_reader(name, query, options={})
          Streams::CSV::CSVStringReader.new(self.new(name, query, options[:db]), options[:csv])
        end

        protected
          def init_cursor
            @init_cursor = true
            self.execute(@initq)
          end

          def execute(sql)
            raise NotImplementedError
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
          @conn = self.class.dedicated_connection(self.options)
        end

        def close
          super()
          @conn.close
        end

        protected
          def execute(sql)
            Que.log level: :debug, sql: sql
            @conn.exec(sql)
          end

        private
          def self.dedicated_connection(options)
            PGUtil.dedicated_connection(options)
          end
      end
    end
  end
end
