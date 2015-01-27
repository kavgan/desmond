require_relative '../../desmond'
require 'csv'

module Desmond
  module Streams
    module CSV
      ##
      # abstract base class for csv streams
      #
      class CSVStream < Streams::Reader
        ##
        # returns the column headers
        #
        def headers
          @options[:headers] || []
        end
      end

      ##
      # parsing arrays out of an io object reading a csv.
      #
      class CSVArrayReader < CSVStream
        COL_SEPS = [",", "|", "\t", ";"]

        ##
        # valid +options+ are (see ruby's CSV class):
        # - col_sep
        # - row_sep
        # - headers
        # - return_headers
        # - quote_char
        #
        def initialize(reader, options={})
          super()
          @options = options.symbolize_keys
          @first_row_headers = false
          if @options[:headers] == :first_row
            @first_row_headers = true
            @options[:headers] = nil
          end
          @reader = Streams::LineReader.new(reader, newline: options[:row_sep])
          @buff = nil
        end

        ##
        # returns the column headers
        #
        def headers
          if @first_row_headers && @options[:headers].nil?
            @buff = self.read
          end
          super()
        end

        ##
        # expects to read a string representing a CSV file from +reader+ supplied in initialize.
        # parses it to an array representing the columns and returns them
        #
        def read
          # if something was buffered, return it now
          if not(@buff.nil?)
            tmp, @buff = @buff, nil
            return tmp
          end
          # read further
          tmp = @reader.read
          return nil if tmp.nil?
          tmp = ::CSV::parse_line(tmp, @options)
          # if the first row contains headers parse them
          if @first_row_headers && @options[:headers].nil?
            @options[:headers] = tmp
            return self.read
          end
          # always return an array of columns
          tmp = tmp.to_hash.values if tmp.kind_of?(::CSV::Row)
          tmp
        end

        ##
        # reached the end of file?
        #
        def eof?
          @reader.eof?
        end

        ##
        # close reader
        #
        def close
          @reader.close
        end

        ##
        # guesses row_sep and col_sep.
        # returns hash
        #
        def self.guess_separators(reader)
          # read 100 lines for guessing
          content = (0..100).map { reader.read }.join('')
          max_count = 0
          row_sep = "\n" # safe bet for now :)
          col_sep = nil
          COL_SEPS.each do |cs|
            count = content.count cs
            if count > max_count
              max_count = count
              col_sep = cs
            end
          end
          { row_sep: row_sep, col_sep: col_sep }
        end

        ##
        # uses `guess_separators` to guess options and returns
        # new instance of class using these. overwrite guesses
        # by supplying custom options.
        #
        def self.guess_and_create(reader, options={})
          options = self.guess_separators(reader).merge(options)
          reader.rewind
          self.new(reader, options)
        end
      end

      ##
      # constructing a csv string from an array reader
      #
      # reads rows from a +reader+ expecting to retrieve
      # arrays of arrays, representing the rows containing the columns
      #
      class CSVStringReader < CSVStream
        ##
        # valid +options+ are (see ruby's CSV class):
        # - col_sep
        # - row_sep
        # - headers
        # - return_headers
        # - quote_char
        #
        def initialize(reader, options={})
          super()
          @reader = reader
          columns = reader.columns if reader.respond_to?(:columns)
          @options = {
            col_sep: ',',
            row_sep: "\n",
            headers: columns,
            return_headers: false
          }.merge((options || {}).symbolize_keys.select do |key, value|
            key == :col_sep || key == :row_sep || key == :headers || key == :return_headers || key == :quote_char 
          end)
          @read_headers = false
        end

        ##
        # reads from the supplied reader in `initialize`, transforming the retrieved rows
        # into a csv string.
        # returns the csv string of the rows processed in this invocation
        #
        def read
          tmp = nil
          # check if we need to read a header line
          if @options[:return_headers] && not(@read_headers)
            @read_headers = true
            tmp = ::CSV.generate_line(@options[:headers], @options)
          end

          # read rows from supplied reader
          if tmp.nil?
            tmp = ::CSV.generate(@options) do |csv|
              @reader.read.map do |row|
                csv << row
              end
            end
          end

          # return to caller
          yield(tmp) if block_given?
          return tmp
        end

        ##
        # reached the end of file?
        #
        def eof?
          @reader.eof?
        end

        ##
        # close reader
        #
        def close
          @reader.close
        end
      end
    end
  end
end
