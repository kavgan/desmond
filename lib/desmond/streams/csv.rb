require_relative '../../desmond'
require 'csv'

#
# every csv streams supports these options identical to Ruby's CSV implementation:
# - col_sep
# - row_sep
# - quote_char
# - force_quotes, only really useful with writers
# - skip_blanks, only really useful with readers
# slightly different implementation:
# - headers, support false, first_row or array
# - return_headers, is a writer option only
# added options:
# - skip_rows, reader only, integer of top rows to skip, defaults to 0,
#              in case you want to override a header row with your own headers
#

module Desmond
  module Streams
    module CSV
      module CSVStreamBaseMethods
        ##
        # returns the column headers
        #
        def headers
          @options[:headers] || []
        end

        ##
        # alias for `headers`
        #
        def columns
          self.headers
        end

        def get_csv_options(options={})
          options = self.default_csv_options.merge(self.whitelisted_csv_options(options))
          options[:force_quotes] = !!options[:force_quotes]
          options[:skip_blanks] = !!options[:skip_blanks]
          options[:headers] = options[:headers].to_sym if options[:headers].respond_to?(:to_sym)
          options[:return_headers] = !!options[:return_headers]
          options[:skip_rows] = options[:skip_rows].to_i if options[:skip_rows].respond_to?(:to_i)
          options
        end

        def default_csv_options
          {
            col_sep: ',',
            row_sep: "\n",
            quote_char: '"',
            force_quotes: false,
            skip_blanks: false,
            headers: false,
            return_headers: false,
            skip_rows: 0
          }
        end

        def whitelisted_csv_options(options={})
          options.symbolize_keys.select do |key, _|
            (key == :col_sep || key == :row_sep || key == :quote_char || key == :force_quotes ||
              key == :skip_blanks || key == :headers || key == :return_headers || key == :skip_rows)
          end
        end
      end

      ##
      # expects supplied +reader+ to return a csv string.
      # this is parsed into arrays and returned by this class.
      #
      class CSVArrayReader < Streams::Reader
        include CSVStreamBaseMethods

        ROW_SEPS = ["\r\n", "\n", "\r"]
        COL_SEPS = [',', '|', "\t", ';']
        QUOTE_CHARS = ['"', '\'']

        ##
        # expects a string +reader+ and +options+ as described above
        #
        def initialize(reader, options={})
          super()
          @options = self.get_csv_options(options)
          @skip_rows = @options.delete(:skip_rows)
          @headers = @options.delete(:headers)
          @options.delete(:return_headers) # not supported by reader anyways
          @reader = Streams::LineReader.new(reader, newline: options[:row_sep])
          @buff = nil
        end

        ##
        # returns the column headers
        #
        def headers
          @buff = self.read if @headers == :first_row
          super()
        end

        ##
        # expects to read a string representing a CSV file from +reader+ supplied in initialize.
        # parses it to an array representing the columns and returns them
        #
        def read
          # if something was buffered, return it now
          unless @buff.nil?
            tmp, @buff = @buff, nil
            return tmp
          end
          # read further
          tmp = @reader.read
          return nil if tmp.nil?
          # skip rows if requested
          if @skip_rows > 0
            @skip_rows -= 1
            return self.read
          end
          tmp = ::CSV.parse_line(tmp, @options)
          # if the haven't parsed first_row headers yet, do it now
          if @headers == :first_row
            @headers = @options[:headers] = tmp
            return self.read
          end
          # always return an array of columns
          tmp = tmp.to_hash.values if tmp.is_a?(::CSV::Row)
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
        # guesses row_sep, col_sep and quote_char.
        # returns hash
        #
        def self.guess_separators(reader, guess_lines=100)
          # read 100 lines for guessing
          content = (0..guess_lines).map { reader.read }.join('')
          row_sep = content.max_substr_count(ROW_SEPS)
          col_sep = content.max_substr_count(COL_SEPS)
          quote_char = content.max_substr_count(QUOTE_CHARS) do |content, qc|
            content.scan("#{col_sep}#{qc}").size + content.scan("#{qc}#{col_sep}").size
          end
          { row_sep: row_sep, col_sep: col_sep, quote_char: quote_char }
        end

        ##
        # uses `guess_separators` to guess options and returns
        # new instance of class using these. overwrite guesses
        # by supplying custom options.
        #
        def self.guess_and_create(reader, options={})
          guess_lines = options.delete(:guess_lines) || 100
          options = self.guess_separators(reader, guess_lines).merge(options)
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
      class CSVStringWriter < Streams::Writer
        include CSVStreamBaseMethods
        ##
        # expects a +reader+ returning arrays of columns or
        # two-dimensional arrays containing rows and then columns.
        # +options+ should be as described above.
        #
        def initialize(reader, options={})
          super()
          @reader = reader
          @options = self.get_csv_options(options)
          @options.delete(:skip_rows) # not supported by writer anyways
          fail ArgumentError, 'headers cannot be first_row for this writer' if @options[:headers] == :first_row
          @options[:headers] = reader.headers if !options[:headers] && reader.respond_to?(:headers)
          @options[:headers] = reader.columns if !options[:headers] && reader.respond_to?(:columns)
        end

        ##
        # reads from the supplied reader in `initialize`, transforming the retrieved rows
        # into a csv string.
        # returns the csv string of the rows processed in this invocation
        #
        def read
          tmp = nil
          # check if we need to read a header line
          if @options[:return_headers] && @options[:headers]
            @options[:return_headers] = false
            tmp = ::CSV.generate_line(@options[:headers], @options)
          end

          # read rows from supplied reader
          if tmp.nil?
            tmp = ::CSV.generate(@options) do |csv|
              tmp = @reader.read
              # did we get an array of rows or just one row?
              if tmp.size > 0 && tmp[0].is_a?(Enumerable)
                tmp.map do |row|
                  csv << row
                end
              elsif tmp.size > 0
                csv << tmp
              else
                return nil
              end
            end
          end

          # return to caller
          yield(tmp) if block_given?
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
      end
    end
  end
end
