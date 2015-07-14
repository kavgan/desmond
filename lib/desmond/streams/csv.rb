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
# - return_headers, if headers are also returned by read/write, instead of just being parsed and swallowed
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
          @headers || []
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
      # this is parsed into arrays and returned by the `read` method.
      #
      class CSVReader < Streams::Reader
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
          @return_headers = @options.delete(:return_headers)
          @reader = Streams::LineReader.new(reader, newline: options[:row_sep])
          @buff = []
        end

        ##
        # returns the column headers
        #
        def headers
          if @headers == :first_row
            # return_headers will get overriden
            return_headers_saved = @return_headers
            tmp = self.read
            # if we want to return the headers first, add them to the front of the buffer
            if return_headers_saved
              @buff.unshift(@headers)
            else
              @buff << tmp
            end
          end
          super()
        end

        ##
        # expects to read a string representing a CSV file from +reader+ supplied in initialize.
        # parses it to an array representing the columns and returns them
        #
        def read(*args) # ignoring any argument for now
          # if something was buffered, return it now
          unless @buff.empty?
            tmp = @buff.shift
            return tmp
          end
          # read further
          tmp = @reader.read
          return nil if tmp.nil?
          # replace invalid characters
          tmp.encode!(Encoding.default_external, invalid: :replace)
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
          # if we are requested to return the headers, we'll do so
          if @return_headers
            @return_headers = false
            @buff << tmp
            tmp = @headers
          end
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
        def self.guess_separators(reader, guess_lines=100, block_size=4096)
          # read 100 lines for guessing
          content = (0..guess_lines).map { reader.read(block_size) }.join('')
          row_sep = content.max_substr_count(ROW_SEPS)
          col_sep = content.max_substr_count(COL_SEPS)
          quote_char = content.max_substr_count(QUOTE_CHARS) do |content, qc|
            limits  = (content.scan(/\A#{qc}/).size + content.scan(/#{qc}\Z/).size)
            outside = (content.scan("#{row_sep}#{qc}").size + content.scan("#{qc}#{row_sep}").size)
            inside  = (content.scan("#{col_sep}#{qc}").size + content.scan("#{qc}#{col_sep}").size)
            total   = limits + outside + inside
            total = 0 if (total % 2) != 0
            total
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
      # constructing a csv string from an array of rows
      #
      # expects an array of columns (or array of array of columns) to be
      # given to the `write` method. Generates a csv string out of them and
      # writes them to the given `writer`.
      #
      class CSVWriter < Streams::Writer
        include CSVStreamBaseMethods
        attr_reader :writer

        ##
        # expects a +writer+ which takes strings.
        # +options+ should be as described above.
        #
        def initialize(writer, options={})
          super()
          @writer = writer
          @options = self.get_csv_options(options)
          @headers = @options[:headers]
          @options.delete(:skip_rows) # not supported by writer anyways
        end

        ##
        # transforms the given row(s) into a csv string and writes them to
        # +writer+ supplied during `initialize`.
        # returns the csv string of the rows processed in this invocation
        #
        def write(row)
          fail 'Expecting an array' unless row.is_a?(Array)
          written = ''

          # check if we need to parse the headers
          if @options[:headers] == :first_row
            @headers = @options[:headers] = row
            return ''
          end

          # check if we need to generate a header line
          if @options[:return_headers] && @options[:headers]
            @options[:return_headers] = false
            if @options[:headers] == :first_row
              #p "CSVWriter headers first_row: #{row}"
              hdr_line = ::CSV.generate_line(row, @options)
            else
              hdr_line = ::CSV.generate_line(@options[:headers], @options)
            end
            @writer.write(hdr_line)
            written += hdr_line
            if @options[:headers] == :first_row
              @options[:headers] = row
              return written
            end
          end

          # generate new row
          tmp = ::CSV.generate(@options) do |csv|
            # did we get an array of rows or just one row?
            if row.size > 0 && row[0].is_a?(Enumerable)
              return row.map { |tmp_row| self.write(tmp_row) }
            elsif row.size > 0
              csv << row
            else
              return nil
            end
          end

          # return to caller
          @writer.write(tmp)
          written + tmp
        end

        def flush
          @writer.flush
        end

        ##
        # close writer
        #
        def close
          @writer.close
        end
      end
    end
  end
end
