require 'csv'

module Desmond
  module Streams
    module CSV
      ##
      # CSVReader constructing a csv file from an array reader
      #
      class CSVReader < Streams::Reader
        ##
        # reads rows from a +reader+ expecting to retrieve
        # arrays of arrays, representing the rows containing the columns
        # valid +options+: are:
        # - col_sep
        # - row_sep
        # - headers
        # - return_headers
        # - quote_char
        #
        def initialize(reader, options={})
          super()
          @reader = reader
          @csv_options = {
            col_sep: ',',
            row_sep: "\n",
            headers: reader.columns,
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
          if @csv_options[:return_headers] && not(@read_headers)
            @read_headers = true
            tmp = ::CSV.generate_line(@csv_options[:headers], @csv_options)
          end

          # read rows from supplied reader
          if tmp.nil?
            tmp = ::CSV.generate(@csv_options) do |csv|
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
        # reached the end of file
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
