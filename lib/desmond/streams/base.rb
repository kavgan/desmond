# TODO make read functions IO.read compatible
# TODO IO.read reads until EOF with no argument, make sure we don't that anymore

module Desmond
  module Streams
    ##
    # abstract base reader class
    #
    class Reader
      attr_reader :options

      def initialize
        @eof = false
        @options = {}
      end

      def read
        fail NotImplementedError
      end

      def close
        fail NotImplementedError
      end

      def rewind
        fail NotImplementedError
      end

      def eof?
        @eof
      end
    end
    ##
    # read the given IO class +reader+ line by line.
    # supported options:
    # - newline: characters separating lines, defaults to \n
    #
    class LineReader < Reader
      def initialize(reader, options={})
        @options = {
          newline: "\n"
        }.merge(options.delete_if { |_, v| v.nil? })
        @reader_obj = reader
        @reader = reader.each_line(@options[:newline])
      end

      def read
        return @reader.next
      rescue StopIteration
        @eof = true
        return nil
      end

      def eof?
        @reader_obj.eof?
      end

      def close
        @reader_obj.close
      end
    end
    class GzipReader < Reader
      def initialize(reader, options={})
        @reader_obj = reader
        @reader = Zlib::GzipReader.new(@reader_obj)
      end
      def read
        t = @reader.read
        return nil if t.nil? || t.empty?
        t
      end
      def eof?
        @reader.eof?
      end

      def close
        @reader.close
      end
    end
    ##
    # abstract base writer class
    #
    class Writer
      def initialize
        @options = {}
      end

      def write
        fail NotImplementedError
      end

      def close
        fail NotImplementedError
      end

      def options
        @options
      end
    end
  end
end
