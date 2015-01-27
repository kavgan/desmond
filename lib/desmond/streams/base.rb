module Desmond
  module Streams
    ##
    # abstract base reader class
    #
    class Reader
      def initialize
        @eof = false
        @options = {}
      end

      def read
        raise NotImplementedError
      end

      def close
        raise NotImplementedError
      end

      def rewind
        raise NotImplementedError
      end

      def eof?
        @eof
      end

      def options
        @options
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
        }.merge(options.delete_if { |k, v| v.nil? })
        @reader_obj = reader
        @reader = reader.each_line(@options[:newline])
      end

      def read
        begin
          return @reader.next
        rescue StopIteration
          @eof = true
          return nil
        end
      end

      def close
        @reader_obj.close
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
        raise NotImplementedError
      end

      def close
        raise NotImplementedError
      end

      def options
        @options
      end
    end
  end
end
