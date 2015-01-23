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

      def eof?
        @eof
      end

      def options
        @options
      end
    end
  end
end
