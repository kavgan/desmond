module Desmond
  module Streams
    ##
    # some convenience methods on readers and writers
    #
    class Utils
      ##
      # pipes +reader+ into +writer+ until eof
      #
      # if a block is given, it can be used to modify
      # what was returned from the +reader+ before it
      # gets sent to the +writer+.
      #
      def self.pipe(reader, writer)
        readtime, writetime = 0.0, 0.0
        until reader.eof?
          start_time = Time.now
          data = reader.read
          readtime += Time.now - start_time
          data = yield(data) if block_given?
          start_time = Time.now
          writer.write(data)
          writetime += Time.now - start_time
        end
        DesmondConfig.logger.info "Pipe time: read #{readtime}, write #{writetime}" unless DesmondConfig.logger.nil?
      end
    end
    ##
    # abstract base reader class
    #
    class Reader
      attr_reader :options

      def initialize
        @eof = false
        @options = {}
      end

      def read(*args) # ignoring any argument for now
        fail NotImplementedError
      end

      def gets(*args) # ignoring any argument for now
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
    # if your reader supports `rewind`, it is recommend to
    # use `guess_and_create` to get a new LineReader. It will
    # try to guess the newline character to use automatically.
    #
    # read the given IO class +reader+ line by line.
    # supported options:
    # - newline: characters separating lines, defaults to \n
    #
    class LineReader < Reader
      NEW_LINE_CHARS = ["\r\n", "\n", "\r"]

      def initialize(reader, options={})
        @options = {
          newline: "\n"
        }.merge(options.delete_if { |_, v| v.nil? })
        @reader_obj = reader
      end

      def read(*args) # ignoring any argument for now
        next_line = @reader_obj.gets(@options[:newline])
        if !next_line.nil? && @options[:newline] == "\n" && next_line.ends_with?("\r\n")
          # spark likes to write the header with a different line separator, oh what a joy :)
          next_line = next_line.strip + "\n"
        end
        return next_line
      end

      def eof?
        @reader_obj.eof?
      end

      def close
        @reader_obj.close
      end

      def self.guess_newline_char(reader, guess_bytes=4096)
        # read a few bytes to use for guessing
        content = ''
        until guess_bytes <= 0 || reader.eof?
          tmp          = reader.read
          guess_bytes -= tmp.length
          content     += tmp
        end
        return content.max_substr_count(NEW_LINE_CHARS)
      end

      def self.guess_and_create(reader, options={})
        guess_bytes = options.delete(:guess_bytes) || 4096
        options = {
          newline: self.guess_newline_char(reader, guess_bytes)
        }.merge(options)
        reader.rewind
        self.new(reader, options)
      end
    end
    class GzipReader < Reader
      def initialize(reader, options={})
        @reader_obj = reader
        @reader = Zlib::GzipReader.new(@reader_obj)
      end
      def read(*args) # ignoring any argument for now
        t = @reader.read(4096)
        return nil if t.nil? || t.empty?
        t
      end
      def gets(*args) # ignoring any argument for now
        @reader.gets
      end
      def rewind
        @reader_obj.rewind
        @reader = Zlib::GzipReader.new(@reader_obj)
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

      def write(data)
        fail NotImplementedError
      end

      def <<(data)
        self.write(data)
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
