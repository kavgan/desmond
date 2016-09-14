module Desmond
  module Streams
    DEFAULT_BLOCK_SIZE = 4_194_304 # 4 MB
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
        @buffer     = ''
      end

      def read(*args) # ignoring any argument for now
        read_line = nil
        while read_line.nil?
          unless @reader_obj.eof?
            @buffer += @reader_obj.read(DEFAULT_BLOCK_SIZE)
          end
          newline_pos = @buffer.index(@options[:newline])
          if newline_pos.present?
            read_line = @buffer[0...(newline_pos + @options[:newline].size)]
            @buffer.slice!(0...(newline_pos + @options[:newline].size))
            break
          elsif @reader_obj.eof?
            break
          end
        end

        # check if there is trailing data without a newline that should be returned
        if read_line.nil? && @reader_obj.eof? && !@buffer.empty?
          read_line = @buffer.dup
          @buffer = ''
        end

        # spark likes to write the header with a different line separator, oh what a joy :)
        if !read_line.nil? && @options[:newline] == "\n" && read_line.ends_with?("\r\n")
          read_line = read_line.strip + "\n"
        end

        return read_line
      end

      def eof?
        @reader_obj.eof? && @buffer.empty?
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

      def self.guess_and_create_from_s3(bucket, key, options={})
        reader = Desmond::Streams::S3::S3Reader.new(bucket, key, range: "bytes=0-#{options.fetch(:guess_bytes, 4096)}")
        self.guess_and_create(reader, options)
      end
    end
    class GzipReader < Reader
      def initialize(reader, options={})
        @reader_obj = reader
        @reader = Zlib::GzipReader.new(@reader_obj)
      end
      def read(*args) # ignoring any argument for now
        t = @reader.read(DEFAULT_BLOCK_SIZE)
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

      def <<(obj)
        self.write(obj.to_s)
      end

      def rewind
        fail NotImplementedError
      end

      def flush
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
