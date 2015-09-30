require 'aws-sdk-v1'

module Desmond
  module Streams
    module S3
      ##
      # reads from S3 using bucket +bucket+ and key +key+.
      # All +options+ valid for AWS::S3.new are supported.
      #
      class S3Reader < Streams::Reader
        DEFAULT_READ_SIZE = 4096
        attr_reader :bucket, :key

        def initialize(bucket, key, options={})
          @bucket = bucket
          @key = key
          @options = { read_size: DEFAULT_READ_SIZE }.merge(options)
          @aws = AWS::S3.new(@options)
          @reader = recreate
        end

        def credentials
          c = @aws.config
          "aws_access_key_id=#{c.access_key_id};aws_secret_access_key=#{c.secret_access_key}"
        end

        def read(*args) # ignoring any argument for now
          r = @reader.read(@options[:read_size])
          return nil if r.nil? || r.empty?
          r
        end

        def gets(*args)
          @reader.gets(*args)
        end

        def eof?
          @reader.eof?
        end

        def rewind
          @reader = recreate
        end

        def close
          @reader.close
        end

        def closed?
          @reader.closed?
        end

        private

        def recreate
          if @aws.buckets[@bucket].objects[@key + '_$folder$'].exists?
            o = @aws.buckets[@bucket].objects.with_prefix(@key + '/')
          else
            fail "#{@bucket}/#{@key} does not exist!" unless @aws.buckets[@bucket].objects[@key].exists?
            o = [ @aws.buckets[@bucket].objects[@key] ]
          end
          fail "#{@bucket}/#{@key} does not exist!" if o.blank?
          # no other way to stream from S3 unfortunately ...
          reader, writer = IO.pipe
          Thread.new do
            begin
              for t in o
                t.read { |chunk| writer.write chunk }
              end
            ensure
              writer.close
            end
          end
          reader
        end
      end

      ##
      # writes to S3 using bucket +bucket+ and key +key+.
      # All +options+ valid for AWS::S3.new are supported.
      #
      class S3Writer < Streams::Writer
        attr_reader :bucket, :key

        def initialize(bucket, key, options={})
          @bucket = bucket
          @key = key
          @options = options
          @aws = AWS::S3.new(@options)
          # create empty s3 object and get writer to it
          @o, @thread, @writer = recreate
          @s3time = 0.0
          @s3calls = 0
        end

        ##
        # retrieves a presigned reading URL valid for/until +expires+ (default 1 week)
        #
        def public_url(expires=(7 * 86400))
          @o.url_for(:read, expires: expires).to_s
        end

        ##
        # write the given data to the underlying S3 object
        #
        def write(data)
          start_time = Time.now
          @writer.write(data)
          @s3time += Time.now - start_time
          @s3calls += 1
        end

        ##
        # close s3 stream
        #
        def close
          @writer.close
          @thread.join
          DesmondConfig.logger.info "S3 write time: #{@s3time}, #{@s3calls}" unless DesmondConfig.logger.nil?
        end

        private

        def recreate
          # we are going to overwrite existing files
          #fail "S3 object already exists: '#{@bucket}': '#{@key}'" if @aws.buckets[@bucket].objects[@key].exists?
          o = @aws.buckets[@bucket].objects.create(@key, '')
          # no other way to stream to S3 unfortunately ...
          reader, writer = IO.pipe
          thread = Thread.new do
            begin
              o.write(estimated_content_length: AWS.config.s3_multipart_threshold + 1) do |buffer, bytes|
                # aws doesn't seem to have a problem with getting more bytes,
                # which makes this code simpler
                while bytes > 0 && !reader.eof?
                  t = reader.read
                  buffer.write(t)
                  bytes -= t.size
                end
              end
            ensure
              reader.close
            end
          end
          return o, thread, writer
        end
      end
    end
  end
end
