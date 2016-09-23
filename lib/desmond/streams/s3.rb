module Desmond
  module Streams
    module S3
      ##
      # reads from S3 using bucket +bucket+ and key +key+.
      # All +options+ valid for AWS::S3.new are supported.
      # option +:range+ will be forwarded to `s3_obj.read`
      #
      class S3Reader < Streams::Reader
        attr_reader :bucket, :key

        def initialize(bucket, key, options={})
          @bucket = bucket
          @key = key
          @options = { read_size: Desmond::Streams::DEFAULT_BLOCK_SIZE }.merge(options)
          @aws_options = @options.reject { |k| [ :read_size, :range ].include?(k) }
          @range_options = @options.select { |k| k == :range }
          @s3objects = nil # holds s3objects to iterate and read
          @closed    = nil # holds whether stream is closed
          @sizes     = nil # holds sizes of all s3objects
          @size      = nil # holds sum of sizes of all s3objects
          @pos       = nil # holds position in all s3objects
          @s3o_pos   = nil # holds position in current s3object
          @s3o_idx   = nil # holds index of current s3object
          recreate
        end

        def credentials
          c = @s3objects.first.client.config
          "aws_access_key_id=#{c[:access_key_id]};aws_secret_access_key=#{c[:secret_access_key]}"
        end

        def read(*args) # ignoring any argument for now
          return nil if (@pos >= @size)
          if @s3o_pos >= @sizes[@s3o_idx]
            @s3o_idx += 1
            @s3o_pos  = 0
          end
          upper_bound = @s3o_pos + @options[:read_size] - 1
          upper_bound = (@sizes[@s3o_idx] - 1) if upper_bound >= @sizes[@s3o_idx]
          data  = @s3objects[@s3o_idx].get({ range: "bytes=#{@s3o_pos}-#{upper_bound}" }.merge(@range_options)).body.read
          @pos += data.size
          @s3o_pos += data.size
          return nil if data.nil? || data.empty?
          return data
        end

        def eof?
          (@pos >= @size)
        end

        def rewind
          recreate
        end

        def close
          @closed = true
          return nil
        end

        def closed?
          @closed
        end

        private

        def recreate
          bucket = Aws::S3::Bucket.new(@bucket, @aws_options)
          folder_objects = bucket.objects(prefix: @key + '_$folder$').each.to_a
          if folder_objects.present?
            @s3objects = folder_objects
          else
            fail "#{@bucket}/#{@key} does not exist!" unless bucket.object(@key).exists?
            @s3objects = [ bucket.object(@key) ]
          end
          fail "#{@bucket}/#{@key} does not exist!" if @s3objects.blank?
          @closed   = false
          @pos      = 0
          @s3o_pos  = 0
          @s3o_idx  = 0
          @sizes    = @s3objects.map { |o| o.content_length }
          @size     = @sizes.sum
          return nil
        end
      end

      ##
      # writes to S3 using bucket +bucket+ and key +key+.
      # All +options+ valid for AWS::S3::Client.new are supported.
      # The option +max_file_size+ can be used to hint the maximum size
      # of the uploaded file for more efficient uploads.
      #
      class S3Writer < Streams::Writer
        # S3 only supports up to 10K multipart chunks
        MAX_NUM_CHUNKS = 10000
        # Minimum chunk size in S3 is 5MiB (except for the last chunk)
        MIN_CHUNK_SIZE = 5 * 1024 * 1024 # MiB

        attr_reader :bucket, :key

        def initialize(bucket, key, options={})
          @bucket = bucket
          @key = key
          @options = options
          @aws_options = @options.reject { |k| [ :max_file_size, :bucket, :key ].include?(k) }
          @s3object = Aws::S3::Bucket.new(@bucket, @aws_options).object(@key)
          @min_chunk_size = [(@options[:max_file_size].to_f / MAX_NUM_CHUNKS).ceil, MIN_CHUNK_SIZE].max
          @multipart_upload = nil
          @multipart_partno = nil
          @multipart_parts  = nil
          @writebuffer = nil
          # create empty s3 object and get writer to it
          recreate
          @s3time = 0.0
          @s3calls = 0
        end

        ##
        # retrieves a presigned reading URL valid for/until +expires+ (default 1 week)
        #
        def public_url(expires=(7 * 86400))
          @s3object.presigned_url(:get, expires_in: expires).to_s
        end

        ##
        # write the given data to the underlying S3 object
        #
        def write(data)
          @writebuffer << data
          if @writebuffer.size > @min_chunk_size
            # write to s3 if we have enough data to write
            self.flush
          end
          return data.size
        end

        ##
        # restarts the writer from the beginning
        #
        def rewind
          recreate
        end

        ##
        # flushes remaining writes
        #
        def flush
          return if @writebuffer.empty?
          start_time = Time.now

          new_part = @multipart_upload.part(@multipart_partno)
          upload_response = new_part.upload(body: @writebuffer)
          @writebuffer.replace ''
          @multipart_parts  << {
            part_number: @multipart_partno,
            etag: upload_response.etag,
          }
          @multipart_partno += 1

          @s3time += Time.now - start_time
          @s3calls += 1
          return nil
        end

        ##
        # close s3 stream
        #
        def close
          self.flush
          if @multipart_parts.blank?
            DesmondConfig.logger.debug "aborting stream upload since there are no parts" unless DesmondConfig.logger.nil?
            @multipart_upload.abort
            @multipart_upload = nil
          else
            DesmondConfig.logger.debug "finishing stream upload: #{@multipart_parts}" unless DesmondConfig.logger.nil?
            @multipart_upload.complete(multipart_upload: { parts: @multipart_parts })
          end
          DesmondConfig.logger.info "S3 write time: #{@s3time}, #{@s3calls}" unless DesmondConfig.logger.nil?
          return nil
        end

        private

        def recreate
          @writebuffer = ''
          @multipart_partno = 1
          @multipart_parts  = []
          unless @multipart_upload.nil?
            @multipart_upload.abort # if we're recreating, there was something wrong
            @multipart_upload = nil
          end
          @multipart_upload = @s3object.initiate_multipart_upload
          return nil
        end
      end
    end
  end
end
