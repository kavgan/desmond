require 'aws-sdk-v1'

module Desmond
  module Streams
    module S3
      ##
      # writes to S3 using bucket +bucket+ and key +name+.
      # All +options+ valid for AWS::S3.new are supported.
      #
      class S3Writer
        def initialize(bucket, name, options={})
          # create empty s3 object
          @o = AWS::S3.new(options).buckets[bucket].objects.create(name, '')
        end

        ##
        # retrieves a presigned reading URL valid for/until +expires+ (default 1 week)
        #
        def public_url(expires=(7 * 86400))
          @o.url_for(:read, expires: expires).to_s
        end

        ##
        # uses +reader+ to write to the initialized S3 object until EOF is reached.
        # does NOT close the reader.
        # S3 object is deleted on error
        #
        def write_from(reader)
          begin
            # we have no idea of the file size, but we're gonna force aws to upload using multipart upload,
            # just so the whole file won't be in memory at some point
            @o.write(estimated_content_length: AWS.config.s3_multipart_threshold + 1) do |buffer, bytes|
              while bytes > 0 && not(reader.eof?)
                t = reader.read
                buffer.write(t)
                bytes -= t.size
              end
            end
          rescue => e
            # remove object if error occurred
            @o.delete
            raise e
          end
        end
      end
    end
  end
end
