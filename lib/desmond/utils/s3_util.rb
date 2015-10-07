class S3Util
  MAX_COPY_SIZE = 5_368_709_120 # AWS S3 can take a maximum of 5GB per multipart part

  ##
  # merges a folder of S3 objects into one single object
  #
  def self.merge_objects(src_bucket, src_prefix, dest_bucket, dest_key)
    s3 = AWS::S3.new
    src_bucket  = s3.buckets[src_bucket]
    dest_bucket = s3.buckets[dest_bucket]

    # calcluate total size to determine how to merge
    total_size  = 0
    num_objects = src_bucket.objects.with_prefix(src_prefix).count
    src_bucket.objects.with_prefix(src_prefix).each do |source_object|
      total_size += source_object.content_length
    end

    # do the merge
    # if every source object is approximatly bigger than the S3 multipart upload threshold, we do a multipart
    if total_size <= num_objects * AWS.config.s3_multipart_min_part_size
      __merge_objects_normal(src_bucket, src_prefix, dest_bucket, dest_key, total_size)
    else
      __merge_objects_multipart(src_bucket, src_prefix, dest_bucket, dest_key, total_size)
    end
  end

  ##
  # strategy: the total size of all source objects is rather small (< 5MB), so we download them all
  #           and reupload them allat once concatenated
  #
  def self.__merge_objects_normal(src_bucket, src_prefix, dest_bucket, dest_key, total_size)
    return __merge_objects_normal_internal(src_bucket.objects.with_prefix(src_prefix), dest_bucket, dest_key, total_size)
  end
  private_class_method :__merge_objects_normal

  ##
  # strategy: the total size of all source object can be large (> 5MB), so we tell S3 to copy them
  #           without ever downloading them.
  #
  def self.__merge_objects_multipart(src_bucket, src_prefix, dest_bucket, dest_key, total_size)
    # premerge objects which might be too small
    __prepare_objects_for_multipart(src_bucket, src_prefix)
    # First, let's start the Multipart Upload
    obj_aggregate = dest_bucket.objects[dest_key].multipart_upload
    # Then we will copy into the Multipart Upload all of the objects in a certain S3 directory.
    src_bucket.objects.with_prefix(src_prefix).each do |source_object|
      # Skip the directory object
      unless (source_object.key == ARGV[1])
        # Note that this section is thread-safe and could greatly benefit from parallel execution.
        source_length = source_object.content_length
        next if source_length == 0
        next if source_object.key.end_with?('.gz') && source_length == 20 # empty gzip file
        pos = 0
        part_size = self::MAX_COPY_SIZE
        until pos >= source_length
          last_byte = (pos + part_size >= source_length) ? source_length - 1 : pos + part_size - 1
          DesmondConfig.logger.info "copying #{source_object.key}: #{pos} - #{last_byte} => #{last_byte - pos} bytes" unless DesmondConfig.logger.nil?
          obj_aggregate.copy_part(source_object.bucket.name + '/' + source_object.key,
            copy_source_range: "bytes=#{pos}-#{last_byte}")
          pos += part_size
        end
      end
    end
    return obj_completed = obj_aggregate.complete()
  end
  private_class_method :__merge_objects_multipart

  ##
  # S3 multipart upload only allows the last part to be smaller than `AWS.config.s3_multipart_min_part_size`.
  # if some other objects are smaller this method tries to merge them together with the following one.
  #
  def self.__prepare_objects_for_multipart(src_bucket, src_prefix)
    objects = src_bucket.objects.with_prefix(src_prefix).to_a
    for i in 0...objects.count
      next if objects[i].content_length == 0
      if i < (objects.count - 1) && objects[i].content_length < AWS.config.s3_multipart_min_part_size
        object1 = objects[i]
        object2 = objects[i + 1]
        DesmondConfig.logger.info "premerging #{object1.key} (#{object1.content_length}) and #{object2.key} (#{object2.content_length})" unless DesmondConfig.logger.nil?
        __merge_objects_normal_internal([object1, object2], src_bucket, object2.key + '_merged', object1.content_length + object2.content_length)
        object1.delete
        object2.delete
        return __prepare_objects_for_multipart(src_bucket, src_prefix)
      end
    end
  end
  private_class_method :__prepare_objects_for_multipart

  ##
  # takes an iterable collection of +src_objects+ and merge them into +dest_bucket+/+dest_key+
  # by downloading and reuploading them.
  #
  def self.__merge_objects_normal_internal(src_objects, dest_bucket, dest_key, total_size)
    writer = Desmond::Streams::S3::S3Writer.new(dest_bucket.name, dest_key)
    src_objects.each do |source_object|
      source_object.read do |chunk|
        writer.write(chunk)
      end
    end
    return dest_bucket.objects[dest_key]
  ensure
    writer.close unless writer.nil?
  end
  private_class_method :__merge_objects_normal_internal
end
