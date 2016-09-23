class S3Util
  MIN_PART_SIZE =     5_242_880 # AWS S3 can take a minimum of 5MB per multipart part
  MAX_COPY_SIZE = 5_368_709_120 # AWS S3 can take a maximum of 5GB per multipart part

  ##
  # merges a folder of S3 objects into one single object
  #
  # opts supported keys:
  # - access_key_id
  # - secret_access_key
  #
  def self.merge_objects(src_bucket, src_prefix, dest_bucket, dest_key, opts={})
    DesmondConfig.logger.info "S3Util.merge_objects with #{src_bucket}, #{src_prefix}, #{dest_bucket}, #{dest_key}" unless DesmondConfig.logger.nil?

    src_bucket  = Aws::S3::Bucket.new(src_bucket,  opts)
    dest_bucket = Aws::S3::Bucket.new(dest_bucket, opts)

    # calcluate total size to determine how to merge
    total_size  = 0
    src_objects = src_bucket.objects(prefix: src_prefix).each.to_a
    num_objects = src_objects.count
    return dest_bucket.object(dest_key) if num_objects == 1 && src_bucket == dest_bucket && src_prefix == dest_key
    raise ArgumentError, 'source and destination cannot be the same' if src_bucket == dest_bucket && src_prefix == dest_key
    src_objects.each do |source_object|
      total_size += source_object.content_length
    end

    # do the merge
    # if every source object is approximatly bigger than the S3 multipart upload threshold, we do a multipart
    if total_size <= num_objects * MIN_PART_SIZE
      __merge_objects_normal(src_bucket, src_objects, dest_bucket, dest_key, src_prefix, total_size, opts)
    else
      __merge_objects_multipart(src_bucket, src_objects, dest_bucket, dest_key, src_prefix, total_size, opts)
    end
  end

  ##
  # strategy: the total size of all source objects is rather small (< 5MB), so we download them all
  #           and reupload them allat once concatenated
  #
  def self.__merge_objects_normal(src_bucket, src_objects, dest_bucket, dest_key, src_prefix, total_size, opts={})
    DesmondConfig.logger.info 'using download copy' unless DesmondConfig.logger.nil?
    return __merge_objects_normal_internal(src_objects, dest_bucket, dest_key, total_size, opts)
  end
  private_class_method :__merge_objects_normal

  ##
  # strategy: the total size of all source object can be large (> 5MB), so we tell S3 to copy them
  #           without ever downloading them.
  #
  def self.__merge_objects_multipart(src_bucket, src_objects, dest_bucket, dest_key, src_prefix, total_size, opts={})
    DesmondConfig.logger.info 'using multipart copy' unless DesmondConfig.logger.nil?
    # premerge objects which might be too small
    __prepare_objects_for_multipart(src_bucket, src_objects, src_prefix, opts)
    DesmondConfig.logger.info 'premerge done' unless DesmondConfig.logger.nil?
    # reloading src_objects after premerging
    src_objects = src_bucket.objects(prefix: src_prefix).each.to_a
    # first, figure out which parts we'll have
    parts         = []
    max_part_size = self::MAX_COPY_SIZE
    num_objects   = src_objects.count
    part_no = 1
    src_objects.each_with_index do |source_object, i|
      source_path   = "#{source_object.bucket.name}/#{source_object.key}"
      source_length = source_object.content_length
      DesmondConfig.logger.info "source_path: #{source_path}" unless DesmondConfig.logger.nil?
      # skip some useless files
      next if source_length == 0
      next if source_object.key.end_with?('.gz') && source_length == 20 # empty gzip file
      pos = 0
      until pos >= source_length
        DesmondConfig.logger.info "no: #{part_no}, pos: #{pos}, source_length: #{source_length}" unless DesmondConfig.logger.nil?
        # trying to make parts as big as supported
        last_byte = (pos + max_part_size >= source_length) ? source_length - 1 : pos + max_part_size - 1
        DesmondConfig.logger.info "last_byte: #{last_byte}" unless DesmondConfig.logger.nil?
        # make sure we have at least MIN_PART_SIZE remaining bytes
        remaining_bytes = source_length - (last_byte + 1)
        if remaining_bytes > 0 && remaining_bytes < self::MIN_PART_SIZE
          last_byte = (source_length - pos) / 2 # if less than MIN_PART_SIZE would be left, make 2 equal parts
        end
        DesmondConfig.logger.info "last_byte: #{last_byte}" unless DesmondConfig.logger.nil?
        parts << { no: part_no, source: source_path, byte_range: "bytes=#{pos}-#{last_byte}", size: (last_byte - pos) }
        part_no += 1
        pos = last_byte + 1
      end
    end
    DesmondConfig.logger.info "parts: #{parts}" unless DesmondConfig.logger.nil?

    # second, actually do the work in parallel
    obj_aggregate   = dest_bucket.object(dest_key).initiate_multipart_upload
    completed_parts = []
    threads = parts.each_with_index.map do |part, idx|
      Thread.new do
        DesmondConfig.logger.info "copying part #{part[:no]}, #{part[:source]}: #{part[:byte_range]} => #{part[:size]} bytes" unless DesmondConfig.logger.nil?
        new_part = obj_aggregate.part(idx + 1)
        uploaded_part = new_part.copy_from(copy_source: part[:source], copy_source_range: part[:byte_range])
        completed_parts << {
          part_number: new_part.part_number,
          etag: uploaded_part.copy_part_result.etag,
        }
      end
    end
    DesmondConfig.logger.info "waiting for copy threads to finish" unless DesmondConfig.logger.nil?
    threads.each(&:join) # wait for the threads to finish
    completed_parts.sort_by! { |p| p[:part_number] }
    DesmondConfig.logger.debug "completing upload: #{completed_parts}" unless DesmondConfig.logger.nil?
    return obj_completed = obj_aggregate.complete(multipart_upload: { parts: completed_parts })
  rescue
    obj_aggregate.abort unless obj_aggregate.nil?
    raise
  end
  private_class_method :__merge_objects_multipart

  ##
  # S3 multipart upload only allows the last part to be smaller than `AWS.config.s3_multipart_min_part_size`.
  # if some other objects are smaller this method tries to merge them together with the following one.
  #
  def self.__prepare_objects_for_multipart(src_bucket, src_objects, src_prefix, opts={})
    objects = src_objects
    for i in 0...objects.count
      next if objects[i].content_length == 0
      if i < (objects.count - 1) && objects[i].content_length < MIN_PART_SIZE
        object1 = objects[i]
        object2 = objects[i + 1]
        DesmondConfig.logger.info "premerging #{object1.key} (#{object1.content_length}) and #{object2.key} (#{object2.content_length})" unless DesmondConfig.logger.nil?
        __merge_objects_normal_internal([object1, object2], src_bucket, object2.key + '_merged', object1.content_length + object2.content_length, opts)
        object1.delete
        object2.delete
        # reloading src_objects
        src_objects = src_bucket.objects(prefix: src_prefix).each.to_a
        return __prepare_objects_for_multipart(src_bucket, src_objects, src_prefix, opts)
      end
    end
  end
  private_class_method :__prepare_objects_for_multipart

  ##
  # takes an iterable collection of +src_objects+ and merge them into +dest_bucket+/+dest_key+
  # by downloading and reuploading them.
  #
  def self.__merge_objects_normal_internal(src_objects, dest_bucket, dest_key, total_size, opts={})
    writer = Desmond::Streams::S3::S3Writer.new(dest_bucket.name, dest_key, opts)
    src_objects.each do |source_object|
      DesmondConfig.logger.info "uploading #{source_object.key} (#{source_object.content_length})" unless DesmondConfig.logger.nil?
      source_object.get do |chunk|
        writer.write(chunk)
      end
    end
    return dest_bucket.object(dest_key)
  ensure
    writer.close unless writer.nil?
  end
  private_class_method :__merge_objects_normal_internal
end
