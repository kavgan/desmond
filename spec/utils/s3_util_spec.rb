require_relative '../spec_helper'

describe S3Util do
  context 'download merge' do
    before do
      no = rand(1024)
      @unique_name = "desmond_test_#{no}"
      @unique_name_merged = "desmond_test_merged_#{no}"
      Aws::S3::Bucket.new(@config[:import_bucket]).object(@unique_name + '/part00').put(body: 'a')
      Aws::S3::Bucket.new(@config[:import_bucket]).object(@unique_name + '/part01').put(body: "b\n")
      Aws::S3::Bucket.new(@config[:import_bucket]).object(@unique_name + '/part02').put(body: 'c')
    end
    after do
      Aws::S3::Bucket.new(@config[:import_bucket]).objects(prefix: @unique_name).each { |o| o.delete }
      Aws::S3::Bucket.new(@config[:import_bucket]).objects(prefix: @unique_name_merged).each { |o| o.delete }
    end

    it 'should merge properly' do
      S3Util.merge_objects(@config[:import_bucket], @unique_name, @config[:import_bucket], @unique_name_merged)
      merged_data = Aws::S3::Bucket.new(@config[:import_bucket]).object(@unique_name_merged).get.body.read
      expect(merged_data).to eq("ab\nc")
    end
  end

  context 'multipart merge' do
    before do
      no = rand(1024)
      @unique_name = "desmond_test_#{no}"
      @unique_name_merged = "desmond_test_merged_#{no}"
      Aws::S3::Bucket.new(@config[:import_bucket]).object(@unique_name + '/part00').put(body: 'a' * (5_242_880 + 1))
      Aws::S3::Bucket.new(@config[:import_bucket]).object(@unique_name + '/part01').put(body: 'b' * (5_242_880 + 1))
    end
    after do
      Aws::S3::Bucket.new(@config[:import_bucket]).objects(prefix: @unique_name).each { |o| o.delete }
      Aws::S3::Bucket.new(@config[:import_bucket]).objects(prefix: @unique_name_merged).each { |o| o.delete }
    end

    it 'should merge properly' do
      S3Util.merge_objects(@config[:import_bucket], @unique_name, @config[:import_bucket], @unique_name_merged)
      merged_data = Aws::S3::Bucket.new(@config[:import_bucket]).object(@unique_name_merged).get.body.read
      expected_data = ('a' * (5_242_880 + 1)) + ('b' * (5_242_880 + 1))
      expect(merged_data).to eq(expected_data)
    end
  end
end
