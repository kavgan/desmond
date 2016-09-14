require_relative '../spec_helper'
require 'stringio'

describe Desmond::Streams::S3::S3Reader do
  context 'simple object' do
    before(:example) do
      @unique_name = "desmond_test_#{rand(1024)}"
      Aws::S3::Bucket.new(@config[:import_bucket]).object(@unique_name).put(body: 'a')
      @reader = Desmond::Streams::S3::S3Reader.new(@config[:import_bucket], @unique_name)
    end
    after(:example) do
      Aws::S3::Bucket.new(@config[:import_bucket]).object(@unique_name).delete
    end

    it 'should return correct credentials' do
      expect(@reader.credentials).to eq("aws_access_key_id=#{Aws.config[:access_key_id]};aws_secret_access_key=#{Aws.config[:secret_access_key]}")
    end

    it 'should have eof be false by default' do
      expect(@reader.eof?).to eq(false)
    end

    it 'should read correct data and set eof' do
      data = ''
      while true do
        read = @reader.read
        break if read.nil?
        data += read
      end
      expect(data).to eq('a')
      expect(@reader.eof?).to eq(true)
    end

    it 'should close the reader' do
      @reader.close
      expect(@reader.closed?).to eq(true)
    end
  end

  context 'folder object' do
    before(:example) do
      @unique_name = "desmond_test_#{rand(1024)}"
      Aws::S3::Bucket.new(@config[:import_bucket]).object(@unique_name + '_$folder$/part00').put(body: 'a')
      Aws::S3::Bucket.new(@config[:import_bucket]).object(@unique_name + '_$folder$/part01').put(body: "b\n")
      Aws::S3::Bucket.new(@config[:import_bucket]).object(@unique_name + '_$folder$/part02').put(body: 'c')
      @reader = Desmond::Streams::S3::S3Reader.new(@config[:import_bucket], @unique_name)
    end
    after(:example) do
      Aws::S3::Bucket.new(@config[:import_bucket]).objects(prefix: @unique_name).each { |o| o.delete }
    end

    it 'should have eof be false by default' do
      expect(@reader.eof?).to eq(false)
    end

    it 'should read correct data and set eof' do
      data = ''
      while true do
        read = @reader.read
        break if read.nil?
        data += read
      end
      expect(data).to eq("ab\nc")
      expect(@reader.eof?).to eq(true)
    end

    it 'should close the reader' do
      @reader.close
      expect(@reader.closed?).to eq(true)
    end
  end
end

describe Desmond::Streams::S3::S3Writer do
  it 'should support streamed writing' do
    unique_name = "desmond_test_#{rand(1024)}"
    content1 = "a,b\n"
    content2 = "c,d\n"
    begin
      w = Desmond::Streams::S3::S3Writer.new(@config[:import_bucket], unique_name)
      w.write content1
      w.write content2
      w.close

      content_all = Aws::S3::Bucket.new(@config[:import_bucket]).object(unique_name).get.body.read
    ensure
      Aws::S3::Bucket.new(@config[:import_bucket]).object(unique_name).delete
    end
    expect(content_all).to eq(content1 + content2)
  end
end
