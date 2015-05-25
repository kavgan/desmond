require_relative '../spec_helper'
require 'stringio'

describe Desmond::Streams::S3::S3Reader do
  before(:example) do
    @unique_name = "desmond_test_#{rand(1024)}"
    AWS::S3.new.buckets[@config[:import_bucket]].objects.create(@unique_name, 'a')
    @reader = Desmond::Streams::S3::S3Reader.new(@config[:import_bucket], @unique_name)
  end
  after(:example) do
    AWS::S3.new.buckets[@config[:import_bucket]].objects[@unique_name].delete
  end

  it 'should have eof be false by default' do
    expect(@reader.eof?).to eq(false)
  end

  it 'should set eof' do
    while !@reader.read.nil? do
    end
    expect(@reader.eof?).to eq(true)
  end

  it 'should close the reader' do
    @reader.close
    expect(@reader.closed?).to eq(true)
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

      content_all = AWS::S3.new.buckets[@config[:import_bucket]].objects[unique_name].read
    ensure
      AWS::S3.new.buckets[@config[:import_bucket]].objects[unique_name].delete
    end
    expect(content_all).to eq(content1 + content2)
  end
end
