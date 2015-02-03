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
  it 'should delete S3 object on error' do
    unique_name = "desmond_test_#{rand(1024)}"
    s3_obj = AWS::S3.new.buckets[@config[:import_bucket]].objects.create(unique_name, '')
    w = Desmond::Streams::S3::S3Writer.new(@config[:import_bucket], unique_name)
    expect { w.write_from(nil) }.to raise_error
    expect(s3_obj.exists?).to eq(false)
  end
end
