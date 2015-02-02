require_relative '../spec_helper'
require 'stringio'

describe Desmond::Streams::Reader do
  it 'should not have read implemented' do
    expect { Desmond::Streams::Reader.new.read }.to raise_error(NotImplementedError)
  end
  it 'should not have close implemented' do
    expect { Desmond::Streams::Reader.new.close }.to raise_error(NotImplementedError)
  end
  it 'should not have rewind implemented' do
    expect { Desmond::Streams::Reader.new.rewind }.to raise_error(NotImplementedError)
  end
  it 'should have eof be false by default' do
    expect(Desmond::Streams::Reader.new.eof?).to eq(false)
  end
end

describe Desmond::Streams::LineReader do
  before(:example) do
    @str_reader = StringIO.new("a\nb\nc")
    @reader = Desmond::Streams::LineReader.new(@str_reader)
  end

  it 'should read line by line' do
    expect(@reader.read).to eq("a\n")
    expect(@reader.read).to eq("b\n")
    expect(@reader.read).to eq("c")
    expect(@reader.read).to eq(nil)
  end

  it 'should set eof' do
    while not(@reader.read.nil?) do
    end
    expect(@reader.eof?).to eq(true)
  end

  it 'should close the reader' do
    @reader.close
    expect(@str_reader.closed?).to eq(true)
  end

  it 'should support different newline characters' do
    reader = Desmond::Streams::LineReader.new(StringIO.new("a\rb\rc"), newline: "\r")
    expect(@reader.read).to eq("a\n")
    expect(@reader.read).to eq("b\n")
    expect(@reader.read).to eq("c")
    expect(@reader.read).to eq(nil)
  end
end

describe Desmond::Streams::Writer do
  it 'should not have write implemented' do
    expect { Desmond::Streams::Writer.new.write }.to raise_error(NotImplementedError)
  end
  it 'should not have close implemented' do
    expect { Desmond::Streams::Writer.new.close }.to raise_error(NotImplementedError)
  end
  it 'should not have options by default' do
    expect(Desmond::Streams::Writer.new.options).to eq({})
  end
end
