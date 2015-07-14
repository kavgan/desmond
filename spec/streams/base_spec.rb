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

  it ' should guess the newline charcater correctly' do
    expect(Desmond::Streams::LineReader.guess_newline_char(StringIO.new("a\nb\nc"))).to eq("\n")
    expect(Desmond::Streams::LineReader.guess_newline_char(StringIO.new("a\rb\rc"))).to eq("\r")
    expect(Desmond::Streams::LineReader.guess_newline_char(StringIO.new("a\r\nb\r\nc"))).to eq("\r\n")
    expect(Desmond::Streams::LineReader.guess_newline_char(StringIO.new("a\nb\rc\r"))).to eq("\r")
    expect(Desmond::Streams::LineReader.guess_newline_char(StringIO.new("a\nb\rc\n"))).to eq("\n")
  end

  it 'should read line by line with \r' do
    @str_reader = StringIO.new("a\rb\rc")
    @reader = Desmond::Streams::LineReader.guess_and_create(@str_reader)
    expect(@reader.read).to eq("a\r")
    expect(@reader.read).to eq("b\r")
    expect(@reader.read).to eq("c")
    expect(@reader.read).to eq(nil)
  end

  it 'should read line by line with \r\n' do
    @str_reader = StringIO.new("a\r\nb\r\nc")
    @reader = Desmond::Streams::LineReader.guess_and_create(@str_reader)
    expect(@reader.read).to eq("a\r\n")
    expect(@reader.read).to eq("b\r\n")
    expect(@reader.read).to eq("c")
    expect(@reader.read).to eq(nil)
  end

  it 'should skip empty last lines' do
    @str_reader = StringIO.new("a\nb\nc\n")
    @reader = Desmond::Streams::LineReader.guess_and_create(@str_reader)
    expect(@reader.read).to eq("a\n")
    expect(@reader.read).to eq("b\n")
    expect(@reader.read).to eq("c\n")
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
    expect { Desmond::Streams::Writer.new.write('data') }.to raise_error(NotImplementedError)
  end
  it 'should not have close implemented' do
    expect { Desmond::Streams::Writer.new.close }.to raise_error(NotImplementedError)
  end
  it 'should not have options by default' do
    expect(Desmond::Streams::Writer.new.options).to eq({})
  end
end
