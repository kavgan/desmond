require_relative '../spec_helper'
require 'stringio'

describe Desmond::Streams::CSV::CSVWriter do
  before(:example) do
    @str_writer = StringIO.new
    @writer = Desmond::Streams::CSV::CSVWriter.new(@str_writer)
  end

  it 'should close the reader' do
    @writer.close
    expect(@str_writer.closed?).to eq(true)
  end

  it 'should parse headers' do
    @writer = Desmond::Streams::CSV::CSVWriter.new(@str_writer, headers: :first_row)
    @writer.write(['a', 'b'])
    @writer.write(['c', 'd'])
    expect(@writer.headers).to eq(['a', 'b'])
    expect(@str_writer.string).to eq("c,d\n")
  end

  it 'should parse and return headers' do
    @writer = Desmond::Streams::CSV::CSVWriter.new(@str_writer, headers: :first_row, return_headers: true)
    @writer.write(['a', 'b'])
    @writer.write(['c', 'd'])
    expect(@writer.headers).to eq(['a', 'b'])
    expect(@str_writer.string).to eq("a,b\nc,d\n")
  end

  it 'should parse headers' do
    @writer = Desmond::Streams::CSV::CSVWriter.new(@str_writer, headers: ['x', 'y'])
    @writer.write(['a', 'b'])
    @writer.write(['c', 'd'])
    expect(@writer.headers).to eq(['x', 'y'])
    expect(@str_writer.string).to eq("a,b\nc,d\n")
  end

  it 'should parse and return headers' do
    @writer = Desmond::Streams::CSV::CSVWriter.new(@str_writer, headers: ['x', 'y'], return_headers: true)
    @writer.write(['a', 'b'])
    @writer.write(['c', 'd'])
    expect(@writer.headers).to eq(['x', 'y'])
    expect(@str_writer.string).to eq("x,y\na,b\nc,d\n")
  end

  it 'should support custom column separators' do
    @writer = Desmond::Streams::CSV::CSVWriter.new(@str_writer, col_sep: '|')
    @writer.write(['a', 'b'])
    @writer.write(['c', 'd'])
    expect(@str_writer.string).to eq("a|b\nc|d\n")
  end

  it 'should support custom row separators' do
    @writer = Desmond::Streams::CSV::CSVWriter.new(@str_writer, row_sep: "\r")
    @writer.write(['a', 'b'])
    @writer.write(['c', 'd'])
    expect(@str_writer.string).to eq("a,b\rc,d\r")
  end

  it 'should have a default quote character' do
    @writer = Desmond::Streams::CSV::CSVWriter.new(@str_writer, force_quotes: true)
    @writer.write(['a', 'b'])
    @writer.write(['c', 'd'])
    expect(@str_writer.string).to eq("\"a\",\"b\"\n\"c\",\"d\"\n")
  end

  it 'should support custom quote characters' do
    @writer = Desmond::Streams::CSV::CSVWriter.new(@str_writer, quote_char: "'", force_quotes: true)
    @writer.write(['a', 'b'])
    @writer.write(['c', 'd'])
    expect(@str_writer.string).to eq("'a','b'\n'c','d'\n")
  end

  it 'should detect when quoting is necessary' do
    @writer = Desmond::Streams::CSV::CSVWriter.new(@str_writer, quote_char: "'")
    @writer.write(['a', 'b,'])
    @writer.write(['c', 'd'])
    expect(@str_writer.string).to eq("a,'b,'\nc,d\n")
  end
end
