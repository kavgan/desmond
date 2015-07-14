require_relative '../spec_helper'
require 'stringio'

describe Desmond::Streams::CSV::CSVReader do
  def guess_separators(content)
    Desmond::Streams::CSV::CSVReader.guess_separators(StringIO.new(content))
  end

  before(:example) do
    @str_reader = StringIO.new("a,b\nc,d\ne,f")
    @reader = Desmond::Streams::CSV::CSVReader.new(@str_reader)
    @str_writer = StringIO.new
    @writer = Desmond::Streams::CSV::CSVReader.new(@str_writer)
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
    expect(@str_reader.closed?).to eq(true)
  end

  it 'should skip headers' do
    @reader = Desmond::Streams::CSV::CSVReader.new(@str_reader, headers: :first_row)
    expect(@reader.headers).to eq(['a', 'b'])
    expect(@reader.read).to match_array(['c', 'd'])
    expect(@reader.read).to match_array(['e', 'f'])
    expect(@reader.read).to be_nil
  end

  it 'should provide option to override headers' do
    @reader = Desmond::Streams::CSV::CSVReader.new(@str_reader, headers: ['x', 'y'])
    expect(@reader.headers).to eq(['x', 'y'])
    expect(@reader.read).to match_array(['a', 'b'])
    expect(@reader.read).to match_array(['c', 'd'])
    expect(@reader.read).to match_array(['e', 'f'])
    expect(@reader.read).to be_nil
  end

  it 'should be able to skip header row' do
    @reader = Desmond::Streams::CSV::CSVReader.new(@str_reader, headers: ['x', 'y'], skip_rows: 1)
    expect(@reader.headers).to eq(['x', 'y'])
    expect(@reader.read).to match_array(['c', 'd'])
    expect(@reader.read).to match_array(['e', 'f'])
    expect(@reader.read).to be_nil
  end

  it 'should be able to return the header row' do
    @reader = Desmond::Streams::CSV::CSVReader.new(@str_reader, headers: ['x', 'y'], return_headers: true)
    expect(@reader.headers).to eq(['x', 'y'])
    expect(@reader.read).to match_array(['x', 'y'])
    expect(@reader.read).to match_array(['a', 'b'])
    expect(@reader.read).to match_array(['c', 'd'])
    expect(@reader.read).to match_array(['e', 'f'])
    expect(@reader.read).to be_nil
  end

  it 'should be able to parse and return the header row with calling headers before' do
    @reader = Desmond::Streams::CSV::CSVReader.new(@str_reader, headers: :first_row, return_headers: true)
    expect(@reader.headers).to eq(['a', 'b'])
    expect(@reader.read).to match_array(['a', 'b'])
    expect(@reader.read).to match_array(['c', 'd'])
    expect(@reader.read).to match_array(['e', 'f'])
    expect(@reader.read).to be_nil
  end

  it 'should be able to parse and return the header row without calling headers before' do
    @reader = Desmond::Streams::CSV::CSVReader.new(@str_reader, headers: :first_row, return_headers: true)
    expect(@reader.read).to match_array(['a', 'b'])
    expect(@reader.read).to match_array(['c', 'd'])
    expect(@reader.read).to match_array(['e', 'f'])
    expect(@reader.read).to be_nil
  end

  it 'should guess col_sep correctly' do
    expect(guess_separators("a,b\nc,d\ne,f")[:col_sep]).to eq(',')
    expect(guess_separators("a|b\nc|d\ne|f")[:col_sep]).to eq('|')
    expect(guess_separators("a;b\nc;d\ne;f")[:col_sep]).to eq(';')
    expect(guess_separators("a\tb\nc\td\ne\tf")[:col_sep]).to eq("\t")
  end

  it 'should guess quote_char correctly' do
    expect(guess_separators("a,b\nc,d\ne,f")[:quote_char]).to eq('"')
    expect(guess_separators("'a',b\nc,'d'\ne,f")[:quote_char]).to eq("'")
    expect(guess_separators("a,b\nc,d\ne,\"f\"")[:quote_char]).to eq("\"")
    expect(guess_separators("a,b\nc,'d\ne,\"f\"")[:quote_char]).to eq("\"")
    expect(guess_separators("a,b'\nc,d\ne,f")[:quote_char]).to eq('"')
  end

  it 'should guess row_sep correctly' do
    expect(guess_separators("a,b\nc,d\ne,f")[:row_sep]).to eq("\n")
    expect(guess_separators("a,b\r\nc,d\r\ne,f")[:row_sep]).to eq("\r\n")
    expect(guess_separators("a,b\rc,d\re,f")[:row_sep]).to eq("\r")
  end
end
