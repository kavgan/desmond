require_relative '../spec_helper'
require 'stringio'

describe Desmond::Streams::CSV::CSVArrayReader do
  def guess_separators(content)
    Desmond::Streams::CSV::CSVArrayReader.guess_separators(StringIO.new(content))
  end

  before(:example) do
    @str_reader = StringIO.new("a,b\nc,d\ne,f")
    @reader = Desmond::Streams::CSV::CSVArrayReader.new(@str_reader)
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
    @reader = Desmond::Streams::CSV::CSVArrayReader.new(@str_reader, headers: :first_row)
    expect(@reader.headers).to eq(['a', 'b'])
    expect(@reader.read).to match_array(['c', 'd'])
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
  end
end
