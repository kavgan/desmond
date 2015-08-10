require_relative '../spec_helper'

describe Desmond::Streams::Database::DatabaseCursorReader do
  it 'should expect execute to be implemented later' do
    expect do
      r = Desmond::Streams::Database::DatabaseCursorReader.new('test', 'invalid')
      r.read
    end.to raise_error(NotImplementedError)
  end
end
