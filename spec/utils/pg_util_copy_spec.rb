require_relative '../spec_helper'

describe 'pgutil copy from' do
  before(:each) do
    c = ActiveRecord::Base.connection
    c.execute("DROP TABLE IF EXISTS copytest")
    c.execute("CREATE TABLE copytest(id INT, txt VARCHAR)")
  end

  after(:each) do
    c = ActiveRecord::Base.connection
    c.execute("DROP TABLE IF EXISTS copytest")
  end

  it 'copy command should work and stop with nil' do
    i = 0
    Desmond::PGUtil.copy_from(
      ActiveRecord::Base.connection,
      "copy copytest from stdin with (format csv, delimiter '|', quote '\"', null '__NULL__')"
    ) do
      i += 1
      if    i == 1
        "1|blub\n"
      elsif i == 2
        "\"2\"|\"blab\"\n"
      elsif i == 3
        "3|__NULL__\n"
      else
        nil
      end
    end
    expect(ActiveRecord::Base.connection.execute('select * from copytest').to_a).to eq([
      { 'id' => '1', 'txt' => 'blub' },
      { 'id' => '2', 'txt' => 'blab' },
      { 'id' => '3', 'txt' => nil }
    ])
  end

  it 'copy command should work and stop with empty string' do
    i = 0
    Desmond::PGUtil.copy_from(
      ActiveRecord::Base.connection,
      "copy copytest from stdin with (format csv, delimiter '|', quote '\"', null '__NULL__')"
    ) do
      i += 1
      if    i == 1
        "1|blub\n"
      elsif i == 2
        "\"2\"|\"blab\"\n"
      elsif i == 3
        "3|__NULL__\n"
      else
        ''
      end
    end
    expect(ActiveRecord::Base.connection.execute('select * from copytest').to_a).to eq([
      { 'id' => '1', 'txt' => 'blub' },
      { 'id' => '2', 'txt' => 'blab' },
      { 'id' => '3', 'txt' => nil }
    ])
  end
end
