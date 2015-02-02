require_relative '../spec_helper'

describe Desmond::ExportJob do

  before(:context) do
    c = ActiveRecord::Base.connection
    c.execute("DROP TABLE IF EXISTS exportdata")
    c.execute("CREATE TABLE exportdata(id INT, txt VARCHAR)")
    c.execute("INSERT INTO exportdata VALUES(0, 'null')")
    c.execute("INSERT INTO exportdata VALUES(1, 'eins')")
  end

  it 'should export to csv' do
    run = Desmond::ExportJob.enqueue('JobId', 'UserId',
      csv: {
        col_sep: '|',
        return_headers: false
      },
      db: {
        connection_id: 'test',
        query: "SELECT * FROM exportdata;"
      },
      s3: {
        bucket: @config[:export_bucket]
      }
    )
    s3_obj = AWS::S3.new.buckets[@config[:export_bucket]].objects[run.filename]
    begin
      expect(s3_obj.read).to eq("0|null\n1|eins\n")
    ensure
      s3_obj.delete
    end
  end
end
