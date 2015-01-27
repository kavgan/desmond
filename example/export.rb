#!/usr/bin/env ruby

require_relative '../lib/desmond'
Que.mode = :sync
Que.logger = Logger.new STDOUT

if __FILE__ == $0
  Desmond::ExportJob.enqueue('JobId', 'UserId',
    csv: {
      col_sep: '|',
      return_headers: true
    },
    #job: {
    #  name: 'Test Job'#,
    #  mail_success: 'tobi@amg.tv',
    #  mail_failure: 'tobi@amg.tv'
    #},
    db: {
      connection_id: ARGV[0],
      username: ARGV[1],
      password: ARGV[2],
      fetch_size: 2,
      query: "SELECT * FROM tobias.test;"
    },
    s3: {
      access_key_id: ARGV[3],
      secret_access_key: ARGV[4],
      bucket: ARGV[5]
    })
end
