#!/usr/bin/env ruby

require_relative '../lib/desmond'

if __FILE__ == $0
  Desmond::ExportJob.enqueue('MyApp_JobId', 'UserId', "SELECT * FROM tobias.test;",
    delimiter: '|',
    include_header: true,
    job: {
      name: 'Test Job',
      mail_success: 'tobi@amg.tv',
      mail_failure: 'tobi@amg.tv'
    },
    db: {
      connection_id: ARGV[0],
      username: ARGV[1],
      password: ARGV[2]
    },
    s3: {
      access_key_id: ARGV[3],
      secret_access_key: ARGV[4],
      bucket_name: ARGV[5]
    })
end
