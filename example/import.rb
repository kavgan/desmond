#!/usr/bin/env ruby

require_relative '../lib/desmond'
Que.mode = :sync
Que.logger = Logger.new STDOUT

if __FILE__ == $0
  Desmond::ImportJob.enqueue(1, 1, {
    db: {
      connection_id: ARGV[0],
      username: ARGV[1],
      password: ARGV[2],
      schema: 'tobias',
      table: 'importtest',
      dropifexists: true
    },
    s3: {
      bucket: 'amg-tobi-test',
      key: 'polizei_export_MyApp_JobId_UserId_2015_01_23T15_30_39_060Z.csv',
      access_key_id: ARGV[3],
      secret_access_key: ARGV[4]
    },
    csv: {
      headers: :first_row
    }
  })
end
