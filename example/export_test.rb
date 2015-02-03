#!/usr/bin/env ruby

require_relative '../lib/desmond'

if __FILE__ == $PROGRAM_NAME
  puts Desmond::ExportJob.test(
    'UserId',
    query: 'SELECT * FROM tobias.test;',
    connection_id: ARGV[0],
    username: ARGV[1],
    password: ARGV[2]
  )
end
