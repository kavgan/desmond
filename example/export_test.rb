#!/usr/bin/env ruby

require_relative '../lib/desmond'

if __FILE__ == $0
  puts Desmond::ExportJob.test('UserId', "SELECT * FROM tobias.test;",
    {
      connection_id: ARGV[0],
      username: ARGV[1],
      password: ARGV[2]
    })
end
