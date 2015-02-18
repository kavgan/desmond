require_relative 'spec_helper'

describe DesmondConfig do
  it 'should ignore Que\'s logger' do
    begin
      prev_logger = DesmondConfig.logger
      test_logger = Logger.new STDERR
      DesmondConfig.logger = test_logger
      expect(DesmondConfig.logger).to eq(test_logger)
      expect(Que.logger).to eq(test_logger)
      Que.logger = Logger.new STDOUT
      expect(DesmondConfig.logger).to eq(test_logger)
      expect(Que.logger).to eq(test_logger)
    ensure
      # reset logger to previous value
      DesmondConfig.logger = prev_logger
    end
  end
end
