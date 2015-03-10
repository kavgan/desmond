require_relative '../spec_helper'

describe Desmond::ImportJob do
  it 'should choose pg with pg connection' do
    run = Desmond::ImportJob.enqueue('JobId', 'UserId', db: {
      connection_id: 'test'
    })
    expect(run.job_class).to eq('Desmond::ImportPgJob')
  end

  it 'should choose RedShift with RedShift connection' do
    run = Desmond::ImportJob.enqueue('JobId', 'UserId', db: {
      connection_id: 'redshift_test'
    })
    expect(run.job_class).to eq('Desmond::ImportRsJob')
  end
end
