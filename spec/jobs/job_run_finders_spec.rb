require_relative '../spec_helper'

describe Desmond::BaseJob do
  include JobTestHelpers

  it 'should be able to find the last run' do
    clazz = new_job do
      define_method(:execute) do |job_id, user_id, options={}|
        42
      end
    end
    expect(clazz.last_run(1)).to eq(nil)
    expect(clazz.enqueue(1, 1).done?).to eq(true)
    expect(clazz.last_run(1).result).to eq(42)
    expect(clazz.last_run(1, 1).result).to eq(42)
  end

  it 'should be able to find the last finished run' do
    clazz = new_job do
      define_method(:execute) do |job_id, user_id, options={}|
        42
      end
    end
    expect(clazz.last_finished_run(1)).to eq(nil)
    self.async do
      expect(clazz.enqueue(1, 1).done?).to eq(false)
      expect(clazz.last_run(1)).not_to eq(nil)
      expect(clazz.last_finished_run(1)).to eq(nil)
      self.async_worker(wait: true)
      expect(clazz.last_run(1).result).to eq(42)
      expect(clazz.last_finished_run(1).result).to eq(42)
    end
    expect(clazz.last_finished_run(1).result).to eq(42)
    expect(clazz.last_finished_run(1, 1).result).to eq(42)
  end

  it 'should be able to find multiple last runs' do
    clazz = new_job
    expect(clazz.last_runs(1, 2).size).to eq(0)
    expect(clazz.enqueue(1, 1).done?).to eq(true)
    expect(clazz.last_runs(1, 2).size).to eq(1)
    expect(clazz.enqueue(1, 2).done?).to eq(true)
    expect(clazz.last_runs(1, 2).size).to eq(2)
    expect(clazz.last_runs(1, 2, 1).size).to eq(1)
    expect(clazz.last_runs(1, 2, 2).size).to eq(1)
  end

  it 'should be able to find unfinished runs' do
    clazz = new_job
    expect(clazz.runs_unfinished(1).size).to eq(0)
    self.async do
      run = clazz.enqueue(1, 1)
      expect(clazz.runs_unfinished(1).size).to eq(1)
      expect(clazz.runs_unfinished(1, 1).size).to eq(1)
      expect(clazz.runs_unfinished(1, 2).size).to eq(0)
      self.async_worker(wait: true)
      run.reload
      expect(run.finished?).to eq(true)
    end
    expect(clazz.runs_unfinished(1).size).to eq(0)
    expect(clazz.runs_unfinished(1, 1).size).to eq(0)
    expect(clazz.runs_unfinished(1, 2).size).to eq(0)
  end

  it 'should be able to find runs without job_id' do
    clazz = new_job(Desmond::BaseJobNoJobId) do
      define_method(:execute) do |job_id, user_id, options={}|
        42
      end
    end
    expect(clazz.enqueue(1).done?).to eq(true)
    expect(clazz.last_run(1).result).to eq(42)
    expect(clazz.last_runs(1)[0].result).to eq(42)
    expect(clazz.runs_unfinished(1).size).to eq(0)
  end
end
