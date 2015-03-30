require_relative '../spec_helper'

describe Desmond::BaseJobNoJobId do
  def new_job(&block)
    clazz_name = "DemondTestJobNoJobId#{rand(4096)}"
    clazz = Class.new(Desmond::BaseJobNoJobId) do
      define_method(:name) do
        clazz_name
      end

      self.instance_eval &block unless block.nil?
    end
    # create a global name for it, so we can run it async (worker needs to be able to find the class by name)
    Object.const_set(clazz_name, clazz)
    clazz
  end

  it 'should run minimal job successfully' do
    expect(new_job.enqueue(1).done?).to eq(true)
  end

  it 'should run custom code' do
    clazz = new_job do
      @job_id = 0
      singleton_class.class_eval do
        attr_accessor :job_id
      end

      define_method(:execute) do |job_id, user_id, options={}|
        self.class.job_id += job_id
      end
    end
    clazz.default_job_id = 42
    expect(clazz.enqueue(1).done?).to eq(true)
    expect(clazz.job_id).to eq(42)
  end

  it 'should be able to find runs without job_id' do
    clazz = new_job do
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
