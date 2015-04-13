require_relative '../spec_helper'

describe Desmond::BaseJobNoJobId do
  include JobTestHelpers

  it 'should run minimal job successfully' do
    expect(new_job(Desmond::BaseJobNoJobId).enqueue(1).done?).to eq(true)
  end

  it 'should run custom code' do
    clazz = new_job(Desmond::BaseJobNoJobId) do
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
end
