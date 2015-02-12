require_relative '../spec_helper'

describe Desmond::BaseJob do
  def new_job(&block)
    Class.new(Desmond::BaseJob) do
      def self.name
        "DemondTestJob#{rand(1024)}"
      end
      def name
        self.class.name
      end

      self.instance_eval &block unless block.nil?
    end
  end

  it 'should run minimal job successfully' do
    expect(new_job.enqueue(1, 1).done?).to eq(true)
  end

  it 'should run custom code' do
    clazz = new_job do
      @test_counter = 0
      singleton_class.class_eval do
        attr_accessor :test_counter
      end

      define_method(:execute) do |job_id, user_id, options={}|
        self.class.test_counter += 1
      end
    end
    expect(clazz.enqueue(1, 1).done?).to eq(true)
    expect(clazz.test_counter).to eq(1)
  end

  it 'should run before hook' do
    clazz = new_job do
      @test_counter = 0
      singleton_class.class_eval do
        attr_accessor :test_counter
      end

      define_method(:before) do |job_run, job_id, user_id, options={}|
        self.class.test_counter = 1 if self.class.test_counter == 0
      end

      define_method(:execute) do |job_id, user_id, options={}|
        self.class.test_counter += 1  if self.class.test_counter > 0
      end
    end
    expect(clazz.enqueue(1, 1).done?).to eq(true)
    expect(clazz.test_counter).to eq(2)
  end

  it 'should run error & after hook' do
    clazz = new_job do
      @test_counter = 0
      singleton_class.class_eval do
        attr_accessor :test_counter
      end

      define_method(:error) do |job_run, job_id, user_id, options={}|
        self.class.test_counter = 1 if self.class.test_counter == 0
      end

      define_method(:success) do |job_run, job_id, user_id, options={}|
        self.class.test_counter += 1 # shouldn't be executed
      end

      define_method(:after) do |job_run, job_id, user_id, options={}|
        self.class.test_counter = 2 if self.class.test_counter == 1
      end

      define_method(:execute) do |job_id, user_id, options={}|
        fail 'Expected behavior'
      end
    end
    expect(clazz.enqueue(1, 1).failed?).to eq(true)
    expect(clazz.test_counter).to eq(2)
  end

  it 'should success error & after hook' do
    clazz = new_job do
      @test_counter = 0
      singleton_class.class_eval do
        attr_accessor :test_counter
      end

      define_method(:error) do |job_run, job_id, user_id, options={}|
        self.class.test_counter += 1 # shouldn't be executed
      end

      define_method(:success) do |job_run, job_id, user_id, options={}|
        self.class.test_counter = 1 if self.class.test_counter == 0
      end

      define_method(:after) do |job_run, job_id, user_id, options={}|
        self.class.test_counter = 2 if self.class.test_counter == 1
      end

      define_method(:execute) do |job_id, user_id, options={}|
        done
      end
    end
    expect(clazz.enqueue(1, 1).done?).to eq(true)
    expect(clazz.test_counter).to eq(2)
  end

  it 'should run after hook' do
    clazz = new_job do
      @test_counter = 0
      singleton_class.class_eval do
        attr_accessor :test_counter
      end

      define_method(:after) do |job_run, job_id, user_id, options={}|
        self.class.test_counter += 1 if self.class.test_counter > 0
      end

      define_method(:execute) do |job_id, user_id, options={}|
        self.class.test_counter = 1  if self.class.test_counter == 0
      end
    end
    expect(clazz.enqueue(1, 1).done?).to eq(true)
    expect(clazz.test_counter).to eq(2)
  end

  it 'should save data into job run' do
    clazz = new_job do
      define_method(:execute) do |job_id, user_id, options={}|
        done(testdata: true)
      end
    end
    expect(clazz.enqueue(1, 1).details).to eq('testdata' => true)
  end

  it 'should fail the job on uncaught exception' do
    clazz = new_job do
      define_method(:execute) do |job_id, user_id, options={}|
        fail 'Expected behavior'
      end
    end
    run = clazz.enqueue(1, 1)
    expect(run.failed?).to eq(true)
    expect(run.error).to eq('Expected behavior')
  end

  it 'should ignore uncaught exception in hook' do
    clazz = new_job do
      define_method(:before) do |job_run, job_id, user_id, options={}|
        fail 'Fatal error'
      end
      define_method(:after) do |job_run, job_id, user_id, options={}|
        fail 'Fatal error'
      end
    end
    run = clazz.enqueue(1, 1)
    expect(run.done?).to eq(true)
    expect(run.error).to eq(nil)
  end
end
