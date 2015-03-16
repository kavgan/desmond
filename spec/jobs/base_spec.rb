require_relative '../spec_helper'

describe Desmond::BaseJob do
  def new_job(&block)
    clazz = Class.new(Desmond::BaseJob) do
      def self.name
        "DemondTestJob#{rand(4096)}"
      end

      self.instance_eval &block unless block.nil?
    end
    # create a global name for it, so we can run it async (worker needs to be able to find the class by name)
    Object.const_set(clazz.name, clazz)
    clazz
  end

  # changes working mode to async for async tests, use `async_worker` to run jobs
  def async
    prev_mode = Que.mode
    Que.mode = :off
    yield
  ensure
    Que.mode = prev_mode
  end

  # works on one job in the que in a separate thread
  def async_worker
    Thread.new do
      Que::Job.work
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

  it 'should have synchronous interface' do
    clazz = new_job do
      @test_counter = 0
      singleton_class.class_eval do
        attr_accessor :test_counter
      end

      define_method(:execute) do |job_id, user_id, options={}|
        self.class.test_counter += 1
        42
      end
    end
    expect(clazz.run(1, 1)).to eq(42)
    expect(clazz.test_counter).to eq(1)
  end

  it 'should throw exception on soft failure when using synchronous interface' do
    clazz = new_job do
      @test_counter = 0
      singleton_class.class_eval do
        attr_accessor :test_counter
      end

      define_method(:execute) do |job_id, user_id, options={}|
        self.class.test_counter += 1
        self.failed('Expected behavior')
      end
    end
    expect{ clazz.run(1, 1) }.to raise_error(Desmond::JobExecutionError, 'Expected behavior')
    expect(clazz.test_counter).to eq(1)
  end

  it 'should throw exception on failure when using synchronous interface' do
    clazz = new_job do
      @test_counter = 0
      singleton_class.class_eval do
        attr_accessor :test_counter
      end

      define_method(:execute) do |job_id, user_id, options={}|
        self.class.test_counter += 1
        fail 'Expected behavior'
      end
    end
    expect{ clazz.run(1, 1) }.to raise_error('Expected behavior')
    expect(clazz.test_counter).to eq(1)
  end

  it 'should throw exception when using synchronous interface and job raised exception' do
    class TestError < StandardError; end
    test_exception = TestError.new('Expected behavior')

    clazz = new_job do
      @test_counter = 0
      singleton_class.class_eval do
        attr_accessor :test_counter
      end

      define_method(:execute) do |job_id, user_id, options={}|
        self.class.test_counter += 1
        raise test_exception
      end
    end
    caught_exception = nil
    begin
      clazz.run(1, 1)
    rescue => e
      caught_exception = e
    end
    expect(caught_exception).to eq(test_exception)
    expect(clazz.test_counter).to eq(1)
  end

  it 'should run hooks in synchronous mode' do
    clazz = new_job do
      @test_counter = 0
      singleton_class.class_eval do
        attr_accessor :test_counter
      end

      define_method(:before) do |job_run, job_id, user_id, options={}|
        self.class.test_counter = 1 if self.class.test_counter == 0
      end

      define_method(:error) do |job_run, job_id, user_id, options={}|
        self.class.test_counter = 3 if self.class.test_counter == 2
      end

      define_method(:success) do |job_run, job_id, user_id, options={}|
        self.class.test_counter += 1 # shouldn't be executed
      end

      define_method(:after) do |job_run, job_id, user_id, options={}|
        self.class.test_counter = 4 if self.class.test_counter == 3
      end

      define_method(:execute) do |job_id, user_id, options={}|
        self.class.test_counter = 2 if self.class.test_counter == 1
        42
      end
    end
    expect(clazz.run(1, 1)).to eq(42)
    expect(clazz.test_counter).to eq(4)
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
        self.class.test_counter = 2 if self.class.test_counter == 1
      end

      define_method(:success) do |job_run, job_id, user_id, options={}|
        self.class.test_counter += 1 # shouldn't be executed
      end

      define_method(:after) do |job_run, job_id, user_id, options={}|
        self.class.test_counter = 3 if self.class.test_counter == 2
      end

      define_method(:execute) do |job_id, user_id, options={}|
        self.class.test_counter = 1 if self.class.test_counter == 0
        fail 'Expected behavior'
      end
    end
    expect(clazz.enqueue(1, 1).failed?).to eq(true)
    expect(clazz.test_counter).to eq(3)
  end

  it 'should run success & after hook' do
    clazz = new_job do
      @test_counter = 0
      singleton_class.class_eval do
        attr_accessor :test_counter
      end

      define_method(:error) do |job_run, job_id, user_id, options={}|
        self.class.test_counter += 1 # shouldn't be executed
      end

      define_method(:success) do |job_run, job_id, user_id, options={}|
        self.class.test_counter = 2 if self.class.test_counter == 1
      end

      define_method(:after) do |job_run, job_id, user_id, options={}|
        self.class.test_counter = 3 if self.class.test_counter == 2
      end

      define_method(:execute) do |job_id, user_id, options={}|
        self.class.test_counter = 1 if self.class.test_counter == 0
      end
    end
    expect(clazz.enqueue(1, 1).done?).to eq(true)
    expect(clazz.test_counter).to eq(3)
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

  it 'should save return value into job run' do
    clazz = new_job do
      define_method(:execute) do |job_id, user_id, options={}|
        { testdata: true, 'testdata2' => 42, testdata3: 42.42, testdata4: [1, 2] } # testing different types
      end
    end
    expect(clazz.enqueue(1, 1).result).to eq('testdata' => true, 'testdata2' => 42, 'testdata3' => 42.42, 'testdata4' => [1, 2])
  end

  it 'should refuse to save non-json data into job run' do
    clazz = new_job do
      define_method(:execute) do |job_id, user_id, options={}|
        Object.new
      end
    end
    run = clazz.enqueue(1, 1)
    expect(run.failed?).to eq(true)
    expect(run.error).to eq('Invalid result type')
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

  it 'should be able to wait for completion' do
    self.async do
      clazz = new_job do
        define_method(:execute) do |job_id, user_id, options={}|
          sleep(2)
        end
      end
      run = clazz.enqueue(1, 1)
      expect(run.finished?).to eq(false)
      self.async_worker
      run.wait_until_finished
      expect(run.done?).to eq(true)
    end
  end

  it 'should be able to timeout while waiting for completion' do
    self.async do
      clazz = new_job do
        define_method(:execute) do |job_id, user_id, options={}|
          sleep(3)
        end
      end
      run = clazz.enqueue(1, 1)
      expect(run.finished?).to eq(false)
      self.async_worker
      run.wait_until_finished(1)
      expect(run.finished?).to eq(false)
    end
  end

  it 'should pass options symbolized' do
    clazz = new_job do
      @options_before = nil
      @options_execute = nil
      @options_after = nil
      singleton_class.class_eval do
        attr_accessor :options_before, :options_execute, :options_after
      end

      define_method(:before) do |job_run, job_id, user_id, options={}|
        self.class.options_before = options
      end
      define_method(:execute) do |job_id, user_id, options={}|
        self.class.options_execute = options
      end
      define_method(:after) do |job_run, job_id, user_id, options={}|
        self.class.options_after = options
      end
    end
    expect(clazz.enqueue(1, 1, 'test1' => true, test2: true).done?).to eq(true)
    options_expected = { test1: true, test2: true }
    expect(clazz.options_before).to eq(options_expected)
    expect(clazz.options_execute).to eq(options_expected)
    expect(clazz.options_after).to eq(options_expected)
  end
end
