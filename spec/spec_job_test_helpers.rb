module JobTestHelpers
  # create a new subclass of +base+, +block+ can be used to define methods and stuff
  def new_job(base=Desmond::BaseJob, &block)
    clazz_name = "DemondTestJob#{rand(4096)}"
    clazz = Class.new(base) do
      define_method(:name) do
        clazz_name
      end

      self.instance_eval &block unless block.nil?
    end
    # create a global name for it, so we can run it async (worker needs to be able to find the class by name)
    Object.const_set(clazz_name, clazz)
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
  def async_worker(wait: false)
    t = Thread.new do
      Que::Job.work
    end
    t.join if wait
  end
end
