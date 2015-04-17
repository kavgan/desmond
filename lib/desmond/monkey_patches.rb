class String
  def max_substr_count(substrs, &block)
    substrs = [substrs] if substrs.is_a?(String)
    substrs.max_by do |substr|
      if block.present?
        block.call(self, substr)
      else
        self.scan(substr).size
      end
    end
  end
end
