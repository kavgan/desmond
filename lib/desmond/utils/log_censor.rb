##
# use this to censor log messages, so that password and other secrets
# are not shown in log files.
#
# recursively iterates through +h+, replacing all
# keys passed through +keys+ with +censorvalue+.
#
def censor_hash_keys(h, keys=[], censorvalue='***censored***')
  keys ||= []
  nh = {}
  h.each do |key, _|
    if h[key].is_a?(Hash)
      nh[key] = censor_hash_keys(h[key], keys, censorvalue)
    elsif keys.include?(key)
      nh[key] = censorvalue
    elsif key.is_a?(String) && keys.include?(key.to_sym)
      nh[key] = censorvalue
    elsif key.is_a?(Symbol) && keys.include?(key.to_s)
      nh[key] = censorvalue
    else
      nh[key] = h[key]
    end
  end
  nh
end
