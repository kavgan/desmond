##
# use this to censor log messages, so that password and other secrets
# are not shown in log files.
#
# recursively iterates through +h+, replacing all
# keys passed through +keys+ with '***censored***'.
#
def censor_hash_keys(h, keys=[])
  nh = {}
  h.each do |key, value|
    if h[key].is_a?(Hash)
      nh[key] = censor_hash_keys(h[key], keys)
    elsif keys.include?(key)
      nh[key] = '***censored***'
    else
      nh[key] = h[key]
    end
  end
  nh
end
