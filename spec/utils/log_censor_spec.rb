require_relative '../spec_helper'

describe 'censor_hash_keys' do
  let (:censorval) { '***censor***' }

  it 'should not change hash by default' do
    h = { a: 1 }
    expect(censor_hash_keys(h)).to eq(h)
  end

  it 'should not change hash with nil censor array' do
    h = { a: 1 }
    expect(censor_hash_keys(h, nil)).to eq(h)
  end

  it 'should not change hash with empty censor array' do
    h = { a: 1 }
    expect(censor_hash_keys(h, [])).to eq(h)
  end

  it 'should censor given key' do
    h = { a: 1 }
    h_censored = { a: censorval }
    expect(censor_hash_keys(h, [:a], censorval)).to eq(h_censored)
  end

  it 'should censor recursivly' do
    h = { a: { b: censorval } }
    h_censored = { a: { b: censorval } }
    expect(censor_hash_keys(h, [:b], censorval)).to eq(h_censored)
  end
end
