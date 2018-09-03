require 'spec_helper'

RSpec.describe DirtyPipeline::Event do
  subject(:event) do
    described_class.create('open', tx_id: transaction_id)
  end
  let(:transaction_id) { Nanoid.generate }

  before do
    Timecop.freeze
    @time_at_start = Time.now
  end
  after { Timecop.return }

  describe '#link_exception' do
    class TestError < StandardError; end

    before do
      begin
        raise TestError, "Something bad happened"
      rescue => ex
        event.link_exception(ex)
      end
    end

    it do
      expect(event.error).to match(
        "exception" => "TestError",
        "exception_message" => "Something bad happened",
        "created_at" => @time_at_start,
      )
      expect(event).to be_failure
    end
  end

  describe '#attempt_retry' do
    before { event.attempt_retry }

    it do
      expect(event.data.keys).to include(*%w(updated_at attempts_count))
      expect(event.attempts_count).to eq(2)
    end
  end

  describe '#to_h' do
    it do
      expect(event.to_h).to match(
        data: {
          "uuid" => event.id,
          "transaction_uuid" => transaction_id,
          "transition" => 'open',
          "args" => [],
          "created_at" => @time_at_start,
          "cache" => {},
          "attempts_count" => 1,
          "status" => "new",
        },
        error: nil
      )
    end
  end
end
