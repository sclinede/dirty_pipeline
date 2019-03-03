require 'spec_helper'

RSpec.describe DirtyPipeline::Task do
  subject(:task) do
    described_class.create('open', tx_id: transaction_id)
  end
  let(:transaction_id) { SecureRandom.uuid }

  before do
    Timecop.freeze
    @time_at_start = Time.now.utc.iso8601
  end
  after { Timecop.return }

  describe '#link_exception' do
    class TestError < StandardError; end

    before do
      begin
        raise TestError, "Something bad happened"
      rescue => ex
        task.link_exception(ex)
      end
    end

    it do
      expect(task.error).to match(
        "exception" => "TestError",
        "exception_message" => "Something bad happened",
        "created_at" => @time_at_start,
      )
      expect(task).to be_failure
    end
  end

  describe '#attempt_retry' do
    before { task.attempt_retry! }

    it do
      expect(task.data.keys).to include(*%w(updated_at attempts_count))
      expect(task.attempts_count).to eq(2)
    end
  end

  describe '#to_h' do
    it do
      expect(task.to_h).to match(
        data: {
          "uuid" => task.id,
          "transaction_uuid" => transaction_id,
          "transition" => 'open',
          "args" => [],
          "created_at" => @time_at_start,
          "cache" => {},
          "attempts_count" => 1,
          "status" => "new",
          "try_next" => false,
        },
        error: nil
      )
    end
  end
end
