require 'spec_helper'

RSpec.describe DirtyPipeline::Railway do
  subject(:railway) do
    described_class.new(mail, transaction_id)
  end
  let(:mail) { Mail.new.tap(&:save) }
  let(:transaction_id) { SecureRandom.uuid }
  let(:event_read) do
    DirtyPipeline::Event.create("read", tx_id: transaction_id)
  end
  let(:event_unread) do
    DirtyPipeline::Event.create("unread", tx_id: transaction_id)
  end
  let(:event_notify) do
    DirtyPipeline::Event.create("notify", tx_id: transaction_id)
  end

  before { railway.clear! }

  context 'when nothing enqueued' do
    it do
      expect(railway[:call].to_a).to eq([])
      expect(railway[:undo].to_a).to eq([])
      expect(railway[:finalize].to_a).to eq([])
    end
  end

  context 'when some events pushed' do
    before do
      railway[:call] << event_read
      railway.switch_to(:call)
    end

    context 'when everything is successful' do
      it do
        expect(railway.running_transaction).to be_nil

        expect(railway.next.id).to eq(event_read.id)
        expect(railway.running_transaction).to eq(transaction_id)
        railway.switch_to(:finalize) # on Success
        railway[:undo] << event_unread
        railway[:finalize].unshift(event_notify)

        expect(railway.queue.to_a).not_to be_empty
        expect(railway[:call].to_a).to be_empty
        expect(railway[:undo].to_a).not_to be_empty

        expect(railway.next.id).to eq(event_notify.id)
        expect(railway.next).to be_nil
        expect(railway.running_transaction).to be_nil
      end
    end

    context 'when call was failed' do
      it do
        expect(railway.running_transaction).to be_nil

        expect(railway.next.id).to eq(event_read.id)
        expect(railway.running_transaction).to eq(transaction_id)
        railway.switch_to(:undo) # on Failure
        railway[:undo] << event_unread
        railway[:finalize].unshift(event_notify)

        expect(railway.queue.to_a).not_to be_empty
        expect(railway[:call].to_a).to be_empty
        expect(railway[:finalize].to_a).not_to be_empty

        expect(railway.next.id).to eq(event_unread.id)
        expect(railway.next).to be_nil
        expect(railway.running_transaction).to be_nil
      end
    end
  end
end
