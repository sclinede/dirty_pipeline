require 'spec_helper'

RSpec.describe DirtyPipeline::Queue do
  subject(:queue) do
    described_class.new("call", "Mail", 13, transaction_id)
  end
  let(:transaction_id) { SecureRandom.uuid }
  let(:event1) { DirtyPipeline::Event.create("open", tx_id: transaction_id) }
  let(:event2) { DirtyPipeline::Event.create("read", tx_id: transaction_id) }

  before { queue.clear! }

  context 'when nothing in the queue' do
    it { expect(queue.to_a).to eq([]) }
  end

  context 'when some events pushed' do
    before { subject << event1 << event2 }

    context 'when nothing is in processing' do
      it do
        expect(queue.to_a.size).to eq(2)
        expect(queue.to_a.first.id).to eq(event1.id)
        expect(queue.to_a.last.id).to eq(event2.id)
        expect(queue.processing_event).to be_nil
      end
    end

    context 'when event is in processing' do
      it do
        expect(queue.pop.id).to eq(event1.id)
        expect(queue.to_a.size).to eq(1)
        expect(queue.processing_event.id).to eq(event1.id)
      end
    end
  end

  context 'when some events unshifted' do
    before { subject.unshift(event1).unshift(event2) }

    context 'when nothing is in processing' do
      it do
        expect(queue.to_a.size).to eq(2)
        expect(queue.to_a.first.id).to eq(event2.id)
        expect(queue.to_a.last.id).to eq(event1.id)
        expect(queue.processing_event).to be_nil
      end
    end

    context 'when event is in processing' do
      it do
        expect(queue.pop.id).to eq(event2.id)
        expect(queue.to_a.size).to eq(1)
        expect(queue.processing_event.id).to eq(event2.id)
      end
    end
  end
end
