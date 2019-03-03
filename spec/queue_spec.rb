require 'spec_helper'

RSpec.describe DirtyPipeline::Queue do
  subject(:queue) do
    described_class.new("call", "Mail", 13, transaction_id)
  end
  let(:transaction_id) { SecureRandom.uuid }
  let(:task1) { DirtyPipeline::Task.create("open", tx_id: transaction_id) }
  let(:task2) { DirtyPipeline::Task.create("read", tx_id: transaction_id) }

  before { queue.clear! }

  context 'when nothing in the queue' do
    it { expect(queue.to_a).to eq([]) }
  end

  context 'when some tasks pushed' do
    before { subject << task1 << task2 }

    context 'when nothing is in processing' do
      it do
        expect(queue.to_a.size).to eq(2)
        expect(queue.to_a.first.id).to eq(task1.id)
        expect(queue.to_a.last.id).to eq(task2.id)
        expect(queue.processing_task).to be_nil
      end
    end

    context 'when task is in processing' do
      it do
        expect(queue.pop.id).to eq(task1.id)
        expect(queue.to_a.size).to eq(1)
        expect(queue.processing_task.id).to eq(task1.id)
      end
    end
  end

  context 'when some tasks unshifted' do
    before { subject.unshift(task1).unshift(task2) }

    context 'when nothing is in processing' do
      it do
        expect(queue.to_a.size).to eq(2)
        expect(queue.to_a.first.id).to eq(task2.id)
        expect(queue.to_a.last.id).to eq(task1.id)
        expect(queue.processing_task).to be_nil
      end
    end

    context 'when task is in processing' do
      it do
        expect(queue.pop.id).to eq(task2.id)
        expect(queue.to_a.size).to eq(1)
        expect(queue.processing_task.id).to eq(task2.id)
      end
    end
  end
end
