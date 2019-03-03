require 'spec_helper'

RSpec.describe DirtyPipeline::Storage do
  subject(:storage) do
    described_class.new(mail, :tasks_store)
  end
  let(:mail) { Mail.new.tap(&:save) }
  let(:task) do
    DirtyPipeline::Task.create("open", tx_id: transaction_id)
  end
  let(:transaction_id) { SecureRandom.uuid }

  before do
    Timecop.freeze
    @time_at_start = Time.now
  end
  after { Timecop.return }

  context 'when storage is pristine' do
    it do
      expect(storage.to_h).to include(
        "status" => nil,
        "state" => {},
      )
    end
  end

  context 'when task was committed' do
    def task_data_from_db(mail_id, task_id)
      DB[:mails].dig(mail_id, :tasks_store, "tasks", task_id)
    end

    def task_error_from_db(mail_id, task_id)
      DB[:mails].dig(mail_id, :tasks_store, "errors", task_id)
    end

    context 'when new task' do
      it do
        storage.commit!(task)

        expect(storage.status).to be_nil
        expect(storage.to_h["state"]).to be_empty
        expect(storage.find_task(task.id).data).to eq(task.data)
        expect(storage.find_task(task.id).error).to be_empty
      end
    end

    context 'when finished task' do
      before do
        task.assign_changes({"read_at" => Time.now.utc.iso8601})
        task.destination = "open"
        task.complete
      end

      it do
        storage.commit!(task)

        expect(storage.status).to eq("open")
        expect(storage.to_h["state"]).to(
          match("read_at" => Time.now.utc.iso8601)
        )
        expect(storage.find_task(task.id).data).to eq(task.data)
        expect(storage.find_task(task.id).error).to be_empty
      end
    end

    context 'when failed task' do
      before do
        begin
          raise TestError, "Something bad happened"
        rescue => ex
          task.link_exception(ex)
        end
      end

      it do
        storage.commit!(task)

        expect(storage.status).to be_nil
        expect(storage.to_h["state"]).to be_empty
        expect(storage.find_task(task.id).error).to eq(task.error)
        expect(storage.find_task(task.id).data).to eq(task.data)
      end
    end
  end
end
