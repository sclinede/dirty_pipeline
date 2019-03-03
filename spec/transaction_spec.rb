require 'spec_helper'

RSpec.describe DirtyPipeline::Transaction do
  let(:mail) { Mail.new.tap(&:save) }
  let(:pipeline) { MailPipeline.new(mail) }
  let(:storage) { pipeline.storage }

  before do
    Timecop.freeze
    @time_at_start = Time.now.utc.iso8601
  end
  after do
    pipeline.railway.next # finish transaction
    Timecop.return
    @time_at_start = nil
    @task = nil
  end

  def status(mail_id)
    DB[:mails].dig(mail_id, :tasks_store, "status")
  end

  def state(mail_id)
    DB[:mails].dig(mail_id, :tasks_store, "state")
  end

  def task_data(mail_id, task_id)
    storage.find_task(task_id).data
  end

  def task_error(mail_id, task_id)
    storage.find_task(task_id).error
  end

  context 'when default transaction' do
    context 'when successful transaction' do
      before do
        pipeline.chain('Receive')
        @task = pipeline.railway.next
        described_class.new(pipeline, @task).call do |destination, *args|
          @task.assign_changes({"received_at" => Time.now.utc.iso8601})
          @task.complete
        end
      end

      it do
        expect(status(mail.id)).to eq "new"
        expect(state(mail.id)).to match("received_at" => Time.now.utc.iso8601)
        expect(@task).to be_success
        expect(task_data(mail.id, @task.id)).to eq(@task.data)
        expect(task_error(mail.id, @task.id)).to eq(@task.error.to_h)
      end
    end

    context 'when transaction aborted' do
      before do
        pipeline.chain('Receive')
        @task = pipeline.railway.next
        described_class.new(pipeline, @task).call do
          throw :abort_transaction, true
        end
      end

      it do
        expect(status(mail.id)).to be_nil
        expect(state(mail.id)).to be_empty
        expect(@task).to be_abort
        expect(task_data(mail.id, @task.id)).to eq(@task.data)
        expect(task_error(mail.id, @task.id)).to be_empty
      end
    end

    context 'when exception raised' do
      before do
        pipeline.chain('Receive')
        @task = pipeline.railway.next
        begin
          described_class.new(pipeline, @task).call do
            raise "No. This never happens"
          end
        rescue => ex
          @rescued_exception = ex
        end
      end

      it do
        expect(status(mail.id)).to be_nil
        expect(state(mail.id)).to be_empty
        expect(@task).to be_failure
        expect(@task.error).to match(
          "exception" => @rescued_exception.class.to_s,
          "exception_message" => @rescued_exception.message,
          "created_at" => @time_at_start,
        )
        expect(task_data(mail.id, @task.id)).to eq(@task.data)
        expect(task_error(mail.id, @task.id)).to eq(@task.error)
      end
    end
  end
end
