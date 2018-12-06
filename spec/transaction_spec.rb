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
    @event = nil
  end

  def status(mail_id)
    DB[:mails].dig(mail_id, :events_store, "status")
  end

  def state(mail_id)
    DB[:mails].dig(mail_id, :events_store, "state")
  end

  def event_data(mail_id, event_id)
    storage.find_event(event_id).data
  end

  def event_error(mail_id, event_id)
    storage.find_event(event_id).error
  end

  context 'when default transaction' do
    context 'when successful transaction' do
      before do
        pipeline.chain('Receive')
        @event = pipeline.railway.next
        described_class.new(pipeline, @event).call do |destination, *args|
          @event.assign_changes({"received_at" => Time.now.utc.iso8601})
          @event.complete
        end
      end

      it do
        expect(status(mail.id)).to eq "new"
        expect(state(mail.id)).to match("received_at" => Time.now.utc.iso8601)
        expect(@event).to be_success
        expect(event_data(mail.id, @event.id)).to eq(@event.data)
        expect(event_error(mail.id, @event.id)).to eq(@event.error.to_h)
      end
    end

    context 'when transaction aborted' do
      before do
        pipeline.chain('Receive')
        @event = pipeline.railway.next
        described_class.new(pipeline, @event).call do
          throw :abort_transaction, true
        end
      end

      it do
        expect(status(mail.id)).to be_nil
        expect(state(mail.id)).to be_empty
        expect(@event).to be_abort
        expect(event_data(mail.id, @event.id)).to eq(@event.data)
        expect(event_error(mail.id, @event.id)).to be_empty
      end
    end

    context 'when exception raised' do
      before do
        pipeline.chain('Receive')
        @event = pipeline.railway.next
        begin
          described_class.new(pipeline, @event).call do
            raise "No. This never happens"
          end
        rescue => ex
          @rescued_exception = ex
        end
      end

      it do
        expect(status(mail.id)).to be_nil
        expect(state(mail.id)).to be_empty
        expect(@event).to be_failure
        expect(@event.error).to match(
          "exception" => @rescued_exception.class.to_s,
          "exception_message" => @rescued_exception.message,
          "created_at" => @time_at_start,
        )
        expect(event_data(mail.id, @event.id)).to eq(@event.data)
        expect(event_error(mail.id, @event.id)).to eq(@event.error)
      end
    end
  end
end
