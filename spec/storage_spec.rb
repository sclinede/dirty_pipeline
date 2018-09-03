require 'spec_helper'

RSpec.describe DirtyPipeline::Storage do
  subject(:storage) do
    described_class.new(mail, :events_store)
  end
  let(:mail) { Mail.new.tap(&:save) }
  let(:event) do
    DirtyPipeline::Event.create("open", tx_id: transaction_id)
  end
  let(:transaction_id) { Nanoid.generate }

  before do
    Timecop.freeze
    @time_at_start = Time.now
  end
  after { Timecop.return }

  context 'when storage is pristine' do
    it do
      expect(storage.to_h).to match(
        "status" => nil,
        "state" => {},
        "events" => {},
        "errors" => {}
      )
    end
  end

  context 'when event was committed' do
    def event_data_from_db(mail_id, event_id)
      DB[:mails].dig(mail_id, :events_store, "events", event_id)
    end

    def event_error_from_db(mail_id, event_id)
      DB[:mails].dig(mail_id, :events_store, "errors", event_id)
    end

    context 'when new event' do
      it do
        storage.commit!(event)

        expect(storage.status).to be_nil
        expect(storage.to_h["state"]).to be_empty
        expect(storage.to_h["errors"]).to be_empty
        expect(storage.to_h.dig("events", event.id)).to eq(event.data)
        expect(event_error_from_db(mail.id, event.id)).to eq(event.error)
        expect(event_data_from_db(mail.id, event.id)).to eq(event.data)
      end
    end

    context 'when finished event' do
      before { event.complete({"read_at" => Time.now}, "open") }

      it do
        storage.commit!(event)

        expect(storage.status).to eq("open")
        expect(storage.to_h["state"]).to match("read_at" => Time.now)
        expect(storage.to_h["errors"]).to be_empty
        expect(storage.to_h.dig("events", event.id)).to eq(event.data)
        expect(event_error_from_db(mail.id, event.id)).to eq(event.error)
        expect(event_data_from_db(mail.id, event.id)).to eq(event.data)
      end
    end

    context 'when failed event' do
      before do
        begin
          raise TestError, "Something bad happened"
        rescue => ex
          event.link_exception(ex)
        end
      end

      it do
        storage.commit!(event)

        expect(storage.status).to be_nil
        expect(storage.to_h["state"]).to be_empty
        expect(storage.to_h.dig("errors", event.id)).to eq(event.error)
        expect(storage.to_h.dig("events", event.id)).to eq(event.data)
        expect(event_error_from_db(mail.id, event.id)).to eq(event.error)
        expect(event_data_from_db(mail.id, event.id)).to eq(event.data)
      end
    end
  end
end
