require 'spec_helper'

RSpec.describe DirtyPipeline::Base do
  before { mail.pipeline.clear! }
  before { Timecop.freeze }
  after  { Timecop.return }

  context "when Golden Flow" do
    let(:mail) { Mail.new.tap(&:save) }

    it "successfuly runs pipeline" do
      Timecop.freeze
      status = mail.pipeline.chain(:Receive)
                            .chain(:Open)
                            .chain(:Unread)
                            .chain(:Delete)
                            .call
                            .status
      expect(status).to be_success
      expect(mail.events_store["status"]).to eq("deleted")
      expect(mail.events_store["state"]).to match(
        "received_at" => Time.now.utc.iso8601,
        "read_at" => nil, # after the Unread
        "deleted_at" => Time.now.utc.iso8601
      )
    end
  end

  context "when undoing last action" do

    let(:mail) do
      Mail.new
          .tap { |mail| mail.body = "No, God, please, Noooo " * 25 }
          .tap(&:save)
    end

    it "successfuly failover pipeline" do
      Timecop.freeze
      status = mail.pipeline.chain(:Receive)
                            .chain(:Open)
                            .chain(:Unread)
                            .chain(:Delete)
                            .call
                            .status
      expect(status).to be_failure
      expect(mail.events_store["status"]).to be_nil
      expect(mail.events_store["state"]).to match(
        "received_at" => nil,
        "read_at" => nil,
      )
    end
  end

  context "when undoing first action" do
    let(:mail) do
      Mail.new
          .tap { |mail| mail.body = "No, God, please, Noooo " * 125 }
          .tap(&:save)
    end

    it "successfuly failover pipeline" do
      Timecop.freeze
      status = mail.pipeline.chain(:Receive)
                            .chain(:Open)
                            .chain(:Unread)
                            .chain(:Delete)
                            .call
                            .status
      expect(status).to be_failure
      expect(mail.events_store["status"]).to be_nil
      expect(mail.events_store["state"]).to match(
        "received_at" => nil,
      )
    end
  end
end
