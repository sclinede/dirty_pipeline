require 'spec_helper'

RSpec.describe DirtyPipeline::Status do
  context 'when status is successful' do
    let(:status) { described_class.success("data") }

    it do
      expect(status).to be_success
      expect(status.tag).to eq(:success)
      expect(status.data).to eq("data")
    end
  end

  context 'when status is failure' do
    context 'when default tag' do
      let(:status) { described_class.failure(ArgumentError.new) }

      it do
        expect(status).to be_failure
        expect(status.tag).to eq(:exception)
        expect(status.data).to be_kind_of(ArgumentError)
      end
    end

    context 'when custom tag is provided' do
      let(:status) do
        described_class.failure(
          {error: 'Validation failed'},
          tag: :validation_error
        )
      end

      it do
        expect(status).to be_failure
        expect(status.tag).to eq(:validation_error)
        expect(status.data[:error]).to eq('Validation failed')
      end
    end
  end
end
