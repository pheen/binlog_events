# frozen_string_literal: true

RSpec.describe BinlogEvents do
  it "has a version number" do
    expect(BinlogEvents::VERSION).not_to be nil
  end
end
