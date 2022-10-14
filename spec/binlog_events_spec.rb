# frozen_string_literal: true

RSpec.describe BinlogEvents do
  # it "has a version number" do
  #   expect(BinlogEvents::VERSION).not_to be nil
  # end

  it "does things" do
    url = "mysql://root:root@127.0.0.1:3306/themis_development_1"

    # BinlogEventsInterface.listen(url) do |table_name, column_name, value|
    described_class.listen(url) do |change|
      puts change
    end
  end
end
