RSpec.describe BinlogEvents do
  # it "has a version number" do
  #   expect(BinlogEvents::VERSION).not_to be nil
  # end

  it "reports events" do
    url = "mysql://root:root@127.0.0.1:1234/db_name"

    described_class.listen(url) do |event|
      pp event
    end
  end
end
