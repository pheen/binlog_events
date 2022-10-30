RSpec.describe BinlogEvents do
  # it "has a version number" do
  #   expect(BinlogEvents::VERSION).not_to be nil
  # end

  it "reports events" do
    url = "mysql://root:root@127.0.0.1:1234/db_name"

    BinlogEvents.listen(url) do |on|
      on.create do |record, changes|
        p "create"
        p "record: #{record}"
        p "changes: #{changes}"
      end

      on.update do |record, changes|
        p "update"
        p "record: #{record}"
        p "changes: #{changes}"
      end

      on.delete do |record, changes|
        p "delete"
        p "record: #{record}"
        p "changes: #{changes}"
      end
    end
  end
end
