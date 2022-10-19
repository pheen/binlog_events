require_relative "binlog_events/version"

require "msgpack"
require "socket"

module BinlogEvents
  def self.listen(mysql_url)
    sock = TCPSocket.new("127.0.0.1", 23578)

    u = MessagePack::Unpacker.new(sock)

    u.each do |obj|
      changes = {}

      obj[3].each do |field_name, (before, after)|
        before = before.values.first
        after = after.values.first

        if before.is_a?(Hash)
          before = before.values.first
        end

        if after.is_a?(Hash)
          after = after.values.first
        end

        changes[field_name] = [before, after]
      end

      hash = {
        action: obj[0],
        table: obj[1],
        id: obj[2],
        changes: changes,
      }

      yield(hash)

      puts ""
    end
  end
end
