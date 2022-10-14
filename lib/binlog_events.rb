require_relative "binlog_events/version"
require "socket"

module BinlogEvents
  def self.listen(mysql_url)
    sock = TCPSocket.new("127.0.0.1", 23578)

    while line = sock.gets
      puts line
    end
  end
end
