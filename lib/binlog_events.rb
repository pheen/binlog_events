# frozen_string_literal: true

require "rutie"
require_relative "binlog_events/version"

module BinlogEvents
  Rutie.new(:binlog_events).init("Init_binlog_events", __dir__)
end
