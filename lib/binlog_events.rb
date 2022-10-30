require_relative "binlog_events/version"

require "msgpack"
require "socket"

module BinlogEvents
  def self.listen(mysql_url, config = {}, &block)
    Callbacks.configure(&block)

    config[:models] ||= begin
      Rails.application.eager_load! if Rails.env.development?
      @rails_found = true

      ActiveRecord::Base.descendants.select do |model|
        !model.abstract_class? && model.table_name
      end
    rescue NameError
      @rails_found = false
    end

    if @rails_found
      # todo: Add a cache as large apps can have hundreds of models.
      table_to_model_map = {}

      config[:models].each do |model|
        if model.base_class?
          table_to_model_map[model.table_name] = model
        end
      end
    end

    sock = TCPSocket.new("127.0.0.1", 23578)

    # todo: write mysql url to sock
    event_stream = MessagePack::Unpacker.new(sock)

    # MessagePack automatically deserializes events as they arrive.
    #
    # {
    #   action: event[0],
    #   table: event[1],
    #   id: event[2],
    #   changes: changes,
    # }
    event_stream.each do |event|
      # todo: remove `record_id`
      action, table_name, attributes, raw_changes = *event
      changes = unwrap_changes(raw_changes)

      if @rails_found
        model = table_to_model_map[table_name]
        record = model.new(attributes)

        Callbacks
          .public_send("#{action}_callback")
          .call(record, changes)
      else
        Callbacks
          .public_send("#{action}_callback")
          .call(attributes, changes)
      end
    end
  end

  # Builds a hash mimicking Rails' `saved_changes`.
  def self.unwrap_changes(event_changes)
    changes = {}

    event_changes.each do |field_name, (before, after)|
      before = before&.values&.first
      after = after&.values&.first

      if before.is_a?(Hash)
        before = before.values.first
      end

      if after.is_a?(Hash)
        after = after.values.first
      end

      changes[field_name] = [before, after]
    end

    changes
  end

  def self.rails_found?
    @rails_found || false
  end

  class Callbacks
    class << self
      def configure(&block)
        block.call(self)
      end

      def create(&block)
        Thread.current[:binlog_events_create_callback] = block
      end

      def update(&block)
        Thread.current[:binlog_events_update_callback] = block
      end

      def delete(&block)
        Thread.current[:binlog_events_delete_callback] = block
      end

      def create_callback
        Thread.current[:binlog_events_create_callback]
      end

      def update_callback
        Thread.current[:binlog_events_update_callback]
      end

      def delete_callback
        Thread.current[:binlog_events_delete_callback]
      end
    end
  end
end
