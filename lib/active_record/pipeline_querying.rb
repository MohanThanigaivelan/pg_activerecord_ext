# frozen_string_literal: true

module ActiveRecord
  module Querying

    def _query_by_sql(sql, binds = [], preparable: nil, pipeline_async: false) # :nodoc:
      connection.select_all(sanitize_sql(sql), "#{name} Load", binds, preparable: preparable, pipeline_async: pipeline_async)
    end

    def _load_from_sql(result_set, &block) # :nodoc:
      column_types = result_set.column_types

      unless column_types.empty?
        column_types = column_types.reject { |k, _| attribute_types.key?(k) }
      end

      message_bus = ActiveSupport::Notifications.instrumenter

      payload = {
        record_count: result_set.length,
        class_name: name
      }

      message_bus.instrument("instantiation.active_record", payload) do
        if result_set.includes_column?(inheritance_column)
          result_set.map { |record| instantiate(record, column_types, &block) }
        else
          # Instantiate a homogeneous set
          result_set.map { |record| instantiate_instance_of(self, record, column_types, &block) }
        end
      end
    end

  end
end
