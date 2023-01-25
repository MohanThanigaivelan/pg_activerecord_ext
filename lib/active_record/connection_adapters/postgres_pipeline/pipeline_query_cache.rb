module ActiveRecord
  module ConnectionAdapters # :nodoc:
    module QueryCache
      def select_all(arel, name = nil, binds = [], preparable: nil, async: false)
        if @query_cache_enabled && !(arel.respond_to?(:locked) && arel.locked)
          sql, binds, preparable = to_sql_and_binds(arel, binds, preparable)

          if async
            lookup_sql_cache(sql, name, binds) || super(sql, name, binds, preparable: preparable)
          else
            cache_sql(sql, name, binds) { super(sql, name, binds, preparable: preparable) }
          end
        else
          super(arel, name = nil, binds = [], preparable: preparable)
        end
      end

      private

      def lookup_sql_cache(sql, name, binds)
        @lock.synchronize do
          if @query_cache[sql].key?(binds)
            ActiveSupport::Notifications.instrument(
              "sql.active_record",
              cache_notification_info(sql, name, binds)
            )
            @query_cache[sql][binds]
          end
        end
      end
    end
  end
end