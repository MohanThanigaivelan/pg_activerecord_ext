module ActiveRecord
  module ConnectionAdapters # :nodoc:
    module QueryCache
      def select_all(arel, name = nil, binds = [], preparable: nil, pipeline_async: false)
        if @query_cache_enabled && !locked?(arel)
          arel = arel_from_relation(arel)
          sql, binds, preparable = to_sql_and_binds(arel, binds, preparable)

          cache_sql(sql, name, binds) { super(sql, name, binds, preparable: preparable, pipeline_async: pipeline_async) }
        else
          super
        end
      end
    end
  end
end
