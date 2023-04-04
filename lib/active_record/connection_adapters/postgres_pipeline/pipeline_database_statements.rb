module ActiveRecord
  module ConnectionAdapters
    module PostgresPipeline
      module DatabaseStatements

        def execute_batch(statements, name = nil)
          statements.each do |statement|
            execute(statement, name)
          end
        end

        # Executes an SQL statement, returning a PG::Result object on success
        # or raising a PG::Error exception otherwise.
        # Note: the PG::Result object is manually memory managed; if you don't
        # need it specifically, you may want consider the <tt>exec_query</tt> wrapper.
        def execute(sql, name = nil)
          if preventing_writes? && write_query?(sql)
            raise ActiveRecord::ReadOnlyError, "Write query attempted while in readonly mode: #{sql}"
          end

          materialize_transactions
          mark_transaction_written_if_write(sql)

          log(is_pipeline_mode? ? "#{sql} [SYNC]" : sql, name) do
            ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
              if is_pipeline_mode?
                flush_pipeline_and_get_sync_result { @connection.send_query_params(sql, []) }
              else
                @connection.async_exec(sql)
              end
            end
          end
        end

        def query(sql, name = nil) #:nodoc:
          materialize_transactions
          mark_transaction_written_if_write(sql)

          log(is_pipeline_mode? ? "#{sql} [SYNC]" : sql, name) do
            ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
              if is_pipeline_mode?
                result = flush_pipeline_and_get_sync_result { @connection.send_query_params(sql, []) }
                result.map_types!(@type_map_for_results).values
              else
                @connection.async_exec(sql).map_types!(@type_map_for_results).values
              end
            end
          end
        end
        def select_all(arel, name = nil, binds = [], preparable: nil, pipeline_async: false)
          arel = arel_from_relation(arel)
          sql, binds, preparable = to_sql_and_binds(arel, binds, preparable)

          if prepared_statements && preparable
            select_prepared(sql, name, binds, pipeline_async: pipeline_async)
          else
            select(sql, name, binds, pipeline_async: pipeline_async)
          end
        rescue ::RangeError
          ActiveRecord::Result.new([], [])
        end

        def select(sql, name = nil, binds = [], pipeline_async: false)
          exec_query(sql, name, binds, prepare: false, pipeline_async: pipeline_async)
        end

        def select_prepared(sql, name = nil, binds = [], pipeline_async: false)
          exec_query(sql, name, binds, prepare: true, pipeline_async: pipeline_async)
        end

        def exec_delete(sql, name = nil, binds = [])
          execute_and_clear(sql, name, binds) do |result|
            result.cmd_tuples
          end
        end
        alias :exec_update :exec_delete

      end
    end
  end
end