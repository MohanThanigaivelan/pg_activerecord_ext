# frozen_string_literal: true

module ActiveRecord
  # = Active Record \Relation
  class Relation

    def initialize(klass, table: klass.arel_table, predicate_builder: klass.predicate_builder, values: {})
      @klass  = klass
      @table  = table
      @values = values
      @loaded = false
      @predicate_builder = predicate_builder
      @future_result = nil
      @delegate_to_klass = false
    end
    def scheduled?
      !!@future_result
    end

    def load(&block)
      if !loaded? || scheduled?
        @records = exec_queries(&block)
        @loaded = true
      end

      self
    end

    def load_in_pipeline
      return load if !connection.is_pipeline_mode?
      unless loaded?

        result = exec_main_query(async: true)

        if result.class == Array
          @records = result
        else
          @future_result = result
        end
        @loaded = true
      end
      self
    end

    def exec_queries(&block)

      skip_query_cache_if_necessary do
        rows = if scheduled?
                 future = @future_result
                 @future_result = nil
                 future.result
               else
                 exec_main_query
               end

        records = instantiate_records(rows, &block)
        preload_associations(records) unless skip_preloading_value

        records.each(&:readonly!) if readonly_value
        records.each(&:strict_loading!) if strict_loading_value

        records
      end
    end

    # Returns size of the records.
    def size
      loaded? ? records.length : count(:all)
    end

    # Returns true if there are no records.
    def empty?
      return records.empty? if loaded?
      !exists?
    end

    private

    def exec_main_query(async: false)
      skip_query_cache_if_necessary do
        if where_clause.contradiction?
          []
        elsif eager_loading?
          apply_join_dependency do |relation, join_dependency|
            if relation.null_relation?
              []
            else
              relation = join_dependency.apply_column_aliases(relation)
              @_join_dependency = join_dependency
              connection.select_all(relation.arel, "SQL", async: async)
            end
          end
        else
          klass._query_by_sql(arel, async: async)
        end
      end
    end

    def instantiate_records(rows, &block)
      return [].freeze if rows.empty?
      if eager_loading?
        records = @_join_dependency.instantiate(rows, strict_loading_value, &block).freeze
        @_join_dependency = nil
        records
      else
        klass._load_from_sql(rows, &block).freeze
      end
    end
  end
end