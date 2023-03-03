# frozen_string_literal: true
module ActiveRecord
  class Migration
    module Compatibility
      class V5_1 < V5_2
        def change_column(table_name, column_name, type, **options)
          if ["PostgreSQL" , "PostgresPipeline" ].include?(connection.adapter_name)
            super(table_name, column_name, type, **options.except(:default, :null, :comment))
            connection.change_column_default(table_name, column_name, options[:default]) if options.key?(:default)
            connection.change_column_null(table_name, column_name, options[:null], options[:default]) if options.key?(:null)
            connection.change_column_comment(table_name, column_name, options[:comment]) if options.key?(:comment)
          else
            super
          end
        end
      end

      class V5_0 < V5_1

        def create_table(table_name, **options)
          if ["PostgreSQL" , "PostgresPipeline"].include?(connection.adapter_name)
            if options[:id] == :uuid && !options.key?(:default)
              options[:default] = "uuid_generate_v4()"
            end
          end

          unless connection.adapter_name == "Mysql2" && options[:id] == :bigint
            if [:integer, :bigint].include?(options[:id]) && !options.key?(:default)
              options[:default] = nil
            end
          end

          # Since 5.1 PostgreSQL adapter uses bigserial type for primary
          # keys by default and MySQL uses bigint. This compat layer makes old migrations utilize
          # serial/int type instead -- the way it used to work before 5.1.
          unless options.key?(:id)
            options[:id] = :integer
          end

          super
        end
      end
    end
  end
end