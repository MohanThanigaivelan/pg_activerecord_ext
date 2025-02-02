# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module PostgresPipeline
      module ReferentialIntegrity # :nodoc:
        def disable_referential_integrity # :nodoc:
          original_exception = nil

          begin
            transaction(requires_new: true) do
              tables.each do |name|
                execute("ALTER TABLE #{quote_table_name(name)} DISABLE TRIGGER ALL")
              end

              # execute(tables.collect { |name| "ALTER TABLE #{quote_table_name(name)} DISABLE TRIGGER ALL" }.join(";"))
            end
          rescue ActiveRecord::ActiveRecordError => e
            original_exception = e
          end

          begin
            yield
          rescue ActiveRecord::InvalidForeignKey => e
            warn <<-WARNING
WARNING: Rails was not able to disable referential integrity.

This is most likely caused due to missing permissions.
Rails needs superuser privileges to disable referential integrity.

    cause: #{original_exception&.message}

            WARNING
            raise e
          end

          begin
            transaction(requires_new: true) do
              tables.each do |name|
                execute("ALTER TABLE #{quote_table_name(name)} ENABLE TRIGGER ALL")
              end
              # execute(tables.collect { |name| "ALTER TABLE #{quote_table_name(name)} ENABLE TRIGGER ALL" }.join(";"))
            end
          rescue ActiveRecord::ActiveRecordError => e
            @logger.error("While enabling referential integrity recieved error #{e}")
          end
        end
      end
    end
  end
end