module ActiveRecord
  module ConnectionAdapters
    module PipelineConnectionPool
      def checkin(conn)
        conn.discard_results rescue nil if conn.adapter_name == "PostgresPipeline"
        super
      end
    end
  end
end

ActiveRecord::ConnectionAdapters::ConnectionPool.prepend(ActiveRecord::ConnectionAdapters::PipelineConnectionPool)