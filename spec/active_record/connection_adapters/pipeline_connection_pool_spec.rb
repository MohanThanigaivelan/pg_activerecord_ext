require 'active_record'
require 'pg_activerecord_ext'

RSpec.describe ActiveRecord::ConnectionAdapters::PipelineConnectionPool do
  context '#checkin' do
    it 'should discard_result before checking in connection for postgres_pipeline adapter' do
      ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
      pipeline_connection = ActiveRecord::Base.connection
      pool = pipeline_connection.instance_variable_get(:@pool)
      expect(pipeline_connection).to receive(:discard_results).and_call_original
      pool.checkin(pipeline_connection)
    end
  end
end