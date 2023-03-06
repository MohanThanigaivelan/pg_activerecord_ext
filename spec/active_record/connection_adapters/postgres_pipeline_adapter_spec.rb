# frozen_string_literal: true
require 'spec_helper'
require 'pg_activerecord_ext'
require 'active_record/pipeline_errors'

require 'active_record/pipeline_future_result'

RSpec.describe 'ActiveRecord::ConnectionAdapters::PostgresPipelineAdapter' do
  before(:all) do
    ActiveRecord::Base.logger = ActiveSupport::Logger.new(STDOUT)
    @pipeline_connection = ActiveRecord::Base.postgres_pipeline_connection(min_messages: 'warning')
    @connection = ActiveRecord::Base.postgresql_connection(min_messages: 'warning')
    @connection.exec_query('CREATE TABLE IF NOT EXISTS postgresql_pipeline_test_table (
      id SERIAL PRIMARY KEY,
      number integer
      );')
    @connection.exec_insert('insert into postgresql_pipeline_test_table (number) VALUES (1)', nil, [], 'id', 'postgresql_pipeline_test_table_id_seq')
    @connection.exec_insert('insert into postgresql_pipeline_test_table (number) VALUES (1)', nil, [], 'id', 'postgresql_pipeline_test_table_id_seq')
  end

  it 'should create pipeline connection with encoding' do
    pipeline_connection = ActiveRecord::Base.postgres_pipeline_connection({min_messages: 'warning', encoding: 'unicode'})
    raw_conn = pipeline_connection.instance_variable_get(:@connection)
    expect(raw_conn.pipeline_status).to eq PG::PQ_PIPELINE_ON
  end

  it 'pipeline mode should be on in connection' do
    raw_conn = @pipeline_connection.instance_variable_get(:@connection)
    expect(raw_conn.pipeline_status).to eq PG::PQ_PIPELINE_ON
  end

  it 'should send the query in pipeline mode' do
    @pipeline_connection.exec_query('SELECT 1')
    @pipeline_connection.exec_query('SELECT 1')

    piped_results = @pipeline_connection.instance_variable_get(:@piped_results)
    expect(piped_results.try(:length)).to eq 2
  end

  describe 'Using exec_query' do
    it 'should return same results for normal and pipeline mode' do
      result = @connection.exec_query('select max(id) from postgresql_pipeline_test_table')
      expect = @pipeline_connection.exec_query('select max(id) from postgresql_pipeline_test_table')

      # Assertions
      expect(expect.result.rows.first.first).to eq result.rows.first.first
    end

    it 'should be able to return multiple query results in pipeline' do
      result_1 = @connection.exec_query('select max(id) from postgresql_pipeline_test_table')
      result_2 = @connection.exec_query('select min(id) from postgresql_pipeline_test_table')
    
      expect_1 = @pipeline_connection.exec_query('select max(id) from postgresql_pipeline_test_table')
      expect_2 = @pipeline_connection.exec_query('select min(id) from postgresql_pipeline_test_table')

      expect(expect_2.result.rows.first.first).to eq result_2.rows.first.first
      expect(expect_1.result.rows.first.first).to eq result_1.rows.first.first
    end

    it 'should only process results until the requested query' do
      future_result_1 = @pipeline_connection.exec_query('select max(id) from postgresql_pipeline_test_table')
      future_result_2 = @pipeline_connection.exec_query('select min(id) from postgresql_pipeline_test_table')
      future_result_3 = @pipeline_connection.exec_query('select * from postgresql_pipeline_test_table')

      expect {  future_result_2.result }.to change{  @pipeline_connection.instance_variable_get(:@piped_results).length }.from(3).to(1)
    end

    it 'should return fatal error on executing multiple SQL statements' do
      pipeline_conn = ActiveRecord::Base.postgres_pipeline_connection(min_messages: 'warning')
      expect {pipeline_conn.execute("select max(id) from postgresql_pipeline_test_table; SHOW TIME ZONE;")}
        .to raise_error(ActiveRecord::StatementInvalid, /cannot insert multiple commands into a prepared statement/)
    end

    it 'should initialize execution stack in future result creation' do
      pipeline_conn = ActiveRecord::Base.postgres_pipeline_connection(min_messages: 'warning')
      future_result = pipeline_conn.exec_query("select max(id) from postgresql_pipeline_test_table")
      expect(future_result.execution_stack).not_to be_nil
    end

    xit 'should clean the postgres pipeline if prior query in pipeline fails' do
      pipeline_conn = ActiveRecord::Base.postgres_pipeline_connection(min_messages: 'warning')
      future_result_first, future_result_second, future_result_third = nil
      expect {
        future_result_first = pipeline_conn.exec_query("select max(id) from postgresql_pipeline_test_table")
        future_result_second = pipeline_conn.exec_query("select max(id) from postgresql_pipeline_test_table; SHOW TIME ZONE;")
        future_result_third = pipeline_conn.exec_query("select min(id) from postgresql_pipeline_test_table")
        future_result_third.result
      }.to  raise_error(ActiveRecord::StatementInvalid)

      expect(future_result_second.error).not_to be_nil
      expect(future_result_second.error).to be_a_kind_of(ActiveRecord::StatementInvalid)

      future_result_post_rescue = pipeline_conn.exec_query("select max(id) from postgresql_pipeline_test_table")
      expect(future_result_post_rescue.result.rows.length).to be(1)
      expect(future_result_first.result.rows.length).to be(1)
      expect(future_result_third.result).to be_nil
      expect(future_result_third.error).to be_a_kind_of(ActiveRecord::PriorQueryPipelineError)
    end
  end

  describe 'Using query' do
    xit 'should assign the results back in the same order, as it was called' do
      assert_equal 1, 2
    end
  end
end
