require 'spec_helper'
require 'pg_activerecord_ext'
require 'model/author'
require 'model/user'

GREEN   = "\e[32m"
BLUE    = "\e[34m"
CLEAR   = "\e[0m"
color = GREEN

RSpec.describe 'ActiveRecord::Relation' do
  before(:each) do
    color = (color == GREEN) ? BLUE : GREEN
  end
  before(:all) do
    @connection = ActiveRecord::Base.postgresql_connection(min_messages: 'warning')
    @connection.drop_table(:users, if_exists: true)
    @connection.drop_table(:authors, if_exists: true)
    @connection.create_table(:users, id: :string, limit: 42, force: true) do |t|
      t.column :description, :string, limit: 5
    end
    @connection.create_table :authors do |t|
      t.column :user_id, :string
    end
    ActiveRecord::Base.logger = ActiveSupport::Logger.new(STDOUT)
    ActiveRecord::Base.establish_connection("adapter" => "postgresql")
    @user_1 = User.create(id: 3)
    @user_2 = User.create(id: 4)
    @author = Author.create(id: 4, user_id: "3")
    @callback = lambda {|*args| Logger.new(STDOUT).debug("#{color} #{args.last[:sql]} #{CLEAR}" )  unless args.last[:name] == "SCHEMA" }
  end


  it 'should fetch results for where clause in pipeline mode when load_in_pipeline is called' do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    ActiveSupport::Notifications.subscribed( @callback, "sql.active_record") do
      users = User.where("id is not null").load_in_pipeline
      expect(users).to eq([@user_1, @user_2])
    end
  end

  it 'should fetch results for where clause in pipeline mode even when load_in_pipeline is not explicity called' do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    ActiveSupport::Notifications.subscribed( @callback, "sql.active_record") do
      users = User.where("id is not null")
      expect(users).to eq([@user_1, @user_2])
    end

  end

  it 'should fetch results  when all queries are loaded in pipeline' do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    ActiveSupport::Notifications.subscribed(@callback, "sql.active_record") do
      users_1 =  User.where("id is not null").load_in_pipeline
      users_2 =  User.where("id = '4'").load_in_pipeline
      expect(users_1).to eq([@user_1, @user_2])
      expect(users_2.first).to eq(@user_2)
    end
  end

  it 'should fetch results when some queries are loaded in pipeline' do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    @sql = []
    track_sql_queries = lambda do |*args|
      unless args.last[:name] == "SCHEMA" || args.last[:name] == "PIPELINE_FETCH"
        Logger.new(STDOUT).debug("#{color} #{args.last[:sql]} #{CLEAR}" )
        @sql << args.last[:sql]
      end
    end
    ActiveSupport::Notifications.subscribed( track_sql_queries, "sql.active_record") do
      users_1 =  User.where("id is not null")
      users_2 =  User.where("id = '4'").load_in_pipeline
      expect(users_1).to eq([@user_1, @user_2])
      expect(users_2.first).to eq(@user_2)
    end
    expect(@sql.first).to eq("SELECT \"users\".* FROM \"users\" WHERE (id = '4')")
    expect(@sql.last).to eq("SELECT \"users\".* FROM \"users\" WHERE (id is not null)")
  end

  it 'should fetch results for dependent queries' do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    ActiveSupport::Notifications.subscribed( @callback, "sql.active_record") do
      users =   User.where("id is not null").load_in_pipeline
      authors = Author.where(user_id: users.first.id)
      expect(authors.first).to eq( @author)
    end
  end

  it 'should fail with exception as the limit of description is set to 5 characters' do
    user = User.new(id: 90 , description: "hellloo")
    expect {user.save!}.to  raise_error(ActiveRecord::ValueTooLong)

    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")

    expect {user.save!}.to  raise_error(ActiveRecord::ValueTooLong)
  end

  it "empty? should work in pipeline mode" do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    expect(User.where("id is not null").load_in_pipeline.empty?).to eq(false)
  end

  it "size should work in pipeline mode" do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    expect(User.where("id is not null").load_in_pipeline.size).to eq(2)
  end

  context "Handling retry scenarios" do
    after(:each) do
      @connection.remove_column :authors, :baz
    end
    it 'should retry when prepare statement cache is expired' do
      ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
      ActiveSupport::Notifications.subscribed( @callback, "sql.active_record") do
        Author.where(user_id: 3).to_a
        @connection.add_column :authors, :baz, :string
        authors = Author.where(user_id: 3).load_in_pipeline
        expect(authors.first).to eq( @author)
      end
    end

    it 'should reload relation when its failure is due to previously submitted query' do
      ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
      ActiveSupport::Notifications.subscribed( @callback, "sql.active_record") do
        Author.where(user_id: 3).to_a
        @connection.add_column :authors, :baz, :string
        authors = Author.where(user_id: 3).load_in_pipeline
        user = User.where("id  = '3'").load_in_pipeline
        expect(authors.first).to eq( @author)
        expect(user).to eq([@user_1])
      end
    end
  end

  it "should set future_result instance when load_in_pipeline is called" do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    adapter = ActiveRecord::Base.connection
    ActiveSupport::Notifications.subscribed( @callback, "sql.active_record") do
      expect(adapter).not_to receive(:initialize_results)
      users = User.where("id is not null").load_in_pipeline
      expect(users.instance_variable_get(:@future_result).class).to eq(ActiveRecord::FutureResult)
    end
  end

  it "should set exception_block to future_result" do
    ActiveRecord::Base.establish_connection("adapter" => "postgres_pipeline")
    ActiveRecord::Base.connection
    ActiveSupport::Notifications.subscribed( @callback, "sql.active_record") do
      exception_caught = false
      exception_proc = Proc.new{ exception_caught = true }
      expect_any_instance_of(PG::Connection).to receive(:get_result).and_raise(PG::Error)
      users = User.where("id is not null").load_in_pipeline(exp_block: exception_proc)
      expect{ users.to_a }.not_to raise_exception(ActiveRecord::StatementInvalid)
      expect(exception_caught).to eq(true)
    end
  end
end