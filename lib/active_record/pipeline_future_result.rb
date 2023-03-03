# frozen_string_literal: true
module ActiveRecord
  class FutureResult # :nodoc:
    attr_accessor :block, :sql, :binds, :execution_stack, :error, :exception_block

    RESULT_TYPES = [ ActiveRecord::Result, Array , Integer]

    rejection_methods = [Kernel].inject([]){ |result, klass| result + klass.instance_methods }

    wrapping_methods = (RESULT_TYPES.inject([]) { |result, klass| result + klass.instance_methods } - [:==] - rejection_methods + [:dup, :pluck, :is_a?, :instance_of?, :kind_of?] ).uniq
    # TODO : Fix logic of rejection methods to reject below 2 functions as well
    wrapping_methods.delete(:__send__)
    #wrapping_methods.delete(:is_a?)
    wrapping_methods.each do |method|
      define_method(method) do |*args, &block|
        result if @pending
        @result.send(method, *args, &block)
      end
    end

    def initialize(connection_adapter, sql, binds)
      @connection_adapter = connection_adapter
      @result = nil
      @event_buffer = nil
      @error = nil
      @pending = true
      @block = nil
      @sql = sql
      @binds = binds
      @creation_time = Time.now
      @resolved_time = nil
      @exception_block = []
      @execution_stack = caller(1, 100)
    end

    def result
      # Wait till timeout until pending is false
      return @result unless @pending

      @connection_adapter.pipeline_fetch(self)
      @result
    end

    def assign(result)
      @result = result
      @result = @block.call(result) if @block
      @pending = false
      @resolved_time = Time.now
    end

    def set_result(result)
      @result = result
      @pending = false
      @resolved_time = Time.now
      self
    end

    def on_error(&block)
      @exception_block << block
    end

    def execute_on_error(exp)
      current_exp = exp
      @exception_block.each do |block|
        begin
          block.call(current_exp)
          current_exp = nil
          break
        rescue StandardError => e
          current_exp = e
        end
      end
      raise current_exp if current_exp
    end


    def assign_error(error)
      @error = error
      @resolved_time = Time.now
      @pending = false
      execute_on_error(error)
    end

    def ==(other)
      if other.class == ActiveRecord::FutureResult
        super
      else
        result if @pending
        @result == other
      end
    end
  end
end