# frozen_string_literal: true
module ActiveRecord
  class FutureResult # :nodoc:
    attr_accessor :block, :sql, :binds, :execution_stack, :error, :retry_method, :retry_args, :retry_object

    RESULT_TYPES = [ ActiveRecord::Result, Array , Integer]

    rejection_methods = [Kernel].inject([]){ |result, klass| result + klass.instance_methods }

    wrapping_methods = (RESULT_TYPES.inject([]) { |result, klass| result + klass.instance_methods } - [:==] - rejection_methods + [:dup, :pluck, :is_a?, :instance_of?, :kind_of?] ).uniq
    # TODO : Fix logic of rejection methods to reject below 2 functions as well
    wrapping_methods.delete(:__send__)
    #wrapping_methods.delete(:is_a?)
    wrapping_methods.each do |method|
      define_method(method) do |*args, &block|
        result
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
      @execution_stack = caller(1, 100)
    end

    def result
      if @error
        resubmit
        @connection_adapter.push_to_piped_results(self)
      end
      # Wait till timeout until pending is false
      return @result unless @pending

      @connection_adapter.pipeline_fetch(self)
      @result
    end

    def resubmit
      retry_object.send(retry_method, *retry_args)
      @result = nil
      @error = nil
      @pending = true
    end
    def assign(result)
      @result = result
      @result = @block.call(result) if @block
      @pending = false
      @resolved_time = Time.now
    end

    def assign_error(error)
      @error = error
      @resolved_time = Time.now
      @pending = false
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
