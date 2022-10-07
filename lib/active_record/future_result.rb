# frozen_string_literal: true

module ActiveRecord
  class FutureResult # :nodoc:
    def initialize()
      @result = nil
      @event_buffer = nil
      @error = nil
      @pending = true
    end

    def result
    end

    def assign
      @pending = false
    end

    def clear; end
  end
end
