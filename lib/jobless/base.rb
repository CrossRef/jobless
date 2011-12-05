require "mongo_mapper"

require_relative "./data"

module Jobless
 
  module Logging
    def error e
      log :error, e
    end

    def warn msg
      log :warn, msg
    end

    def info msg
      log :info, msg
    end

    def debug msg
      log :debug, msg
    end
    
    private
    
    def log level, obj
      logger = settings.logger
      if logger.class == Proc
        logger.call(level, obj)
      elsif logger == :console
        puts "#{level.to_s}: #{obj.to_s}"
      elsif logger == :database
        LogLine.method(level).call(obj)
      else
        raise StandardError.new("Unknown logger type #{logger.to_s}")
      end
    end
  end

  module Configuration
    class Settings
      def initialize hsh
        @settings = {}.merge(hsh)
      end

      def [] name
        @settings[name]
      end

      def []= name, val
        @settings[name] = val
      end

      def method_missing name
        if @settings.include? name
          @settings[name]
        else
          super
        end
      end
    end
  
    def configure &block
      instance_eval(&block)
      Jobless.prepare settings
    end

    def set name, val
      settings[name] = val
    end

    def settings
      @defaults ||= {
        :db_host => "localhost",
        :db_name => "jobless",
        :logger => :database
      }
      @settings ||= Settings.new @defaults
      @settings
    end
  end

  module Base
    # Register a new job.
    def job kind, work
      Job.register kind, work
    end
    
    # Shorthand for a worker without tags. The worker will execute
    # once and then terminate.
    def task options={}, &block
      worker([], options, &block)
    end

    # Shorthand for a worker without tags and a force_period set to
    # the given period.
    def periodic period, options={}, &block
      worker([], options.merge({:with_period => period}), &block)
    end
      
    def worker tags=[], options={}, &block
      tags = [tags] unless tags.class == Array
      Worker.work(tags, options, &block)
    end
  end

  class Application
    include Base
    include Logging
    include Configuration
  end
  
  # Method delegation code stolen from Sinatra. Mixing this module
  # causes jobless methods to be delegated 
  module Delegator
    def self.delegate target, *methods
      methods.each do |method_name|
        define_method(method_name) do |*args, &block|
          return super(*args, &block) if respond_to? method_name
          Delegator.target.send(method_name, *args, &block)
        end
        private method_name
      end
    end

    delegate :job, :task, :periodic, :worker, :configure, :settings,
             :warn, :debug, :error, :info

    class << self
      attr_accessor :target
    end

    self.target = Application.new
  end

end
