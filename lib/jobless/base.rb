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
      if @params[:logger].class == Proc
        @params[:logger].call(level, obj)
      elsif @params[:logger] == :console
        puts "#{level.to_s}: #{obj.to_s}"
      else
        LogLine.method(level).call(obj)
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

  module Work
    # Get at that work!
    attr_accessor :work
    
    def has_work?
      @work.nil?
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
      worker([], self, options, &block)
    end

    # Shorthand for a worker without tags and a force_period set to
    # the given period.
    def periodic period, options={}, &block
      worker([], self, options.merge({:with_period => period}), &block)
    end
      
    def worker tags=[], options={}, &block
      Jobless.prepare(settings)
      tags = [tags] unless tags.class == Array
      Worker.work(tags, self, options, &block)
    end
  end

  class Application
    include Base
    include Work
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

    delegate :job, :task, :periodic, :worker, :configure

    class << self
      attr_accessor :target
    end

    self.target = Application.new
  end

end
