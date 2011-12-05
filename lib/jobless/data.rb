require "mongo_mapper"
require "socket"

module Jobless

  class Job
    include MongoMapper::Document

    key :kind, String
    key :work
    key :started, Boolean, :default => false
    key :completed, Boolean, :default => false
    key :failed, Boolean, :default => false
    key :failure, String
    key :started_at, Time
    key :finished_at, Time

    def self.next kinds, context=self, &block
      opts = {
        :query => {:kind => {"$in" => kinds}, :started => false},
        :update => {
          "$set" => {"started" => true, "started_at" => Time.now}
        },
        :new => true
      }
      
      doc = MongoMapper.database.collection(:jobs).find_and_modify(opts)
      if doc
        mapper_doc = new(doc)
        begin
          Worker.check_in "working"
          context.instance_exec(mapper_doc[:work], &block)
          mapper_doc.completed = true
          
        rescue StandardError => e
          mapper_doc.failed = true
          mapper_doc.failure = e.message
          LogLine.error e
        end

        mapper_doc.finished_at = Time.now
        mapper_doc.save
      end

      Worker.check_in "ready"

      !doc.nil?
    end
  end

  class Worker
    include MongoMapper::Document

    key :host, String
    key :pid, Integer
    key :name, String
    key :status, String
    key :check_in_at, Time
    key :ended, Boolean, :default => false

    many :log_lines, :class_name => "Jobless::LogLine"

    def self.terminate
      host = Socket.gethostbyname(Socket.gethostname).first
      pid = Process.pid
      if worker = first(:host => host, :pid => pid)
        worker.ended = true
        worker.save
      end
    end

    def self.work_loop kind, context=self, options={}, &block
      options = {:affinity => 20}.merge(options)
      while true
        sleep(options[:affinity]) unless Job.next(kinds, context, &block)
      end
    end

    def self.work kinds=nil, context=self, options={}, &block
      at_exit { terminate }
      check_in "ready"
      begin
        if kinds.nil? || kinds.empty?
          check_in "working"
          context.instance_eval(&block)
        else
          work_loop kinds, options, &block
        end
      rescue StandardError => e
        LogLine.error e
      end
      terminate
    end
    
    def self.check_in status
      host = Socket.gethostbyname(Socket.gethostname).first
      pid = Process.pid
      worker = Worker.first_or_new(:host => host, :pid => pid)
      worker.host = host
      worker.pid = pid
      worker.check_in_at = Time.now
      worker.status = status
      worker.name = File.basename($0)
      worker.ended = false
      worker.save
    end

    def self.me
      host = Socket.gethostbyname(Socket.gethostname).first
      pid = Process.pid
      Worker.first(:host => host, :pid => pid)
    end
  end

  class LogLine
    include MongoMapper::Document

    key :message, String
    key :level, String
    key :component, String

    one :exception_info, :class_name => "Jobless::ExceptionInfo"
    belongs_to :worker, :class_name => "Jobless::Worker"
    
    timestamps!

    def self.error e
      component = File.basename($0)
      create(:worker => Worker.me,
             :message => e.message,
             :level => "error",
             :component => component,
             :exception_info => ExceptionInfo.new_for_exception(e))
    end

    def self.warn msg
      create(:worker => Worker.me,
             :message => msg,
             :level => "warn",
             :component => File.basename($0))
    end

    def self.info msg
      create(:worker => Worker.me,
             :message => msg,
             :level => "info",
             :component => File.basename($0))
    end

    def self.debug msg
      create(:worker => Worker.me,
             :message => msg,
             :level => "debug",
             :component => File.basename($0))
    end
  end

  class ExceptionInfo
    include MongoMapper::EmbeddedDocument

    key :name, String
    key :message, String
    key :backtrace, Array

    belongs_to :log_line, :class_name => "Jobless::LogLine"

    def self.new_for_exception e
      new(:name => e.class.name, :message => e.message, :backtrace => e.backtrace)
    end
  end

  def self.prepare settings
    MongoMapper.connection = Mongo::Connection.new(settings.db_host)
    MongoMapper.database = settings.db_name

    Job.ensure_index :kind
    Job.ensure_index :started
    Job.ensure_index :completed
    Job.ensure_index :failed
    Job.ensure_index :finished_at

    Worker.ensure_index([[:host, 1], [:pid, 1]])
    
    LogLine.ensure_index :worker
    LogLine.ensure_index :created_at
  end

end
