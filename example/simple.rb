require_relative "../lib/jobless"

configure do
  set :logger, :database
  set :db_host, "192.168.1.150"
end

def do_something work
  info "processing work"
  puts "work is #{work}"
  info "finished processing work"
end

worker :uncompress_deposits do |work|
  warn("db_host setting is #{settings.db_host}")
  do_something(work)
  info "done"
end
