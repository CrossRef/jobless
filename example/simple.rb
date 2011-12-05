require_relative "../lib/jobless"

configure do
  set :logger, :console
  set :db_host, "192.168.1.150"
end

def do_something work
  puts "work is #{work}"
end

worker :uncompress_deposits do |cxt|
  cxt.warn("db_host setting is #{cxt.settings.db_host}")
  do_something(cxt.work)
  cxt.info "done"
end
