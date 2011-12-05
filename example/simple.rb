require_relative "../lib/jobless"

configure do
  set :logger, :console
  set :db_host, "192.168.1.150"
end

worker :uncompress_deposits do
  warn "something"
end
