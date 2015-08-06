use Mix.Config

config :dike,
  masters: ['test1@ws009-lx', 'test2@ws009-lx', 'test3@ws009-lx', 'test4@ws009-lx', 'test5@ws009-lx'],
  db_adapter: :dike_db_adapter_ets,
  db_dir: '/tmp/',
  db_mode: :per_group,
  paxos_timeout: 50,
  garbage_collect_interval: 10

