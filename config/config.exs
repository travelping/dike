use Mix.Config

config :dike,

  # List of masternodes, must be identical on every clusternode, may contain 1,3 or 5 elements
  masters: ['test1@ws009-lx', 'test2@ws009-lx', 'test3@ws009-lx', 'test4@ws009-lx', 'test5@ws009-lx'],

  # Currently only the ets adapter is supported.
  db_adapter: :dike_db_adapter_ets,

  # Specifies if in the db_adapter to use one table `per_group`, `per_vm` or `per_machine`.
  db_mode: :per_group,

  # Directory to store the database (only used if the `db_adapter` supports on-disk storage).
  db_dir: '/tmp/',

  # Timeout (in milliseconds) for the paxos protocol events.
  # If set low failover time will decrease but if set to low failover will prevent paxos from deciding.
  paxos_timeout: 50,

  # Trigger erlang garbage collection all n requests in the processes making up a paxos group.
  garbage_collect_interval: 10

