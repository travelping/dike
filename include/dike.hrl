
-record(routing_table_entry, {group_name,
			      nodes,
			      lastchange,
			      module}).

-define(GROUP_SIZE, application:get_env(dike, group_size, 3)).

-define(CHANGE_MEMBER_TAG, '$paxos_coordinator_member_change$x').

-define(SERVER_PERSISTED_TAG, '$paxos_server_persisted$x').

-define(PING_TIMEOUT, 10000).

-define(CLIENT_RETRY_INTERVAL, 1000).

-define(CLIENT_CALL_TIMEOUT, 20000). % rises chance of double-issuing requests if to low

-define(PERSISTENCE_INTERVAL, 100).

-define(PERSISTENCE_VARIANCE, 20).

-define(DB_TRANSACTION_HANDLER, dike_transaction_handler).

-define(COORDINATOR_TAG, '$paxos_coordinator$-').

-define(MAX_RT_AGE, 60).

-define(INTERCOMM_TIMEOUT, 10000).

-define(UPDATE_SUBSCRIBER_TIMEOUT, timer:minutes(1)).
