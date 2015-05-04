
-module(paxos_hashring).

-export([start/0,
	 send/2,
	 cast/2,
	 check_consistency/0,
         get_group_count/0]).

-define(GROUP_COUNT, 16).

start() ->
    [dike_master:add_group(VNode , arithmetic_paxos) || VNode <- generate_paxos_groups()].

send(Entity, Req) ->
    Id = erlang:phash2(Entity) rem ?GROUP_COUNT,
    dike_dispatcher:request(list_to_atom("vnode-" ++ integer_to_list(Id+1)), Req).

cast(Entity, Req) ->
    Id = erlang:phash2(Entity) rem ?GROUP_COUNT,
    dike_dispatcher:cast(list_to_atom("vnode-" ++ integer_to_list(Id+1)), Req).


check_consistency() ->
    dike_dispatcher:refresh_routing_table(),
    RetVal = lists:map(fun(Group) ->
			       [paxos_server:call(Node, Group, read) || Node <- dike_dispatcher:get_nodes(Group)]
		       end,
		       generate_paxos_groups()),
    [true=dike_lib:uniform_list(ReadValueRow) || ReadValueRow <- RetVal],
    RetVal.

get_group_count() -> ?GROUP_COUNT.

generate_paxos_groups() ->
    [list_to_atom("vnode-" ++ integer_to_list(I)) || I <- lists:seq(1, ?GROUP_COUNT)].
