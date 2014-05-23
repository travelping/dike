%    __                        __      _
%   / /__________ __   _____  / /___  (_)___  ____ _
%  / __/ ___/ __ `/ | / / _ \/ / __ \/ / __ \/ __ `/
% / /_/ /  / /_/ /| |/ /  __/ / /_/ / / / / / /_/ /
% \__/_/   \__,_/ |___/\___/_/ .___/_/_/ /_/\__, /
%                           /_/            /____/
%
% Copyright (c) Travelping GmbH <info@travelping.com>

-module(dike_master).

-behaviour(paxos_server).

-export([handle_call/3, init/1, init/2, export_state/1]).

-include_lib("../include/dike.hrl").

-export([start_link/1,
	 join/1,
	 join/0,
	 stop/1,
	 add_group/2,
	 get_routing_table/0]).

-record(state, {group_table,
		machine_table,
		groups=[],
		nodes=[],
		master_nodes=[]}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Group) ->
    gen_paxos:start_link_with_subscriber(master, ?MODULE, Group).

get_routing_table() ->
    Masters = dike_lib:masters(),
    case length(Masters) of
	1 ->
	    {error, single_node_deployment};
	_ ->
	    paxos_server:call(Masters, master, {get_routing_table, node()})
    end.

join() ->
    join(node()).

join(Node) ->
    Masters = dike_lib:masters(),
    case length(Masters) of
	1 ->
	    {error, single_node_deployment};
	_ ->
    paxos_server:call(Masters, master, {join, Node})
    end.

add_group(Gname, PaxosServerModule) ->
    Masters = dike_lib:masters(),
    case length(Masters) of
	1 ->
	    dike_stub_dispatcher:add_group(Gname, PaxosServerModule);
	_ ->
	    paxos_server:call(Masters, master, {add_group, Gname, PaxosServerModule})
    end.

stop(Gname) ->
    paxos_server:stop(Gname).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% bahaviour callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_Options) ->
    GT = ets:new(group_table, [ordered_set, private, {keypos, 2}]),
    MT = ets:new(machine_table, [bag, private, {keypos, 1}]),
    {ok, MasterNodes} = application:get_env(dike, masters),
    lager:debug([{class, dike}], "master starting on node ~p", [node()]),
    {ok, #state{group_table=GT,
		machine_table=MT,
		groups=[],
		nodes=MasterNodes,
		master_nodes=MasterNodes}}.

init(State = #state{group_table=GT_List, machine_table=MT_List}, _Options) ->
    GT = ets:new(group_table, [ordered_set, private, {keypos, 1}]),
    MT = ets:new(machine_table, [bag, private, {keypos, 1}]),
    ets:insert(GT, GT_List),
    ets:insert(MT, MT_List),
    State#state{group_table=GT, machine_table=MT}.

export_state(State=#state{group_table=GT, machine_table=MT}) ->
    State#state{group_table=ets:tab2list(GT), machine_table=ets:tab2list(MT)}.

handle_call({get_routing_table, Node}, From, State=#state{group_table=GT}) ->
    RetVal = {get_groups_for_node(State, Node), ets:tab2list(GT)},
    {reply,
     fun() ->
	     paxos_server:reply(From, RetVal)
     end,
     State};

%handle_call({add_groups, GList}, From, State) when is_list(GList) ->


handle_call({add_group, Gname, PaxosServerModule}, From, State = #state{groups=Groups, group_table=GT}) ->
    case lists:member(Gname, Groups) of
	true ->
	    [RVal=#routing_table_entry{group_name=Gname}] = ets:lookup(GT, Gname),
	    {reply, fun() -> paxos_server:reply(From, RVal) end, State};
	false ->
	    add_group_int(State, Gname, PaxosServerModule),
	    [RVal=#routing_table_entry{group_name=Gname, nodes=Nodes}] = ets:lookup(GT, Gname),
	    RFun = fun() ->
                           lager:debug([{class, dike}], "added a group to dike master: ~p", [Gname]),
			   [ok = dike_dispatcher:new_group(Node, Gname, PaxosServerModule, Nodes) || Node <- Nodes],
			   paxos_server:reply(From, RVal)
		   end,
	    {reply, RFun, State#state{groups=[Gname | Groups]}}
    end;

handle_call({join, Node}, From, State = #state{nodes=Nodes}) ->
    case lists:member(Node, Nodes) of
	true ->
	    {reply, fun() -> paxos_server:reply(From, already_known) end, State};
	false ->
	    Sideeffects = join_node(State, Node),
	    {reply,
	     fun() ->
		     paxos_server:reply(From, {[Node | Nodes]}),
		     Sideeffects()
	     end,
	     State#state{nodes=[Node | Nodes]}}
    end;

handle_call(_Req, _From, State) ->
    {noreply, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% internals
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

join_node(State=#state{nodes=Nodes}, Node) ->

    case find_loaded_node_and_avg(State) of
	{nil, 0, 0} ->
	    fun() -> nothing end;
	{NodeWithMostGroups, GroupCnt, GroupMemberCnt} when GroupCnt >= GroupMemberCnt / length(Nodes) , GroupCnt > 1 ->
	    Sideeffects = move_n_groups(State, NodeWithMostGroups, Node, trunc(GroupCnt +1 - GroupMemberCnt / length(Nodes))),
	    fun() -> [H() || H <- Sideeffects] end;
	_U ->
	    fun() -> nothing end
    end.

find_loaded_node_and_avg(State=#state{nodes=Nodes}) ->
    {_Node, _GroupCNt, _Avg} = find_loaded_node_and_avg(State, Nodes, nil, 0, 0).

find_loaded_node_and_avg(_State, [], Node, GroupCnt, OverallCnt) ->
    {Node, GroupCnt, OverallCnt};

find_loaded_node_and_avg(State=#state{machine_table=MT}, [H|T], Node, N, OverallCnt) ->
    case length(ets:lookup(MT, H)) of
	A when A > N ->
	    find_loaded_node_and_avg(State, T, H, A, OverallCnt + A);
	A when A =< N ->
	    find_loaded_node_and_avg(State, T, Node, N, OverallCnt + A)
    end.

move_n_groups(State=#state{machine_table=MT}, NodeWithMostGroups, EmptyNode, AmountToMove) ->
    {GroupsToMigrate, _} = lists:split(AmountToMove, ets:lookup(MT, NodeWithMostGroups)),
    [move_participation(State, NodeWithMostGroups, EmptyNode, Group) || {_Machine, Group} <- GroupsToMigrate].

move_participation(_State=#state{group_table=GT, machine_table=MT}, From, To, Group) ->
    ets:delete(MT, {From, Group}),
    ets:insert(MT, {To, Group}),
    [RTE=#routing_table_entry{group_name=Group, nodes=MemberList, module=Module}] = ets:lookup(GT, Group),
    ets:insert(GT, RTE#routing_table_entry{nodes=dike_lib:replace(From, To, MemberList)}),
    fun() ->
	    gen_server:cast({dike_dispatcher, To}, {join_group, Group, Module, MemberList, From, To})

    end. % this must be done in a sideeffect from the paxos_server.

add_group_int(State=#state{}, GroupName, ModuleName) ->
    add_group_int(State, GroupName, ModuleName, ?GROUP_SIZE).

add_group_int(State, _GroupName, _ModuleName, 0) ->
    State;

add_group_int(State=#state{machine_table=MT, group_table=GT}, GroupName, ModuleName, N) ->
    Node = find_node_with_least_groups(State, GroupName),
    ets:insert(MT, {Node, GroupName}),
    case ets:lookup(GT, GroupName) of
	[] ->
	    ets:insert(GT, #routing_table_entry{module=ModuleName, group_name=GroupName, nodes=[Node]});
	[E=#routing_table_entry{module=ModuleName, group_name=GroupName, nodes=NodeList}] ->
	    if length(NodeList) < ?GROUP_SIZE ->
		    ets:insert(GT, E#routing_table_entry{nodes=[Node | NodeList]});
	       true ->
		    lager:debug([{class, dike}], "in dike_Master, trying to add a Node to a group that is already full!", [])
	    end
    end,
    add_group_int(State, GroupName, ModuleName, N-1).

find_node_with_least_groups(#state{nodes=Nodes, machine_table=MT}, GroupName) ->
    {Node, _GroupCount} = lists:foldl(fun(Elem, {Node, Count}) ->
					      Grps = [Grp || {_Elem, Grp} <- ets:lookup(MT, Elem)],
					      case lists:member(GroupName, Grps) of
						  true ->
						      {Node, Count};
						  false ->
						      if length(Grps) < Count ->
							      {Elem, length(Grps)};
							 true ->
							     {Node, Count}
						      end
					      end
				      end,
				      {nil, 1000000}, %% this number should be an upper bound for the groupcount on a node...
				      Nodes),
    Node.

get_groups_for_node(#state{machine_table=MT}, Node) ->
    [Group || {_, Group} <- ets:lookup(MT, Node)].
