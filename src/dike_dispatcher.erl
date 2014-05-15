%%%-------------------------------------------------------------------
%%% @author Ole Rixmann <orixmann@ws002-lx>
%%% @copyright (C) 2013, Ole Rixmann
%%% @doc
%%%
%%% @end
%%% Created : 31 Jan 2013 by Ole Rixmann <orixmann@ws002-lx>
%%%-------------------------------------------------------------------
-module(dike_dispatcher).

-include_lib("../include/dike.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1,
	 get_nodes/1,
	 request/2,
	 request/3,
	 cast/2,
	 group_update/2,
	 refresh_routing_table/0,
	 check_remote_group/2,
	 new_group/4,
	 get_routing_table/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(DISPATCHER_TIMEOUT, 2000).

-record(state, {routing_table,
		rt_timestamp=0,
		local_groups=[],
		timer}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Masters) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Masters], []).

check_remote_group(G, _M) ->
%    Groups= get_nodes(G) -- [node()],
    gen_paxos:busy_ping(node(), G).



get_nodes(PGroup) ->
    get_nodes(PGroup, initial).

get_nodes(PGroup, initial) ->
    case catch ets:lookup(dispatcher_table, PGroup) of
        [#routing_table_entry{group_name=PGroup, nodes=RVal}]  ->
            RVal;
        _ ->
            timer:sleep(500),
            catch refresh_routing_table(), %% well this is desperate - but works :)
            get_nodes(PGroup, final)
    end;

get_nodes(PGroup, final) ->
    case catch ets:lookup(dispatcher_table, PGroup) of
        [#routing_table_entry{group_name=PGroup, nodes=RVal}]  ->
            RVal;
        _E ->
            {error, not_found}
    end.

refresh_routing_table() ->
    gen_server:call(?MODULE, refresh_routing_table, 20000).

request(Node, PGroup, Val) ->
    request(Node, PGroup, Val, 0, 5).

request(_,_,_, N, N) ->
    throw({error, not_found});

request(Node, PGroup, Val, I, N) ->
    case get_nodes(PGroup) of
	Nodes when is_list(Nodes), length(Nodes) == 1 ->
	    stub_paxos:append(hd(Nodes), PGroup, Val);
        Nodes when is_list(Nodes) ->
            paxos_server:call(Nodes, PGroup, Val);
        {error, not_found} ->
	    timer:sleep(timer:seconds(5)),
	    request(Node, PGroup, Val, I+1, N)
    end.


request(PGroup, Val) ->
    case get_nodes(PGroup) of
	Nodes when is_list(Nodes), length(Nodes) == 1 ->
	    stub_paxos:append(hd(Nodes), PGroup, Val);
        Nodes when is_list(Nodes) ->
            paxos_server:call(Nodes, PGroup, Val);
        {error, not_found} ->
            throw({error, not_found})
    end.

cast(PGroup, Val) ->
    Nodes =  get_nodes(PGroup),
    paxos_server:cast(Nodes, PGroup, Val).

group_update(GName, NewMembers) ->
    catch gen_server:call(?MODULE, {group_update, GName, NewMembers}).

new_group(Node, Gname, PaxosServerModule, Nodes) ->
    try
	ok = gen_server:call({dike_dispatcher, Node}, {new_group, Gname, PaxosServerModule, Nodes})
    catch
	_Error:_Reason ->
	    new_group(Node, Gname, PaxosServerModule, Nodes)
    end.

get_routing_table() ->
    gen_server:call(?MODULE, get_routing_table).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([_Masters]) ->
    process_flag(trap_exit, true),
    RoutingTable=ets:new(dispatcher_table, [ordered_set, protected, named_table, {keypos, 2}, {read_concurrency, true}]),
    TRef = erlang:send_after(?DISPATCHER_TIMEOUT, ?MODULE, timeout),
    {ok, #state{routing_table=RoutingTable, rt_timestamp=dike_lib:timestamp(),
		timer=TRef, local_groups=[]}}.

handle_call(get_routing_table, _From, State=#state{routing_table=RT}) ->
    {reply, ets:tab2list(RT), State};


handle_call({group_update, GName, NewMembers}, _From, State=#state{routing_table=RT, local_groups=LGS}) ->
    A= length(NewMembers) - length(NewMembers -- [node()]),
    LGS2 = if A == 0 ->
		   LGS -- [GName];
	      true ->
		   LGS
	   end,
    case ets:lookup(RT, GName) of
	[RTE] ->
	    ets:insert(RT, RTE#routing_table_entry{nodes=NewMembers, lastchange=dike_lib:timestamp()}),
	    {reply, ok, State#state{local_groups=LGS2}};
	[] ->
	    {reply, ok, update_routing_table(State, true)}
    end;

handle_call(refresh_routing_table, _From, State) ->
    S2 = update_routing_table(State, true),
    {reply, ok, S2};

handle_call({new_group, Gname, PaxosServerModule, Nodes}, _From, State=#state{routing_table=RoutingTable, local_groups=MyGroups}) ->
    S3=case ets:lookup(RoutingTable, Gname) of
	   [#routing_table_entry{group_name=Gname}] ->
	       case {paxos_server:ping(node(), Gname, PaxosServerModule), gen_paxos:busy_ping(node(), Gname)} of
		   {pang, pang} ->
		       %%the dispatcher found the routingtable before being informed about starting this group....
		       ets:insert(RoutingTable, #routing_table_entry{group_name=Gname,
								     nodes=Nodes,
								     module=PaxosServerModule}),
		       exit(whereis(dike_lib:get_group_coordinator_name(Gname)), normal),
		       exit(whereis(paxos_server:generate_paxos_server_name(Gname, PaxosServerModule)), normal),
		       gen_paxos:start_link_with_subscriber(Gname, PaxosServerModule, Nodes),
		       State;
		   _ ->
		       State
	       end;
	   [] ->
               lager:debug([{class, dike}], "adding group ~p to dike on ~p!~n", [Gname, node()]),
	       S2 = case dike_lib:position(Nodes, node()) of
			not_found ->
			    nothing_to_do_on_this_node,
			    State;
			_Pos ->
			    gen_paxos:start_link_with_subscriber(Gname, PaxosServerModule, Nodes),
			    State#state{local_groups=[Gname| MyGroups]}
		    end,
	       ets:insert(RoutingTable, #routing_table_entry{group_name=Gname,
							     nodes=Nodes,
							     module=PaxosServerModule}),
	       S2
       end,
    {reply, ok, S3};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({join_group, Group, Module, Nodes, From, To}, State=#state{routing_table=RT, local_groups=MyGroups}) ->
    case dike_lib:position(MyGroups, Group) of
	not_found ->
            lager:debug([{class, dike}], "joining group ~p on ~p~n", [Group, node()]),
	    Nodes2=dike_lib:replace(From, To, Nodes),
	    gen_paxos:start_link_and_replace(Group, Module, Nodes2, From, From),
	    ets:insert(RT, #routing_table_entry{nodes=Nodes2, module=Module, group_name=Group, lastchange=dike_lib:timestamp()}),
	    {noreply, State#state{local_groups=[Group | MyGroups]}};
	_Pos ->
	    {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State=#state{timer=TRef}) ->
    erlang:cancel_timer(TRef),
    S2 = update_routing_table(State),
    check_local_groups(S2),
    TRef2 = erlang:send_after(?DISPATCHER_TIMEOUT, ?MODULE, timeout),
    {noreply, S2#state{timer=TRef2}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #state{local_groups=MyGroups}) ->
    lager:info([{class, dike}], "dispatcher terminating for reason ~p~n", [Reason]),
    [gen_paxos:stop(Group) || Group <- MyGroups],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
update_routing_table(State) ->
    update_routing_table(State, false).

update_routing_table(State=#state{routing_table= RT, rt_timestamp=Timestamp}, Forced) ->
    Now = dike_lib:timestamp(),
    TabCount = get_table_entry_count(RT),
    if Now - ?MAX_RT_AGE > Timestamp ; Forced == true ; TabCount == 0 ->
	    case  catch dike_master:get_routing_table() of
		{MyGroups , V} when is_list(V), is_list(MyGroups) ->
		    ets:delete_all_objects(RT),
		    ets:insert(RT, V),
		    State#state{rt_timestamp=dike_lib:timestamp(), local_groups=MyGroups};
		_H ->
		    State
	    end;
       true ->
	    State
    end.

check_local_groups(State=#state{routing_table=RT, local_groups=MyGroups}) ->

    [check_local_group(State, RTE) || [RTE] <- [ets:lookup(RT, G) || G <- MyGroups]],
    State.

check_local_group(_State, RTE=#routing_table_entry{group_name=GName, nodes=Nodes}) ->
    case dike_lib:position(Nodes, node()) of
	not_found ->
	    just_got_stopped;
	_V ->
	    case gen_paxos:ping(node(), GName) of
		pong ->
		    ok;
		pang ->
		    Test2 = whereis(dike_lib:get_group_coordinator_name(GName)),
		    if Test2 == undefined ->
			    gen_paxos:restart_group_statefull(RTE);
		       true ->
			    nothing
		    end
	    end
    end.

%% restart_group_statefull_if_persisted(State=#state{}, RTE) ->
%%     {ok, DBAdapter} = application:get_env(dike, db_adapter),
%%     case DBAdapter:persisting() of
%% 	true ->
%% 	    gen_paxos:restart_group_statefull(RTE);
%% 	false ->
%% 	    gen_paxos:

get_table_entry_count(Tab) ->
    proplists:get_value(size, ets:info(Tab)).
