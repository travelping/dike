%    __                        __      _
%   / /__________ __   _____  / /___  (_)___  ____ _
%  / __/ ___/ __ `/ | / / _ \/ / __ \/ / __ \/ __ `/
% / /_/ /  / /_/ /| |/ /  __/ / /_/ / / / / / /_/ /
% \__/_/   \__,_/ |___/\___/_/ .___/_/_/ /_/\__, /
%                           /_/            /____/
%
% Copyright (c) Travelping GmbH <info@travelping.com>

-module(gen_paxos).

-include_lib("paxos_lib.hrl").
-include_lib("dike.hrl").

-behaviour(gen_server).

-behaviour(paxos_fsm).


%% API

-export([%start_link/2,
	 %start_link/3,
	 restart_group_statefull/1,
	 start_link_and_replace/5,
	 start_link_copy_state/4,
	 start_link_with_subscriber/3,
	 append_no_reply/3, 
	 append/4,
	 newest_outcome/1,
	 subscribe/1,
	 stop/2,
	 stop/1,
	 lock_log_complete/2,
	 unlock_log_complete/2,
	 unlock_log_complete_after_persisting/3,
	 set_and_unlock_log_complete/3,
	 ping/2,
	 busy_ping/2,
	 request_issued_ping/3]).


%% gen_server callbacks

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

%% paxos_coordinator callbacks

-export([send/3, broadcast/3, callback/2]).

-define(SERVER, ?MODULE). 
-define(UNLOCK_TIMEOUT, timer:minutes(1)).

-define(UPDATE_LC_TIMEOUT, timer:seconds(5)).

-record(state, {group_name,
		index=-1,
		position, 
		others = [],
		calls,
		log,
		log_complete=-1,
		log_complete_locked=false, %% false | {setting_up, From, From2} | {true, From2}
		paxos_server_persisted=-1,
		subscriber=nil,
		group_members=[],
		new_persistence_variance,
		db_adapter,
		members_persisted_at,
		log_cut}).



%%%===================================================================
%%% API
%%%===================================================================


restart_group_statefull(#routing_table_entry{group_name=GName, nodes=Nodes, module=Module}) ->
    %lager:debug([{class, dike}], "restarting group ~p on node ~p~n", [GName, node()]),
    catch gen_paxos:stop(GName),
    catch paxos_server:stop({GName, Module}),
    
    {_DBMod, _DBHandler} = init_db(GName),
    _FakeState = #state{group_name = GName},
    
    %% case DBMod:get(DBHandler, persisted_at) of
    %% 	{error, undefined} ->
    %% 	    start_link(GName, Nodes, false, {DBMod, DBHandler}),
    %% 	    paxos_server:start_link(GName, Module);
    %% 	{ok, V} when is_integer(V) ->
    %% 	    {ok, ServerState} = DBMod:get(DBHandler, generate_subject(FakeState, {persisted_state, V})),
    %% 	    {ok, GenPaxosState} = DBMod:get(DBHandler, generate_subject(FakeState, {persisted_gen_paxos_state, V})),
    %% 	    %lager:debug([{class, dike}], "found old states while starting: ~p ~p~n", [ServerState, GenPaxosState]),
    %% 	    start_link(GName, Nodes, false, {DBMod, DBHandler}),
    %% 	    paxos_server:start_link(GName, Module);
    %% 	A ->
    %% 	    %lager:debug([{class, dike}], "bad db response while trying to get old states: ~p~n", [A])
    %% end,
    
    case find_node_with_state(Nodes, GName, Module) of 
	error_empty ->
	    %lager:debug([{class, dike}], "Restarting an instance without State, this is inconsistent if values have been decided!~n", []),
	    start_link_with_subscriber(GName, Module, Nodes);
	NodeWithState ->	    
	    start_link_copy_state(GName, Module, Nodes, NodeWithState)
    end.

start_link_and_replace(Group, Module, Nodes, CopyStateFrom, Replace) ->
    start_link(Group, Nodes, {true, node()}, init_db(Group)),
    paxos_server:start_link(Group, Module, Nodes, CopyStateFrom, Replace).

start_link_copy_state(Group, Module, Nodes, NodeWithState) ->
    start_link(Group, Nodes, {true, node()}, init_db(Group)),
    paxos_server:start_link(Group, Module, NodeWithState).

start_link_with_subscriber(Group, Module, Nodes) ->
    {DBMod, DBHandler} = init_db(Group),
    FakeState = #state{group_name = Group},
    case DBMod:get(DBHandler, generate_subject(FakeState, persisted_at)) of
	{error, undefined} ->
	    start_link(Group, Nodes, {true, node()}, {DBMod, DBHandler}),
	    paxos_server:start_link(Group, Module);
	{ok, V} when is_integer(V) ->
	    {ok, ServerState} = DBMod:get(DBHandler, generate_subject(FakeState, {persisted_state, V})),
	    {ok, GenPaxosState} = DBMod:get(DBHandler, generate_subject(FakeState, {persisted_gen_paxos_state, V})),
	    %lager:debug([{class, dike}], "found old states while starting: ~p ~p~n", [ServerState, GenPaxosState]),
	    start_link(Group, Nodes, {true, node()}, {DBMod, DBHandler}),
	    paxos_server:start_link(Group, Module);
	A ->
	    lager:debug([{class, dike}], "bad db response while trying to get old states: ~p~n", [A])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% There will be a paxos coordinator started that handles the paxos group
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------

start_link(PaxosGroupName, GroupMembers, Locked, {DBMod, DBHandler}) ->
    case dike_lib:position(GroupMembers, node()) of
	not_found ->
	    discarded;
	_ ->
	    gen_server:start_link({local, get_group_coordinator_name(PaxosGroupName)}, 
				  ?MODULE, 
				  [PaxosGroupName, GroupMembers, Locked, {DBMod, DBHandler}], 
				  [])
    end.

ping(Node, GName) ->
    case catch gen_server:call({get_group_coordinator_name(GName), Node}, ping, ?PING_TIMEOUT) of
	{'EXIT', _Reason} ->
	    pang;
	pong ->
	    pong;
	_ ->
	    pang
    end.

request_issued_ping(Node, GName, Ref) ->
    case catch gen_server:call({get_group_coordinator_name(GName), Node}, {request_issued_ping, Ref, self()}, ?PING_TIMEOUT) of 
	{'EXIT', _Reason} ->
	    pang;
	pong ->
	    pong;
	_R ->
	    %lager:debug([{class, dike}], "busy_ping failing because of ~p~n", [_R]),
	    pang
    end.

busy_ping(Node, GName) ->
    case catch gen_server:call({get_group_coordinator_name(GName), Node}, busy_ping, ?PING_TIMEOUT) of
	{'EXIT', _Reason} ->
	    %lager:debug([{class, dike}], "busy_ping failing because of ~p~n", [_Reason]),
	    pang;
	pong ->
	    pong;
	busy ->
	    %lager:debug([{class, dike}], "busy_ping failing because of ~p~n", [busy]),
	    pang;
	_R ->
	    %lager:debug([{class, dike}], "busy_ping failing because of ~p~n", [_R]),
	    pang
    end.
 

set_and_unlock_log_complete(Node, GName, NLC) ->
    case catch gen_server:call({get_group_coordinator_name(GName), Node}, {set_and_unlock_log_complete, NLC}, ?UNLOCK_TIMEOUT) of
	ok -> 
	    ok;
	_ ->
	    set_and_unlock_log_complete(Node, GName, NLC)
    end.

unlock_log_complete_after_persisting(Node, GName, ExportedServerState) ->
    case catch gen_server:call({get_group_coordinator_name(GName), Node}, {unlock_log_complete_after_persisting, ExportedServerState}, ?UNLOCK_TIMEOUT) of
	ok -> 
	    ok;
	_ ->
	    unlock_log_complete_after_persisting(Node, GName, ExportedServerState)
    end.

unlock_log_complete(Node, GName) ->
    case catch gen_server:call({get_group_coordinator_name(GName), Node}, unlock_log_complete, ?UNLOCK_TIMEOUT) of
	ok -> 
	    ok;
	_ ->
	    unlock_log_complete(Node, GName)
    end.

lock_log_complete(Node, GName) ->
    case catch gen_server:call({get_group_coordinator_name(GName), Node}, {lock_log_complete, node()}, ?INTERCOMM_TIMEOUT) of
	busy ->
	    busy;
	ok ->
	    ok;
	_ ->
	    busy
    end.

append(Node, GName, Ref, V) when is_reference(Ref) ->
%    %lager:debug([{class, dike}], "trying to append  : ~p~n", [{Node, GName, V}]),
    case catch gen_server:call({get_group_coordinator_name(GName), Node}, {append, {Ref, self()}, V}, ?INTERCOMM_TIMEOUT) of
	ok ->
	    ok;
	{_Error, _Reason} ->
	    busy;
	busy ->
	    busy
    end;

append(Node, GName, cast, V) ->
    case catch gen_server:call({get_group_coordinator_name(GName), Node}, {append, {cast, self()}, V}, ?INTERCOMM_TIMEOUT) of
	ok ->
	    ok;
	{'EXIT', _Reason} ->
	    busy;
	busy ->
	    busy
    end.

append_no_reply([], GName, V) ->
    %lager:debug([{class, dike}], "failed to append_no_reply {group, val} = ~p~n", [{GName, V}]),
    error;

append_no_reply(Nodes, GName, V) when is_list(Nodes) ->
    try gen_server:call({get_group_coordinator_name(GName), hd(Nodes)}, {append, {cast, self()}, V}, ?INTERCOMM_TIMEOUT)
    catch
	_Error:_Reason ->
	    append_no_reply(tl(Nodes), GName, V)
    end.

newest_outcome(GName) ->
    gen_server:call(get_group_coordinator_name(GName), newest_outcome).

subscribe(GName) ->
    case catch gen_server:call(get_group_coordinator_name(GName), {subscribe, self()}, ?INTERCOMM_TIMEOUT) of 
	ok -> 
	    ok;
	ErrorReason -> 
	    %lager:debug([{class, dike}], "failed to subscribe for ~p on node ~p for ~p~n", [GName, node(), ErrorReason]),
	    subscribe(GName)
    end.

stop(GName) ->
    stop(node(), GName).

stop(Node, GName) ->
    gen_server:call({get_group_coordinator_name(GName), Node}, stop, ?INTERCOMM_TIMEOUT).
  
%%%===================================================================
%%% paxos_coordinator callbacks
%%%===================================================================

send(Node, S, Msg) ->
    gen_server:cast({get_group_coordinator_name(gname_from_subject(S)) , Node}, {relay, Msg}).

broadcast([], _, _) ->
    ok;

broadcast([H | Others], Subject, Msg) ->
    send(H, Subject, Msg),
    broadcast(Others, Subject, Msg).

callback({P, _Log}, Msg = {round_decided, {{_Grp, _Idx}, _N, _V}}) ->
    catch gen_server:call(P, Msg),
    ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([GroupName, GroupMembers, Locked, {DBMod, DBHandler}]) ->
    Position = dike_lib:position(GroupMembers, node()),
    OtherGroupMembers = GroupMembers -- [node()],
    Calls = ets:new(calls, [ordered_set, private, {keypos, 1}]),
    PaxosLog = ets:new(paxos_log, [ordered_set, public, {keypos, 1}]),
    
    {A, B, C} = now(), %% there may be better ways to initialize the random seed
    random:seed({A*Position, B+Position, C*Position}),

    IsLocked = log_complete_locked_p(Locked),

    %if IsLocked ->
	    %lager:debug([{class, dike}], "started gen_paxos in locked-mode for group ~p on node ~p~n~n", [GroupName, node()]);
    %   true ->
	    %lager:debug([{class, dike}], "started gen_paxos for group ~p on node ~p~n~n", [GroupName, node()])
    %end,
    
    {ok, #state{group_name=GroupName,
		position=Position,
		others=OtherGroupMembers,
		log=PaxosLog,
		calls=Calls,
		log_complete_locked=Locked,
		subscriber=nil,
		group_members=GroupMembers,
		new_persistence_variance=random:uniform(?PERSISTENCE_VARIANCE * 2),
		db_adapter={DBMod, DBHandler},
		members_persisted_at=lists:duplicate(5,0),
		log_cut=0}}.

handle_call({request_issued_ping, Ref, Pid}, _From, State=#state{group_name=GroupName, db_adapter={DBMod, DBProc}, calls = Calls, log_complete=LC, log_complete_locked=LCL}) ->
    case ets:match_object(Calls, {'_', {{Ref, Pid}, '_'}}) of
	[] ->
	    lager:info("request issued ping failing on instance: ~p, no call-entry found!", [{node(), GroupName} ]),
	    {reply, pang, State, ?UPDATE_LC_TIMEOUT};
	[{Idx, {{Ref, Pid}, _Req}} = Entry] ->
	    DecidedVal = DBMod:get(DBProc, generate_subject(State, Idx)),
	    %lager:info([{class, dike}], "in request_issued_ping: ~p, Calls entry: ~p, val in log ~p, IncLC val in log: ~p~n", [{LC, LCL}, Entry, DecidedVal, DBMod:get(DBProc, generate_subject(State, LC + 1))]),
	    {reply, pong, State, ?UPDATE_LC_TIMEOUT};
	List when is_list(List) ->
	    lager:info([{class, dike}], "Error! in request_issued_ping found to many entries! ~p~n", [List]),
	    {reply, pong, State, ?UPDATE_LC_TIMEOUT}
    end;

handle_call(ping, _, State) ->
    {reply, pong, State, ?UPDATE_LC_TIMEOUT};

handle_call(busy_ping, _From, State=#state{log_complete_locked=LCL}) when LCL == false ->
    %%    %lager:debug([{class, dike}], "in busy ping on ~p log_complete=~p, calls: ~p~n", [{node(), Group}, LC, ets:tab2list(Calls)]),
    {reply, pong, State, ?UPDATE_LC_TIMEOUT};

handle_call(busy_ping, _From, State=#state{log_complete_locked=_LCL}) ->
    {reply, busy, State, ?UPDATE_LC_TIMEOUT};

handle_call(stop, _From, State=#state{subscriber=Sub, group_name=GName}) ->
    gen_server:call(Sub, stop),
    paxos_registry:unregister(GName),
    {stop, normal, ok, State};

handle_call({set_and_unlock_log_complete, NLC}, From, State=#state{index=I, subscriber=Sub}) when Sub /= nil ->
    %lager:debug([{class, dike}], "unlocking log_complete after starting on node ~p, new_log_complete=~p~n", [node(), NLC]),    
    gen_server:reply(From, ok),
    NewState = update_log_complete(State#state{log_complete_locked=false, log_complete=NLC, index=max(NLC, I)}),
    parse_update_log_complete_resp(State, NewState, {noreply});

handle_call({unlock_log_complete_after_persisting, ExportedServerState}, From, State=#state{log_complete=LC, index=I, calls=Calls, log_cut=_LC, paxos_server_persisted=PSP, db_adapter={DBMod, DBProc}, subscriber=Sub}) when Sub /= nil ->
    gen_server:reply(From, ok),

    DBMod:update(DBProc, generate_subject(State, {persisted_state, LC}), ExportedServerState),
    DBMod:update(DBProc, generate_subject(State, {persisted_gen_paxos_state, LC}), State),
    DBMod:update(DBProc, generate_subject(State, persisted_at), LC),
    
    DBMod:bulk_delete(DBProc, [generate_subject(State, {persisted_state, PSP}), 
			       generate_subject(State, {persisted_gen_paxos_state, PSP})]),
    
    start_instance(State, I+1, {self(), {?SERVER_PERSISTED_TAG, node(), LC}}, active),
    ets:insert(Calls, {I + 1, {self(), {?SERVER_PERSISTED_TAG, node(), LC}}}),
    
    %lager:debug([{class, dike}], "unlocking log_complete after persisting on node ~p, log_complete=~p~n", [node(), LC]),
    NewState = update_log_complete(State#state{log_complete_locked=false, paxos_server_persisted=LC}),
    parse_update_log_complete_resp(State, NewState, {noreply});

handle_call(unlock_log_complete, From, State=#state{subscriber=Sub}) when Sub /= nil ->
    %lager:debug([{class, dike}], "simply unlocking log_complete on node ~p~n", [node()]),
    gen_server:reply(From, ok),
    NewState = update_log_complete(State#state{log_complete_locked=false}),
    parse_update_log_complete_resp(State, NewState, {noreply});

handle_call({lock_log_complete, NodeLocking}, From, State=#state{log_complete_locked={setting_up, _, NodeLocking}}) ->    
    {noreply, State#state{log_complete_locked={setting_up, From, NodeLocking}}, ?UPDATE_LC_TIMEOUT};

handle_call({lock_log_complete, NodeLocking}, _From, State=#state{log_complete_locked={true, NodeLocking}}) ->    
    {reply, ok, State};

handle_call({lock_log_complete, NodeLocking}, From, State=#state{log_complete_locked=LCL}) when LCL == false ->    
    case log_complete_lockable(State) of
	true ->
	    {reply, ok, State#state{log_complete_locked={true, NodeLocking}}, ?UPDATE_LC_TIMEOUT};
	false ->
	    {noreply, State#state{log_complete_locked={setting_up, From, NodeLocking}}, ?UPDATE_LC_TIMEOUT}
    end;

handle_call({lock_log_complete, _}, _From, State) ->
    {reply, busy, State, ?UPDATE_LC_TIMEOUT};

handle_call({append, From, V}, _From2, State= #state{index=I, calls=Calls, log_complete_locked=false}) ->
    ets:insert(Calls, {I + 1, {From, V}}),
    start_instance(State, I+1, {From, V}, active),
    {reply, ok, State#state{index=I+1}, ?UPDATE_LC_TIMEOUT};

handle_call({append, _, _}, _, State=#state{log_complete_locked=LCL}) when LCL /= false ->
    {reply, busy, State, ?UPDATE_LC_TIMEOUT};

handle_call({subscribe, PID}, _From, State=#state{subscriber=nil})  ->
    {reply, ok, State#state{subscriber=PID}, ?UPDATE_LC_TIMEOUT};

handle_call({round_decided, {{_Grp, Idx}, _N, _V}} , _From, State = #state{index=I, subscriber=Sub, group_name=_GName, paxos_server_persisted=PSP, log_complete=LC, log_complete_locked=LCL, new_persistence_variance=Variance}) when Idx =< I ->
%    %lager:debug([{class, dike}], "round decided on ~p Value: ~p", [{node(), GName}, _V]),
    Lockable = log_complete_lockable(State),
    CompLCL = log_complete_locked_p(LCL),

    S2=if LC - ?PERSISTENCE_INTERVAL -  ?PERSISTENCE_VARIANCE + Variance > PSP , LCL==false , Lockable ->
	       gen_server:cast(Sub, persist_state),
	       State#state{log_complete_locked={true, node()}, new_persistence_variance=random:uniform(?PERSISTENCE_VARIANCE * 2)}; %TODO: store gen_paxos state ... don't know at which point....
	  CompLCL ->
	       State;
	  true ->
	       update_log_complete(State)
       end,
    parse_update_log_complete_resp(State, S2, {reply, ok});

handle_call(Request, _From, State) ->
    %lager:debug([{class, dike}], "unhandled request in gen_paxos:handle_call on ~p req: ~p~nState: ~p~n", [node(), Request, State]),
    {reply, ok, State, ?UPDATE_LC_TIMEOUT}.


handle_cast({relay, {_, {S, _N, _V, From}} = Msg}, State=#state{group_members=Nodes}) ->
    case dike_lib:position(Nodes, From) of
	not_found ->
	    {noreply, State, ?UPDATE_LC_TIMEOUT};
	_Pos ->
	    relay(S, idx_from_subject(S), Msg, State)
    end;

%%
handle_cast({round_decided, {{_Grp, Idx}, _N, _V}}, State = #state{index=I}) when Idx =< I ->
    NewState = update_log_complete(State),
    parse_update_log_complete_resp(State, NewState, {noreply});


handle_cast({round_decided, {{_Node, _Grp, Idx}, _N, _V}}, State = #state{index=I}) when Idx =< I ->
    NewState = update_log_complete(State),
    parse_update_log_complete_resp(State, NewState, {noreply});

handle_cast(_Msg, State) ->
    lager:info([{class, dike}], "in gen_paxos, handle_cast, message unhandled:~p~n", [_Msg]),
    {noreply, State, ?UPDATE_LC_TIMEOUT}.

handle_info(timeout, State) ->
    NewState = update_log_complete(State),
    parse_update_log_complete_resp(State, NewState, {noreply});

handle_info(_Info, State) ->
    lager:info([{class, dike}], "in gen_paxos, handle_info called: ~p~n", [[_Info, State]]),
    {noreply, State, ?UPDATE_LC_TIMEOUT}.

terminate(_Reason, State=#state{db_adapter={DBMod, DBProc}}) ->
    %lager:debug([{class, dike}], "gen_paxos terminating on node ~p, ~p~n", [node(), [_Reason, State]]),
    case application:get_env(dike, db_mode) of
	{ok, per_group} ->
	    DBMod:stop(DBProc),
    	    ok;
	_ ->
	    ok
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_group_coordinator_name(GroupName) ->
    dike_lib:get_group_coordinator_name(GroupName).


start_instance(State, Idx, V) ->
    start_instance(State, Idx, V, passive).

start_instance(State=#state{others=O, position=P, log=L, db_adapter=DBAdapter}, Idx, V, Mode) ->
    S = generate_subject(State,Idx),
    case lookup_instance(S) of
	not_found -> 
	    {ok, Pid} = paxos_fsm:start(S, P, V, O, [{self(), L}], ?MODULE, DBAdapter, Mode),
	    paxos_registry:register(S,Pid),
	    {ok, Pid};
	Pid ->
	    {ok, Pid}
    end.

generate_subject(#state{group_name=Name}, Idx) ->
    case application:get_env(dike, db_mode) of
	{ok, per_machine} ->
	    {node(), Name, Idx};
	{ok, _} ->
	    {Name, Idx}
    end.

idx_from_subject({_Node, _GName, Idx}) ->
    Idx;
idx_from_subject({_GName, Idx}) ->
    Idx.

gname_from_subject({_Node, GName, _Idx}) ->
    GName;
gname_from_subject({GName, _Idx}) ->
    GName.




%%% answer with decide to requests that are in the decided part of the log

relay(S, Idx, {CMD, {_,_,_,From}}, State=#state{log_complete=LC, db_adapter={DBMod, DBProc}, group_name=_GName}) when Idx =< LC ->
    case CMD of
	decide ->
	    {noreply, State, ?UPDATE_LC_TIMEOUT};
	_ ->
	    case DBMod:get(DBProc, S) of
		{error, undefined} -> 
		    lager:debug([{class, dike}], "trying to answer with decide to an older round (< LC), found no Value!!~n",[]);
		{ok, {decided, LN, Val}} -> 
		    send(From, S, {decide, {S, LN, Val, node()}});
		{ok, {_PN, _N, _V}} ->
		    lager:debug([{class, dike}], "trying to answer with decide to an older round (< LC), found no Value!!~n",[])
	    end,
	    {noreply, State, ?UPDATE_LC_TIMEOUT}
    end;

%%% relay messages to the statemachine responsible/start the statemachine responsible in the unsafe part of the log

relay(S, Idx, {MType, {S,_,_, From}} = Msg, State = #state{index=I, log_complete=LC, db_adapter={DBMod, DBProc}, group_name=_GName}) when I >= Idx , Idx > LC->
    case DBMod:get(DBProc, generate_subject(State, Idx)) of
	{ok, {decided, LN, Val}} -> 
	    case MType of 
		decide ->
		    nil;
		_ ->
		    send(From, S, {decide, {S,LN, Val,node()}})
	    end,
	    {noreply, State, ?UPDATE_LC_TIMEOUT};
	{error, undefined} -> 
	    do_relay(S, Idx, Msg, State);
	{ok, {_PN, _N, _V}} ->
	    do_relay(S, Idx, Msg, State)
    end;

relay(S, Idx, {_, {_,_,_, _}} = Msg, State = #state{index=I, log_complete=LC}) when I < Idx , LC < Idx ->
    do_relay(S, Idx, Msg, State);

relay(S, _Idx, M, State) ->
    %lager:debug([{class, dike}], "received an unhandled relay-message: ~p~n", [[S, M, State]]),
    {noreply, State, ?UPDATE_LC_TIMEOUT}.



do_relay(S, Idx, {Cmd, {S, _N, V, _From}} = Msg, State = #state{index=I}) ->
    Pid = case lookup_instance(S) of 
	      not_found ->
		  case Cmd of
		      A when A == prepare ; A == propose ; A == decide ->
			  {ok, PID} = start_instance(State, Idx, V),
			  PID;
		      _ ->
			  {ok, PID} = start_instance(State, Idx, ?UNDECIDED, passive),
			  PID
		  end;
	      PID ->
		  PID
	  end,
    gen_fsm:send_event(Pid, Msg),
    {noreply, State#state{index=max(I, Idx)}, ?UPDATE_LC_TIMEOUT}.

lookup_instance(Subj) ->
    case paxos_registry:lookup(Subj) of
	[{Subj, PID}] ->
	    PID;
	[] ->
	    not_found;
	[H|T] ->
	    %lager:debug([{class, dike}], "looked up a paxos_fsm instance on node ~p and got  ~p~n", [node(), [H|T]]),
	    H
    end.

update_subscriber(Sub, Msg) ->
    try
	gen_server:call(Sub, Msg, ?UPDATE_SUBSCRIBER_TIMEOUT)
    catch
	Error:Reason ->
	    lager:error([{class, dike}], "Error! not able to update subscriber ~p on Node ~p~n", [{Error, Reason}, node()]),
	    update_subscriber(Sub, Msg)
    end.


%% updates log_complete which is the index of the log where we have no holes underneath so the action can be passed to the subscribers.
%% these updates can be blocked (mainly to transfer the subscribers state somewhere else) which may happen in cooperation with this f-n.
%% after unlocking this f-n updates the subscribers state to the actual hole-free point automatically (therefor the handle_calls timeout is used).

update_log_complete(State=#state{log_complete=LC, 
				 index=I, 
				 calls=Calls, 
				 db_adapter={DBMod, DBProc}
				}) when LC < I ->
    IncLC = LC + 1,
    DecidedVal = DBMod:get(DBProc, generate_subject(State, IncLC)),
    
    case ets:lookup(Calls, IncLC) of
	[{IncLC, {From, StoredVal}}] ->
	    update_log_complete_request_issued(State, node(), IncLC, {From, StoredVal}, DecidedVal);
	[] ->
	    update_log_complete_no_request_issued(State, node(), IncLC, DecidedVal)
    end;

update_log_complete(State=#state{}) ->
    State.



update_log_complete_request_issued(State=#state{calls=Calls,
						group_name=GName}, 
				   _Me,
				   IncLC, 
				   {From, StoredVal},
				   {ok, {decided, _N, {From, {?SERVER_PERSISTED_TAG, Any, RemotePersistedIdx} = StoredVal}}}) ->
    ets:delete(Calls, IncLC),
    %lager:debug([{class, dike}], "i (~p, ~p) persisted index ~p, got through at ~p~n", [node(), GName, RemotePersistedIdx, IncLC]),
    S2 = State#state{log_complete=IncLC},
    update_log_complete(update_and_cut_log(S2, Any, RemotePersistedIdx));

update_log_complete_request_issued(State=#state{index=I},
				   _Me,
				   IncLC,
				   {From, StoredVal},
				   {ok, {decided, _N, {_Other, {?SERVER_PERSISTED_TAG, Any, RemotePersistedIdx}}}}) ->
    reissue_request(State, From, StoredVal),
    S2 = State#state{log_complete=IncLC, index=I+1},				 
    update_log_complete(update_and_cut_log(S2, Any, RemotePersistedIdx));


update_log_complete_request_issued(State=#state{calls=Calls,
						paxos_server_persisted=PSP,
						group_members=Nodes,
						group_name=GName},
				   Me,
				   IncLC,
				   {_From, _StoredVal},
				   {ok, {decided, _N, {_Other, {?CHANGE_MEMBER_TAG, Me, New, NewsLC}}}}) ->
    ets:delete(Calls, IncLC), %% do something for requests we wanted to issue....
    if NewsLC >= PSP ->
	    NewMembers=dike_lib:replace(Me, New, Nodes),
	    dike_dispatcher:group_update(GName, NewMembers),
	    {replaced, IncLC};
       true ->
	    update_log_complete(State#state{log_complete=IncLC})
    end;

update_log_complete_request_issued(State=#state{index=I,
						calls=Calls,
						paxos_server_persisted=PSP,
						group_members=Nodes,
						group_name=GName},
				   Me,
				   IncLC,
				   {From, StoredVal},
				   {ok, {decided, _N, {Other, {?CHANGE_MEMBER_TAG, Old, New, NewsLC} = Val}}}) when Old=/=Me ->
    if NewsLC >= PSP ->
	    %lager:debug([{class, dike}], "on node ~p, ~p got replaced by node ~p -> 2~n", [node(), Old, New]),
	    NewMembers=dike_lib:replace(Old, New, Nodes),
	    Others= NewMembers -- [node()],
	    Pos=dike_lib:position(NewMembers, node()),
	    dike_dispatcher:group_update(GName, NewMembers),
	    S2 = case {Other, Val} of
		     {From, StoredVal} ->
			 ets:delete(Calls, IncLC),
			 State#state{group_members=NewMembers, others=Others, position=Pos, log_complete=IncLC};
		     _ ->
			 reissue_request(State, From, StoredVal),
			 S3 = State#state{group_members=NewMembers, others=Others, position=Pos, log_complete=IncLC},
			 S3#state{index=I+1}
		 end,
	    update_log_complete(S2);
       true ->
	    case {Other, Val} of
		{From, StoredVal} ->
		    ets:delete(Calls, IncLC);
		_ ->
		    reissue_request(State, From, StoredVal)
	    end,
	    update_log_complete(State#state{log_complete=IncLC})
    end;

update_log_complete_request_issued(State=#state{log_complete_locked=LCL,
						subscriber=Sub,
						calls=Calls},
				   _Me,
				   IncLC,
				   {From, StoredVal},
				   {ok, {decided, _N, {From, StoredVal}}}) ->
    %%	    %lager:debug([{class, dike}], "~p <- ~p finished round ~p with value ~p in paxos round ~p from ~p~n", [GName, node(), IncLC, StoredVal, N, From]),
    ets:delete(Calls, IncLC),
    update_subscriber(Sub, {paxos_update, From, IncLC, StoredVal, leader}),
    S2 = State#state{log_complete=IncLC},
    case LCL of
	false ->
		    update_log_complete(S2);
	{setting_up, LCL_From, LockingNode} ->
	    case log_complete_lockable(S2) of
		true ->
		    gen_server:reply(LCL_From, ok),
		    S2#state{log_complete_locked={true, LockingNode}};
		false ->
		    update_log_complete(S2)
	    end;
	{true, _} ->
	    %lager:debug([{class, dike}], "Error! in update_log_complete, i should be blocked but seem to have issued a request o.0~n", []),
	    update_log_complete(S2)
    end;


update_log_complete_request_issued(State=#state{index=I, 
						subscriber=Sub},
				   _Me,
				   IncLC,
				   {From, StoredVal},
				   {ok, {decided, _N, {Other, OtherVal}}}) ->
    reissue_request(State, From, StoredVal),
    update_subscriber(Sub, {paxos_update, Other, IncLC, OtherVal, follower}),
    update_log_complete(State#state{log_complete=IncLC, index=I+1});

update_log_complete_request_issued(State=#state{index=I},
				   _Me,
				   IncLC,
				   {From, StoredVal},
				   {ok, {decided, _N, ?UNDECIDED}}) ->
    reissue_request(State, From, StoredVal),
    update_log_complete(State#state{log_complete=IncLC, index=I+1});


update_log_complete_request_issued(State=#state{index=I},
				   _Me,
				   IncLC,
				   {From, StoredVal},
				   {error, undefined}) when IncLC < I ->
    case lookup_instance(generate_subject(State, IncLC)) of
	not_found ->
	    %lager:debug([{class, dike}], "Error! paxos instance not started locally although it should be~n", []),
	    {ok, _PID} = start_instance(State, IncLC, {From, StoredVal}, active);
	_ ->
	    nothing
    end,
    State;

update_log_complete_request_issued(State=#state{},
				   _Me,
				   _IncLC,
				   {_From, _StoredVal},
				   {error, undefined}) ->
    %% round may be started but has not logged to the db
    State;

update_log_complete_request_issued(State=#state{},
				   _Me,
				   _IncLC,
				   {_From, _StoredVal},
				   {ok, {PN, N, _V}}) when is_integer(PN), is_integer(N) ->
    %% this round has not finished yet, entry is from the running paxos instance
    State;
				       

update_log_complete_request_issued(State=#state{},
				   _Me,
				   _IncLC,
				   {_From, _StoredVal},
				   {ok, {PN, N, V}}) ->
    %lager:debug([{class, dike}], "Error! very bad paxos round outcome ~p~n", [{PN, N, V}]),
    State.


%% in this function, we did not issue a request for the log-position ourself.
update_log_complete_no_request_issued(State=#state{},
				      _Me,
				      IncLC,
				      {ok, {decided, _N, {_Other, {?SERVER_PERSISTED_TAG, Any, RemotePersistedIdx}}}}) ->
    update_log_complete(update_and_cut_log(State#state{log_complete=IncLC}, Any, RemotePersistedIdx));
	
update_log_complete_no_request_issued(State=#state{paxos_server_persisted=PSP, 
						   group_members=Nodes,
						   group_name=GName},
				      Me,
				      IncLC,
				      {ok, {decided, _N, {_From, {?CHANGE_MEMBER_TAG, Old, New, NewsLC}}}}) when Me =/= Old ->
    if NewsLC > PSP ->
	    NewMembers=dike_lib:replace(Old, New, Nodes),
	    Others= NewMembers -- [node()],
	    Pos=dike_lib:position(NewMembers, node()),
	    dike_dispatcher:group_update(GName , NewMembers),
	    update_log_complete(State#state{group_members=NewMembers, others=Others, position=Pos, log_complete=IncLC});
       true ->
	    update_log_complete(State#state{log_complete=IncLC})
    end;

update_log_complete_no_request_issued(State=#state{paxos_server_persisted=PSP, 
						   group_members=Nodes,
						   group_name=GName},
				      Me,
				      IncLC,
				      {ok, {decided, _N, {_From, {?CHANGE_MEMBER_TAG, Me, New, NewsLC}}}}) ->
    if NewsLC > PSP ->
	    %lager:debug([{class, dike}], "on node ~p, group ~p got replaced by node ~p~n", [node(), GName, New]),
	    NewMembers=dike_lib:replace(Me, New, Nodes),
	    dike_dispatcher:group_update(GName, NewMembers),
	    {replaced, IncLC};
       true ->
	    update_log_complete(State#state{log_complete=IncLC})
    end;

update_log_complete_no_request_issued(State=#state{paxos_server_persisted=PSP, 
						   group_members=Nodes,
						   group_name=GName},
				      Me,
				      IncLC,
				      {ok, {decided, _N, {_From, {?CHANGE_MEMBER_TAG, Me, New, NewsLC}}}}) ->
    if NewsLC > PSP ->
	    %lager:debug([{class, dike}], "on node ~p, group ~p got replaced by node ~p~n", [node(), GName, New]),
	    NewMembers=dike_lib:replace(Me, New, Nodes),
	    dike_dispatcher:group_update(GName, NewMembers),
	    {replaced, IncLC};
       true ->
	    update_log_complete(State#state{log_complete=IncLC})
    end;

update_log_complete_no_request_issued(State=#state{subscriber=Sub},
				      _Me,
				      IncLC,
				      {ok, {decided, _N, {From, Val}}}) ->
    update_subscriber(Sub, {paxos_update, From, IncLC, Val, follower}),
    update_log_complete(State#state{log_complete=IncLC});

update_log_complete_no_request_issued(State=#state{},
				      _Me,
				      IncLC,
				      {ok, {decided, _N, ?UNDECIDED}}) ->
    update_log_complete(State#state{log_complete=IncLC});

update_log_complete_no_request_issued(State=#state{index=I},
				      _Me,
				      IncLC,
				      {error, undefined}) when IncLC < I ->
    case lookup_instance(generate_subject(State, IncLC)) of
	not_found ->
	    {ok, _PID} = start_instance(State, IncLC, ?UNDECIDED, active);
	_ ->
	    nothing
    end,
    State;

update_log_complete_no_request_issued(State=#state{},
				      _Me,
				      _IncLC,
				      {error, undefined}) ->
    State;

update_log_complete_no_request_issued(State=#state{},
				      _Me,
				      _IncLC,
				      {ok, {PN, _N, _V}}) when PN =/= decided ->
    State.

    
log_complete_lockable(#state{log_complete=LC, calls=Calls}) ->
    case ets:last(Calls) of
	'$end_of_table' ->
	    true;
	A when A < LC ->
	    %lager:debug([{class, dike}], "locking log_complete, old calls remain...~n", []),
	    true;
	B when B >= LC ->
	    false
    end.

update_and_cut_log(State=#state{members_persisted_at=MPA,
				group_members=Members,
				group_name=GName,
				log_cut=LogCut,
				db_adapter={DBMod, DBProc}}, 
		   Node, 
		   PersistedAt) ->
    case dike_lib:position(Members,Node) of
	not_found ->
	    State;
	N ->
	    case dike_lib:replace_nth(MPA, N, PersistedAt) of
		not_found ->
		    
		    State;
		[H|T] ->
		    Min=lists:min([H|T]),
		    if Min > LogCut ->
			    %lager:debug([{class, dike}], "cut log for group ~p on ~p, new log_cut: ~p~n", [GName, node(), Min]),
			    DBMod:bulk_delete(DBProc, [generate_subject(State, Idx) || Idx <- lists:seq(LogCut + 1, Min)]),
			    %% DBMod:partial_apply(DBProc, 
			    %% 			generate_subject(State, LogCut),
			    %% 			fun({GName, I}, _V) when I < Min ->
			    %% 				delete;
			    %% 			   (_K,_V) ->
			    %% 				%lager:debug([{class, dike}], "seeing a key i should not ~p@~p: (~p,~p)~n", [GName, node(), _K, _V]),
			    %% 				nothing
			    %% 			end,
			    %% 			fun({_, M}, _V) when M == Min->
			    %% 				true;
			    %% 			   (_,_) ->
			    %% 				false 
			    %% 			end),
			    State#state{members_persisted_at=[H|T],
					log_cut=Min};
		       true ->
			    State#state{members_persisted_at=[H|T]}
		    end
	    end
    end.

init_db(GroupName) ->
    
    {ok, DBAdapter} = application:get_env(dike, db_adapter),
    case application:get_env(dike, db_mode) of
	{ok, V} when V==per_vm ; V==per_machine ->
	    {DBAdapter, ?DB_TRANSACTION_HANDLER};
	{ok, per_group} ->
	    {ok, DBFolder} = application:get_env(dike, db_dir),
	    DBFolder2 = DBFolder ++ "dikedb_" ++ atom_to_list(GroupName) ++ "_" ++ atom_to_list(node()),
	    {ok, Pid} = DBAdapter:open(DBFolder2),
	    {DBAdapter, Pid}
    end.




find_node_with_state(Nodes, G, M) ->
    find_node_with_state(Nodes, node(), G, M, Nodes).

find_node_with_state([], _Node, _G, _M, _AN) ->
    %lager:debug([{class, dike}], "found no node with valid state~n", []),
    error_empty;
find_node_with_state([Node|T], Node, G, M, AN) ->
    find_node_with_state(T, Node, G, M, AN);
find_node_with_state([H|T], Node, G, M, AN) ->
    case  rpc:call(H, dike_dispatcher, check_remote_group, [G, M]) of 
	pong -> 
	    %lager:debug([{class, dike}], "found node with valid state ~p~n", [H]),
	    H;
	_H ->
	    %lager:debug([{class, dike}], "found invalid node ~p~n", [[H,_H]]),
	    find_node_with_state(T, Node, G, M, AN)
    end.

check_calls_stopping(#state{calls=Calls}, _Idx) ->
    ets:foldl(fun(E, Acc) ->
		      %lager:debug([{class, dike}], "Error! found a call in stopping gen_paxos: ~p~n", [E]),
		      Acc
	      end,
	      0,
	      Calls).

reissue_request(State=#state{index=I, calls=Calls, log_complete=LC}, 
		From, 
		StoredVal) ->
    IncLC = LC + 1,
    ets:delete(Calls, IncLC),
    ets:insert(Calls, {I + 1, {From, StoredVal}}),
    {ok, _PID} = start_instance(State, I+1, {From, StoredVal}, active).
    
parse_update_log_complete_resp(State = #state{index=_I, subscriber=Sub, group_name=GName}, {replaced, Idx}, _) ->
    check_calls_stopping(State, Idx),
    gen_server:call(Sub, stop),
    paxos_registry:unregister(GName),
    {stop, normal, ok, State};

parse_update_log_complete_resp(_, NewState, {reply, RVal}) ->
    {reply, RVal, NewState, ?UPDATE_LC_TIMEOUT};

parse_update_log_complete_resp(_, NewState, {noreply}) ->
    {noreply, NewState, ?UPDATE_LC_TIMEOUT}.


log_complete_locked_p({true, _}) ->
    true;

log_complete_locked_p({setting_up, _ , _}) ->
    false;

log_complete_locked_p(_) ->
    false.
