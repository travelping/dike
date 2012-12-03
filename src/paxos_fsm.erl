%    __                        __      _
%   / /__________ __   _____  / /___  (_)___  ____ _
%  / __/ ___/ __ `/ | / / _ \/ / __ \/ / __ \/ __ `/
% / /_/ /  / /_/ /| |/ /  __/ / /_/ / / / / / /_/ /
% \__/_/   \__,_/ |___/\___/_/ .___/_/_/ /_/\__, /
%                           /_/            /____/
%
% Copyright (c) Travelping GmbH <info@travelping.com>

-module(paxos_fsm).

-behaviour(gen_fsm).

-include_lib("include/paxos_lib.hrl").

-export([version_info/0]).

%% @doc functions for users.
-export([start/7, start/8, stop/1, get_result/1, behaviour_info/1]).

%% @doc functions for gen_fsm.
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).

%% @doc states of FSM.
-export([nil/2, preparing/2, proposing/2, acceptor/2,
         learner/2, decided/2]).

-define( DEFAULT_TIMEOUT, timer:seconds(20)).
-define( DECIDED_TIMEOUT, 1).
-define( VALID_VOTE(StateData, S, N), StateData#paxos_fsm_state.subject == S, StateData#paxos_fsm_state.prepared_n > N).

behaviour_info(callbacks) ->
    [{send, 3}, {broadcast, 3}, {callback, 2}];
behaviour_info(_) ->
    undefinded.

version_info()-> {?MODULE, 1}.

%% @doc   Users call this function. Initializes PAXOS FSM.
%%        subject_identifier - subject name. this names process, unless paxos_fsm can't find others.
%%        n()                - paxos_fsm agent id. this must be unique in the paxos_fsm group.
%%                             no more than length of players.
%%        any()              - value to suggest (prepare, propose).
%%        other_players()    - member list of paxos_fsm group, which consists of agents, except oneself.
%%        return_pids()       - when consensus got decided, result is to be sent to return_pid().
%%
%% @spec  start( subject_identifier(), n(), any(), other_players(), return_pid() ) -> Result
%%    Result = {ok, Pid} | ignore | { error, Error }
%%    Error  = {already_started, Pid } | term()
%%    Pid = pid()
%%

start(S, InitN, V, Others, ReturnPids, Module, DBAdapter) ->
    start(S, InitN, V, Others, ReturnPids, Module, DBAdapter, passive).

start(S, InitN, V, Others, ReturnPids, Module, {_Module, _Proc} = DBAdapter, Mode) ->
    All = length(Others)+1,    
    Quorum = All / 2 ,
    DefaultInitStateData = #paxos_fsm_state{ subject=S, value=V,
					     all=All, quorum=Quorum, others=Others, init_n=InitN,
					     return_pids=ReturnPids,
					     coordinator_module=Module, 
					     n=0,
					     db_adapter=DBAdapter},
    {StateData, BroadcastMsg} = alter_state_for_mode(DefaultInitStateData, Mode),

    db_update(StateData),
    {ok, Pid} = gen_fsm:start_link(
		  ?MODULE,                        %Module
		  [StateData, Mode],                %Args
		  []),
    if BroadcastMsg =/= none ->
	    broadcast(Others, StateData, BroadcastMsg);
       true ->
	    nothing
    end,
    {ok, Pid}.

alter_state_for_mode(State=#paxos_fsm_state{subject=S, value=V, all=All, init_n=InitN}, active) ->
    NewN = get_next_n( 0, All, InitN),
    {State#paxos_fsm_state{prepared_n=NewN, current=1},
     {prepare, {S, NewN,V, node()}}};

alter_state_for_mode(State, _) ->
    {State, none}.

stop(S) -> 
    gen_fsm:send_all_state_event( generate_global_address( node(),S ), stop).

get_result(S)->
    gen_fsm:sync_send_all_state_event( generate_global_address( node(),S ), result).


%% ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ %%
%%   codes below are for gen_fsm. users don't need.       %%
%% ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ %%

init([StateData, Mode])->
    F=fun(passive) ->
	      nil;
	 (active) ->
	      preparing
      end,
    process_flag(trap_exit, true),
    {ok,
     F(Mode),  
     StateData,
     ?DEFAULT_TIMEOUT};
init(V) ->
    lager:info([{class, dike}], "in wrong init...!!!! -> ~p~n", [V]).

broadcast(Others, #paxos_fsm_state{subject=S, coordinator_module=Mod}, Message)->
    Mod:broadcast(Others, S, Message).

send(Node, #paxos_fsm_state{subject=S, coordinator_module=Mod}, Message)->
    Mod:send(Node, S, Message).

%% @doc send back message to originator to share the consensus value.
callback(StateData=#paxos_fsm_state{subject=S, value=V, n=N}, V, ReturnPids)->
    Mod  = StateData#paxos_fsm_state.coordinator_module,
    [Mod:callback(ReturnPid, {round_decided, {S, N, V}}) || ReturnPid <- ReturnPids].

get_next_n( PN , All , InitN) -> (( PN div All )+1) * All + InitN.

generate_global_address( Node, Subject )->  {global, {?MODULE, Node, Subject}}.
%%generate_local_address( Node, Subject )->  {local, {?MODULE, Node, Subject}}.


    %% =========================================
%% states:
%%  - nil
%%  - preparing
%%  - proposing
%%  - acceptor
%%  - learner
%%  - decided
%% events: n' < n < n''...
%%  - {prepare, {S, N, V}}
%%  - {prepare_result, {S, N, V}}
%%  - propose
%%  - propose_result
%%  - timeout
%%  - decide
%% Data:
%%  - {{Sc, Nc, Vc}, {All, Quorum, Current, Others, InitN}}

%% =========================================
%%  - nil ( master lease time out )


nil( {prepare,  {S, N, V, From}}, StateData) 
  when N > StateData#paxos_fsm_state.prepared_n , S == StateData#paxos_fsm_state.subject ->
						%    %lager:debug([{class, dike}], "paxos_fsm ~p received prepare for new round on node ~p~n", [self(), node()]),
    NV = case V of
	     V when V=/=?UNDECIDED , StateData#paxos_fsm_state.value==?UNDECIDED , StateData#paxos_fsm_state.n==0 , StateData#paxos_fsm_state.prepared_n==0->
		 V;
	     _ ->
		 StateData#paxos_fsm_state.value
	 end,	 
    higher_round_started_go_acceptor(From, N, StateData#paxos_fsm_state{value=NV});

nil( {prepare,  {S, N, _V, _From}}, StateData) 
  when N < StateData#paxos_fsm_state.prepared_n , S == StateData#paxos_fsm_state.subject ->
    start_new_round(StateData);
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%
%%%%

nil( {decide,  {S, _, _, _}} = Msg, StateData ) when  S==StateData#paxos_fsm_state.subject->
    decided_callback( StateData#paxos_fsm_state{}, Msg);

nil( timeout, StateData )-> 
    start_new_round(StateData);

nil(UnknownEvent, StateData)-> % ignore
    lager:debug([{class, dike}],  "unknown event: ~p : all ignored.~n", [{UnknownEvent, StateData}] ),
    {next_state, nil, StateData, ?DEFAULT_TIMEOUT}.


%% =========================================
%%  - preparing

preparing( {prepare,  {S, N, _V, From}},  StateData ) 
  when N > StateData#paxos_fsm_state.prepared_n, StateData#paxos_fsm_state.subject==S  ->
    higher_round_started_go_acceptor(From, N, StateData);

preparing( {prepare_result,  {S, PN, {N, V}, _From}}, StateData )
  when PN == StateData#paxos_fsm_state.prepared_n , S == StateData#paxos_fsm_state.subject, StateData#paxos_fsm_state.current + 1 > StateData#paxos_fsm_state.quorum ->
    NV = if N > StateData#paxos_fsm_state.n ->
		 V;
	    true ->
		 StateData#paxos_fsm_state.value
	 end,
    %lager:debug([{class, dike}], "got quorum at prepare!, sending out: ~p~n", [[StateData#paxos_fsm_state.others, S, {propose, {S, PN, NV, node()}} ]]),
    broadcast( StateData#paxos_fsm_state.others, StateData, {propose, {S, PN, NV, node()}} ),
						%           lager:info([{class, dike}], "proposing ~p...~n", [{propose, {S,Nc,Vc,node()}}]),
    db_update(StateData, {PN, StateData#paxos_fsm_state.n, V}),
    {next_state, proposing, StateData#paxos_fsm_state{current=1, n=PN, value=NV}, ?DEFAULT_TIMEOUT};



preparing( {prepare_result,  {S, PN, {N, V}, _From}}, StateData )
  when PN == StateData#paxos_fsm_state.prepared_n , S == StateData#paxos_fsm_state.subject  ->
    %lager:debug([{class, dike}], "received a vote while preparing.", []),
    Current = StateData#paxos_fsm_state.current,
    {NN, NV} = if N > StateData#paxos_fsm_state.n ->
		       {N, V};
		  true ->
		       {StateData#paxos_fsm_state.n, StateData#paxos_fsm_state.value}
	       end,
    {next_state, preparing, StateData#paxos_fsm_state{current=Current+1, n=NN, value=NV}, ?DEFAULT_TIMEOUT};

preparing( {decide,  {S, _, _, _}} = Msg, StateData ) when  S==StateData#paxos_fsm_state.subject->
    decided_callback( StateData#paxos_fsm_state{}, Msg);

preparing( timeout, StateData)-> 
    start_new_round(StateData);

preparing( _Event, StateData) ->
    lager:debug([{class, dike}], "received unhandled message while preparing: ~p~n", [{_Event, StateData}]),
    {next_state, preparing, StateData, ?DEFAULT_TIMEOUT}.

%% =========================================
%%  - proposing
%% =========================================
proposing( {prepare,  {S, N, _V, From}},  StateData) 
  when S == StateData#paxos_fsm_state.subject , N > StateData#paxos_fsm_state.prepared_n ->  %a newer round has been started, somebody didn't get the memo TODO
    higher_round_started_go_acceptor(From, N, StateData);

proposing( {propose_result,  {S, N, V, _From}}, StateData)
  when N==StateData#paxos_fsm_state.n, StateData#paxos_fsm_state.quorum > StateData#paxos_fsm_state.current+1 , S == StateData#paxos_fsm_state.subject ->
    %lager:debug([{class, dike}], "received a vote while proposing ~p~n", [{S,N,V}]),
    Current = StateData#paxos_fsm_state.current,
    {next_state, proposing, StateData#paxos_fsm_state{current=Current+1}, ?DEFAULT_TIMEOUT };

proposing( {propose_result,  {S, N, _V, _From}}, StateData) 
  when N==StateData#paxos_fsm_state.n, StateData#paxos_fsm_state.quorum < StateData#paxos_fsm_state.current+1 , S == StateData#paxos_fsm_state.subject ->
    %lager:debug([{class, dike}], "got quorum at proposing!!~n", []),
    broadcast( StateData#paxos_fsm_state.others, StateData, {decide, {S, N, StateData#paxos_fsm_state.value, node()}} ),
    Current=StateData#paxos_fsm_state.current,
    decided_callback( StateData#paxos_fsm_state{current=Current+1}, {propose_result,  {S, N, StateData#paxos_fsm_state.value, _From}});

proposing( {decide,  {S, _, _, _}} = Msg, StateData ) when  S==StateData#paxos_fsm_state.subject->
    decided_callback( StateData#paxos_fsm_state{}, Msg);

proposing( timeout, StateData)-> 
    start_new_round(StateData);

proposing( {prepare_result, {S, N, _V, _From}}, StateData) when StateData#paxos_fsm_state.prepared_n == N ->
    %% ignore
    {next_state, proposing, StateData};

proposing( _Event, StateData) ->
    lager:debug([{class, dike}], "received unhandled message while proposing: ~p~n", [{_Event, StateData}]),
    {next_state, proposing, StateData, ?DEFAULT_TIMEOUT}.


%% =========================================
%%  - acceptor
%% =========================================

acceptor( {prepare,  {S, N, _V, From}},  StateData ) 
  when N > StateData#paxos_fsm_state.prepared_n , StateData#paxos_fsm_state.subject == S ->
    higher_round_started_go_acceptor(From, N, StateData);

acceptor( {propose,  {S, N, V, From}},  StateData) 
  when N == StateData#paxos_fsm_state.prepared_n, StateData#paxos_fsm_state.subject==S -> 
    db_update(StateData, {N, N, V}),
    send( From, StateData, {propose_result , {S, N, [], node() }} ),
    {next_state, learner, StateData#paxos_fsm_state{n=N, value=V, prepared_n=N}, ?DEFAULT_TIMEOUT};

acceptor( {decide,  {S, _, _, _}} = Msg, StateData ) when  S==StateData#paxos_fsm_state.subject->
    decided_callback( StateData#paxos_fsm_state{}, Msg);

acceptor( timeout, StateData=#paxos_fsm_state{}) -> 
    start_new_round(StateData);

acceptor( _Event, StateData) ->
    lager:debug([{class, dike}], "acceptor unknown event: ~p~n" , [{_Event , StateData}]),
    {next_state, acceptor, StateData, ?DEFAULT_TIMEOUT}.

%% =========================================
%%  - learner
%% =========================================

learner( {prepare,  {S, N, _V, From}}, StateData) when N > StateData#paxos_fsm_state.prepared_n , StateData#paxos_fsm_state.subject==S-> 
    higher_round_started_go_acceptor(From, N, StateData);  

learner( {decide,  {S, _, _, _}} = Msg, StateData ) when  S==StateData#paxos_fsm_state.subject->
    decided_callback( StateData#paxos_fsm_state{}, Msg);

learner( timeout, StateData=#paxos_fsm_state{}) -> 
    start_new_round(StateData);

learner( _Event, StateData )->
    lager:debug([{class, dike}], "learner unknown event: ~p ,~p~n" , [{_Event , StateData}]),
    {next_state, learner, StateData, ?DEFAULT_TIMEOUT }.

%% =========================================
%%  - decided ( within master lease time )
%% =========================================

decided( {decide, {S, _, _, _}}, StateData) when S == StateData#paxos_fsm_state.subject ->
    {next_state, decided, StateData, ?DECIDED_TIMEOUT };

decided( {_Message, {S,_N,_V, From}}, StateData) when S == StateData#paxos_fsm_state.subject->
    send( From, StateData, {decide, {S,StateData#paxos_fsm_state.n, StateData#paxos_fsm_state.value,node()}} ),
    {next_state, decided, StateData, ?DECIDED_TIMEOUT };

decided( {_Message, {S,_N, From}}, StateData) when S == StateData#paxos_fsm_state.subject ->
    send( From, StateData, {decide, {S,StateData#paxos_fsm_state.n, StateData#paxos_fsm_state.value,node()}} ),
    {next_state, decided, StateData, ?DECIDED_TIMEOUT};

decided( timeout,  StateData )->
    {stop, normal, StateData };

decided( _Event, StateData )->
    lager:debug([{class, dike}], "decided unknown event: ~p ~n" , [{_Event , StateData}]),
    {next_state, decided, StateData , ?DECIDED_TIMEOUT}.

%% @doc must be called back whenever the subject got consensus!
decided_callback(StateData, {decide, {S, N, V, _From} = Params}) 
  when N =/= -1, N < StateData#paxos_fsm_state.n , V /= StateData#paxos_fsm_state.value , S == StateData#paxos_fsm_state.subject ->
    %lager:debug([{class, dike}], "error, error, got a decided callback on the wrong value!~p~n",[[Params, StateData]]),
    decided_callback(StateData,{propose_result, Params});    

decided_callback(StateData, {_CMD, {S, N, V, _From}})
  when S==StateData#paxos_fsm_state.subject ->
    db_update(StateData, {decided, N, V}),
    StateData2 = StateData#paxos_fsm_state{n=N, prepared_n=N, value=V},
    callback(StateData2, V, StateData2#paxos_fsm_state.return_pids ),
    {next_state, decided, StateData2, ?DECIDED_TIMEOUT}.


code_change(_,_,_,_)->
    ok.

handle_event( stop, _StateName, StateData )->
    {stop, normal, StateData}.

handle_info({'EXIT', _PID, _Reason}, State, StateData)->
    {next_state, State, StateData};

handle_info(_INFO, State, StateData)->
    {next_state, State, StateData}.

handle_sync_event(result, _From, StateName, StateData)->
    {reply, {StateName, StateName#paxos_fsm_state.value}  , StateName, StateData};

handle_sync_event(stop, From, StateName, StateData)->
    {stop, From, StateName, StateData}.

terminate(R, decided, _StateData) when R == normal ; R== killed ; R==shutdown ->
    ok;

terminate(Reason, StateName, StateData) ->
    %lager:debug([{class, dike}], "paxos_fsm terminating on node ~p , ~p~n", [node(), [Reason, StateName, StateData]]),
    ok.

start_new_round(StateData) ->
    NewN = get_next_n( StateData#paxos_fsm_state.prepared_n, StateData#paxos_fsm_state.all, StateData#paxos_fsm_state.init_n),
    db_update(StateData, {NewN, StateData#paxos_fsm_state.n, StateData#paxos_fsm_state.value}),
    S=StateData#paxos_fsm_state.subject,
    V=StateData#paxos_fsm_state.value,
    %lager:debug([{class, dike}], "Starting new Paxos round on ~p, {S,N,V} ~p~n", [node(), {S, NewN, V}]),
    broadcast( StateData#paxos_fsm_state.others, StateData, {prepare, {S, NewN, ?UNDECIDED, node()}} ),
    {next_state, preparing, StateData#paxos_fsm_state{current=1, prepared_n=NewN}, ?DEFAULT_TIMEOUT}.

higher_round_started_go_acceptor( From, N,  StateData=#paxos_fsm_state{} ) ->
    db_update(StateData, {N, StateData#paxos_fsm_state.n, StateData#paxos_fsm_state.value}),
    send( From, StateData, {prepare_result, {StateData#paxos_fsm_state.subject, N, {StateData#paxos_fsm_state.n, StateData#paxos_fsm_state.value}, node()}}),
    %lager:debug([{class, dike}], "received prepare for higher round, going acceptor...old-round:~p new-round:~p~n", [StateData#paxos_fsm_state.prepared_n, N]),
    {next_state, acceptor, StateData#paxos_fsm_state{prepared_n=N}, ?DEFAULT_TIMEOUT}.


db_update(StateData=#paxos_fsm_state{prepared_n=PN, n=N, value=V}) ->
    db_update(StateData, {PN, N, V}).

db_update(#paxos_fsm_state{db_adapter={DBModule, DBProc},
				    subject=S}, Msg) ->
    DBModule:update(DBProc, S, Msg).

