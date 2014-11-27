%    __                        __      _
%   / /__________ __   _____  / /___  (_)___  ____ _
%  / __/ ___/ __ `/ | / / _ \/ / __ \/ / __ \/ __ `/
% / /_/ /  / /_/ /| |/ /  __/ / /_/ / / / / / /_/ /
% \__/_/   \__,_/ |___/\___/_/ .___/_/_/ /_/\__, /
%                           /_/            /____/
%
% Copyright (c) Travelping GmbH <info@travelping.com>

-module(paxos_server).

-include_lib("dike.hrl").

-behaviour(gen_server).

%% API
-export([start_link/2,
	 start_link/3,
	 start_link/5,
	 call/3,
	 cast/3,
	 stop/1,
	 behaviour_info/1,
	 reply/2,
	 ping/3,
	 generate_paxos_server_name/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

%% share the code with stub_paxos
-export([client_handle_call/5]).

behaviour_info(callbacks) ->
    [{handle_call, 3},
     {init, 1},
     {init, 2},
     {export_state, 1}];

behaviour_info(_) ->
    undefinded.

-define(SERVER, ?MODULE).
-define(MAX_RECEIVE_RETRIES, 3).
-define(LOG_UPDATE, '$paxos_log_update-').


-record(state, {group,
		module,
		client_state,
		state_log_pos=-1}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Group, Module, Nodes, CopyStateFrom, Replace) ->
    gen_server:start_link({local, generate_paxos_server_name(Group, Module)}, ?MODULE, [Group,Module, Nodes, CopyStateFrom, Replace], []),
    {Group, Module}.

start_link(PaxosGroup, Module, CopyStateFrom) ->
    gen_server:start_link({local, generate_paxos_server_name(PaxosGroup, Module)}, ?MODULE, [PaxosGroup,Module, CopyStateFrom], []),
    {PaxosGroup, Module}.


start_link(PaxosGroup, Module) ->
    gen_server:start_link({local, generate_paxos_server_name(PaxosGroup, Module)}, ?MODULE, [PaxosGroup,Module], []),
    {PaxosGroup, Module}.

ping(Node, GName, Module) ->
    case catch gen_server:call({generate_paxos_server_name(GName, Module), Node}, ping, ?PING_TIMEOUT) of
	{'EXIT', _Reason} ->
	    pang;
	pong ->
	    pong;
	_R ->
	    pang
    end.

cast(AllNodes, [], PGroup, Val) ->
    AllNodes2 = case PGroup of
		    master ->
			AllNodes;
		    _ ->
			dike_dispatcher:get_nodes(PGroup)
		end,
    cast(AllNodes2, AllNodes2, PGroup, Val);

cast(AllNodes, Nodes, PGroup, Val) ->
    Node = lists:nth(random:uniform(length(Nodes)), Nodes),
    case cast_helper(Node, PGroup, Val, false) of
	busy ->
	    cast(AllNodes, Nodes -- [Node], PGroup, Val);
	RV ->
	    RV
    end.

% this cast tries all nodes of a group in random order if nodes are busy
cast(Nodes, PaxosGroup, Msg) when is_list(Nodes) ->
    cast(Nodes, Nodes, PaxosGroup, Msg);

% this cast retries the same server until it is not busy anymore
cast(Node, PaxosGroup, Msg) when not is_list(Node) ->
    cast_helper(Node, PaxosGroup, Msg, true).

% used to call a single server, if retry is true will retry this server until not busy anymore,
% otherwise will return busy
cast_helper(Node, PaxosGroup, Msg, Retry) ->
    case gen_paxos:append(Node, PaxosGroup, cast, Msg) of
	busy ->
	    if Retry==true ->
		    timer:sleep(?CLIENT_RETRY_INTERVAL),
		    cast_helper(Node, PaxosGroup, Msg, Retry);
	       true ->
		    busy
	    end;
	ok ->
	    ok
    end.

call(AllNodes, [], PGroup, Val) ->
    AllNodes2 = case PGroup of
                    master ->
                        AllNodes;
                    _ ->
                        dike_dispatcher:get_nodes(PGroup)
                end,
    call(AllNodes2, AllNodes2, PGroup, Val);

call(AllNodes, Nodes, PGroup, Val) ->
    Node = lists:nth(random:uniform(length(Nodes)), Nodes),
    case call_helper(Node, PGroup, Val, false) of
        busy ->
            call(AllNodes, Nodes -- [Node], PGroup, Val);
        RV ->
            RV
    end.

% this call tries all nodes of a group in random order if nodes are busy
call(Nodes, PaxosGroup, Msg) when is_list(Nodes) ->
    call(Nodes, Nodes, PaxosGroup, Msg);

% this call retries the same server until it is not busy anymore
call(Node, PaxosGroup, Msg) when not is_list(Node) ->
    call_helper(Node, PaxosGroup, Msg, true).

% used to call a single server, if retry is true will retry this server until not busy anymore,
% otherwise will return busy
call_helper(Node, PaxosGroup, Msg, Retry) ->
    Ref=make_ref(),
    case gen_paxos:append(Node, PaxosGroup, Ref, Msg) of
        busy ->
            if Retry==true ->
                   timer:sleep(?CLIENT_RETRY_INTERVAL),
                   call_helper(Node, PaxosGroup, Msg, Retry);
               true ->
                   busy
            end;
        ok ->
            receive_helper(Ref, PaxosGroup, Node, Msg)
    end.

stop({PaxosGroup, Module}) ->
    gen_server:call(generate_paxos_server_name(PaxosGroup, Module), stop),
    gen_paxos:stop(PaxosGroup).

reply({single_node_reply, From}, Msg) ->
    gen_server:reply(From, Msg);
reply(cast, _Msg) ->
    ok;
reply({Ref, From}, Msg) ->
    From ! {?LOG_UPDATE, Ref, Msg};
reply(A, B) ->
    lager:debug([{class, dike}], "in paxos_server:reply, badargs: ~p, ~p", [A, B]).


export_state(PaxosGroup, Module, CopyStateFrom) ->
    case catch gen_server:call({generate_paxos_server_name(PaxosGroup, Module), CopyStateFrom}, export_state) of
	{_SLP, _State} = V ->
	    V;
	_ ->
	    export_state(PaxosGroup, Module, CopyStateFrom)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([PaxosGroup, Module, Nodes, CopyStateFrom, Replace]) ->
    case gen_paxos:lock_log_complete(CopyStateFrom, PaxosGroup) of
	ok ->
	    {SLP, State} = export_state(PaxosGroup, Module, CopyStateFrom),
	    if CopyStateFrom =/= Replace ->
		    gen_paxos:unlock_log_complete(CopyStateFrom, PaxosGroup),
		    gen_paxos:lock_log_complete(Replace, PaxosGroup);
	       true ->
		    nothing
	    end,
	    UnaffectedMembers = Nodes -- [node(), Replace],
	    gen_paxos:append_no_reply(UnaffectedMembers, PaxosGroup, {?CHANGE_MEMBER_TAG, Replace, node(), SLP}),
	    {ok, ClientState} = Module:init(State, [{paxos_group, PaxosGroup}]),
	    ok=gen_paxos:unlock_log_complete(Replace, PaxosGroup),
	    gen_paxos:subscribe(PaxosGroup),
	    ok=gen_paxos:set_and_unlock_log_complete(node(), PaxosGroup, SLP),
	    {ok, #state{group=PaxosGroup,
			module=Module,
			client_state=ClientState,
			state_log_pos=SLP}};
	_ ->
	    init([PaxosGroup, Module, Nodes, CopyStateFrom, Replace])
    end;

init([PaxosGroup, Module, CopyStateFrom]) ->
    case gen_paxos:lock_log_complete(CopyStateFrom, PaxosGroup) of
	ok ->
	    {SLP, State} = export_state(PaxosGroup, Module, CopyStateFrom),
	    {ok, ClientState} = Module:init(State, [{paxos_group, PaxosGroup}]),
	    gen_paxos:unlock_log_complete(CopyStateFrom, PaxosGroup),
	    gen_paxos:subscribe(PaxosGroup),
	    ok=gen_paxos:set_and_unlock_log_complete(node(), PaxosGroup, SLP),
	    {ok, #state{group=PaxosGroup,
			module=Module,
			client_state=ClientState,
			state_log_pos=SLP}};
	_ ->
	    init([PaxosGroup, Module, CopyStateFrom])

    end;

init([PaxosGroup, Module]) ->
    gen_paxos:subscribe(PaxosGroup),
    {ok, ClientState} = Module:init([{paxos_group, PaxosGroup}]),
    gen_paxos:unlock_log_complete(node(), PaxosGroup),
    {ok, #state{group=PaxosGroup,
		module=Module,
		client_state=ClientState}}.

handle_call(ping, _, State) ->
    {reply, pong, State};

handle_call(export_state, _From, State=#state{module=Module, client_state=CState, state_log_pos=SLP}) ->
    ExportableState = Module:export_state(CState),
    {reply, {SLP, ExportableState}, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call({paxos_update, From, IncLC, Request, Mode}, _From, State=#state{module=Module, client_state=CState, state_log_pos=SLP}) ->
    SLP2 = if IncLC > SLP -> %% this is because rounds to control the group topology may interrupt a straight linear order
                   dike_lib:maybe_garbage_collect(SLP, IncLC),
		   IncLC;
	      true ->
                   dike_lib:maybe_garbage_collect(SLP),
		   SLP
	   end,
    CS2 = client_handle_call(Module, Request, From, CState, Mode),
    {reply, ok, State#state{client_state=CS2, state_log_pos=SLP2}};

handle_call(_Msg, _From, State) ->
    {reply, ignored, State}.

handle_cast(persist_state,  State=#state{group=Group, module=Module, client_state=CState}) ->
    ExportedCState = Module:export_state(CState),
    %% todo: call persistence module (ExportedCState)
    gen_paxos:unlock_log_complete_after_persisting(node(), Group, ExportedCState),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

client_handle_call(Module, Request, From, CState, Mode) ->
    try Module:handle_call(Request, From, CState) of
        {reply, ReplyFN, CState2} ->
            (Mode == leader) andalso proc_lib:spawn(fun() -> client_reply(Request, ReplyFN) end),
            CState2;
        {noreply, CState2} ->
            CState2;
        V ->
            lager:warning([{class, dike}], "Application returned unsupported value: ~p~n", [V]),
            CState
    catch
        Class:Error ->
            lager:error([{class, dike}], "Error in application transition~nRequest: ~p~nError: ~p~nStacktrace: ~p~n", [Request, {Class, Error}, erlang:get_stacktrace()]),
            (Mode == leader) andalso paxos_server:reply(From, {error, client_application_error}),
            CState
    end.

client_reply(Request, ReplyFN) ->
    try
        ReplyFN()
    catch
        Error:Reason ->
            lager:error([{class, dike}], "Error in application aftereffects~nRequest: ~p~nError: ~p~nStacktrace: ~p~n", [Request, {Error, Reason}, erlang:get_stacktrace()])
    end.

generate_paxos_server_name(PaxosGroup, Module) ->
    list_to_atom("$paxos_server$-" ++ atom_to_list(PaxosGroup) ++ "-" ++ atom_to_list(Module)).

receive_helper(Ref, PaxosGroup, Node, Msg) ->
    receive_helper(Ref, PaxosGroup, Node, Msg, 0).

receive_helper(Ref, PaxosGroup, Node, Msg, ?MAX_RECEIVE_RETRIES -1) ->
    case gen_paxos:request_issued_ping(Node, PaxosGroup, Ref) of
	pong ->
	    receive_helper(Ref, PaxosGroup, Node, Msg, ?MAX_RECEIVE_RETRIES);
	pang ->
	    lager:error([{class, dike}], "Error! pinging gen_paxos on ~p failed, returning error for the request issued ~p", [{PaxosGroup, Node}, Msg]),
	    error
    end;

receive_helper(Ref, PaxosGroup, Node, Msg, ?MAX_RECEIVE_RETRIES) ->
    receive
	{?LOG_UPDATE, Ref, Answer} ->
	    Answer
    after ?CLIENT_CALL_TIMEOUT ->
	    lager:error([{class, dike}], "Error! got no response from ~p although it was busy-pinged in last try, msg : ~p", [{PaxosGroup, Node},  Msg]),
	    error
    end;

receive_helper(Ref, PaxosGroup, Node, Msg, Cnt) ->
    receive
	{?LOG_UPDATE, Ref, Answer} ->
	    Answer
    after ?CLIENT_CALL_TIMEOUT ->
	    receive_helper(Ref, PaxosGroup, Node, Msg, Cnt + 1)
    end.
