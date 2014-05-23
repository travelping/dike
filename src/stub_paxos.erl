%%%-------------------------------------------------------------------
%%% @author olerixmanntp <olerixmanntp@kiiiiste>
%%% @copyright (C) 2013, olerixmanntp
%%% @doc
%%%
%%% @end
%%% Created : 13 Sep 2013 by olerixmanntp <olerixmanntp@kiiiiste>
%%%-------------------------------------------------------------------
-module(stub_paxos).

-behaviour(gen_server).

%% API
-export([start_link/2,
	 append/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(STUB_TAG, '$stub_paxos$-').

-record(state, {paxos_server_mod,
		group,
		paxos_server_state}).

%%%===================================================================
%%% API
%%%===================================================================

append(Node, GName, V) ->
    lager:debug([{class, dike}], "trying to append  : ~p", [{Node, GName, V}]),
    gen_server:call({generate_group_name(GName), Node}, {append, V}).

start_link(Gname, PaxosServerModule) ->
    gen_server:start_link({local, generate_group_name(Gname)}, ?MODULE, [Gname, PaxosServerModule], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Gname, PaxosServerModule]) ->
    Options=[{paxos_group, Gname}],
    {ok, InitialState} = PaxosServerModule:init(Options),
    lager:info([{class, dike}], "Initialized state for ~p", [{Gname, PaxosServerModule}]),
    {ok, #state{group=Gname,
		paxos_server_mod=PaxosServerModule,
		paxos_server_state=InitialState}}.

handle_call({append, V}, From, State = #state{paxos_server_mod=PSM, paxos_server_state=PSS}) ->
    try
	NewPSS = case PSM:handle_call(V, {single_node_reply, From}, PSS) of
		     {reply, ReplyFN, PSS2} ->
			 ErrorHelper = fun() -> try
						    ReplyFN()
						catch
						    Error:Reason ->
							lager:error([{class, dike}], "Error in application aftereffects~nRequest: ~p~nError: ~p", [V, {Error, Reason, erlang:get_stacktrace()}])
						end
				       end,
			 catch spawn(ErrorHelper),
			 PSS2;
		     {noreply, PSS2} ->
			 PSS2;
		     V ->
			 PSS
		 end,
	{noreply, State#state{paxos_server_state=NewPSS}}
    catch
	Class:Error ->
	    lager:error([{class, dike}], "Error in application transition~nRequest: ~p~nError: ~p", [V, {Class, Error, erlang:get_stacktrace()}]),
	    {reply, {error, client_application_error}, State}
    end;

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

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

generate_group_name(Gname) ->
    list_to_atom(atom_to_list(?STUB_TAG) ++ atom_to_list(Gname)).
