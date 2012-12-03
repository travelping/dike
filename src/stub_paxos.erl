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
    lager:debug([{class, dike}], "trying to append  : ~p~n", [{Node, GName, V}]),
    gen_server:call({generate_group_name(GName), Node}, {append, V}).


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Gname, PaxosServerModule) ->
    gen_server:start_link({local, generate_group_name(Gname)}, ?MODULE, [Gname, PaxosServerModule], []).

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
init([Gname, PaxosServerModule]) ->
    Options=[{paxos_group, Gname}],
    {ok, InitialState} = PaxosServerModule:init(Options),
    lager:info([{class, dike}], "Initialized state for ~p~n", [{Gname, PaxosServerModule}]),
    {ok, #state{group=Gname, 
		paxos_server_mod=PaxosServerModule,
		paxos_server_state=InitialState}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_call({append, V}, From, State = #state{paxos_server_mod=PSM, paxos_server_state=PSS}) ->
    try
	NewPSS = case PSM:handle_call(V, {single_node_reply, From}, PSS) of
		     {reply, ReplyFN, PSS2} ->
			 
			 ErrorHelper = fun() -> try
						    ReplyFN()
						catch 
						    Error:Reason ->
							lager:debug([{class, dike}], "Error ~p in application logic aftereffects! Request: ~p~n", [{Error, Reason}, V])
						end
				       end,
			 catch spawn(ErrorHelper),
			 PSS2;
		     {noreply, PSS2} ->
			 PSS2;
		     V ->
						%lager:debug([{class, dike}], "paxos_server got a bad reply from its client module: ~p~n", [V]),
			 PSS
		 end,
	{noreply, State#state{paxos_server_state=NewPSS}}
    catch
	Class:Error ->
	    lager:error([{class, dike}], "Error! client application brought an error up in a transition: ~p~n", [{Class, Error}]),
	    {reply, {error, client_application_error}, State}
    end;

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

generate_group_name(Gname) ->
    list_to_atom(atom_to_list(?STUB_TAG) ++ atom_to_list(Gname)).
