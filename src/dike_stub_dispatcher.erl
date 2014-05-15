%%%-------------------------------------------------------------------
%%% @author olerixmanntp <olerixmanntp@kiiiiste>
%%% @copyright (C) 2013, olerixmanntp
%%% @doc
%%%
%%% @end
%%% Created : 13 Sep 2013 by olerixmanntp <olerixmanntp@kiiiiste>
%%%-------------------------------------------------------------------
-module(dike_stub_dispatcher).

-behaviour(gen_server).

-include_lib("../include/dike.hrl").

%% API
-export([start_link/1,
	 add_group/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, dike_dispatcher).

-record(state, {routing_table,
		groups=[],
		timer}).

%%%===================================================================
%%% API
%%%===================================================================

add_group(GName, PaxosServerModule) ->
    gen_server:call(?SERVER, {add_group, GName, PaxosServerModule}).

start_link(Master) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Master], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Master]) when not is_list(Master) ->
    process_flag(trap_exit, true),
    RoutingTable=ets:new(dispatcher_table, [ordered_set, protected, named_table, {keypos, 2}, {read_concurrency, true}]),
    case node() of
	Master ->
	    ok;
	_ ->
	    {ok, Entries} = get_routing_table(Master),
	    ets:insert(RoutingTable, Entries)
    end,
    {ok, #state{routing_table=RoutingTable, groups=[]}}.

get_routing_table(Node) ->
    gen_server:call({?SERVER, Node}, get_routing_table).

handle_call(get_routing_table, _From, State= #state{routing_table=RT}) ->
    {reply, {ok, ets:tab2list(RT)}, State};

handle_call({add_group, Gname, PaxosServerModule}, _From, State = #state{routing_table=RT, groups=Groups}) ->
    case lists:member(Gname, Groups) of
	true ->
	    {reply, ok, State};
	false ->
	    stub_paxos:start_link(Gname, PaxosServerModule),
	    ets:insert(RT, #routing_table_entry{group_name=Gname, nodes=[node()]}),
	    {reply, ok, State#state{groups=[Gname |Groups]}}
    end;

handle_call(refresh_routing_table, _From, State) ->
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, unhandled, State}.

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
