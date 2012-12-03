-module(dike_db_adapter_mdb).

-behaviour(dike_db_adapter).

-behaviour(gen_server).

% dike_db_adapter callbacks
-export([open/1,
	 get/2,
	 put/3,
	 update/3,
	 partial_apply/4,
	 bulk_delete/2,
	 stop/1,
	 persisting/0]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-record(state, {handle,
		callbacks,
		bulk,
		last_commit,
		txn_ops=0}).

-define(SERVER, ?MODULE). 
-define(TXN_DELAY, 20000). % in micro secs
-define(INCOMING_TIMEOUT, 1).
-define(CALL_TIMEOUT_CLIENT, 10000000).
-define(CALL_TIMEOUT_SERVER, 8000000).
-define(TXN_MAX_OPS, 5000).

%%%===================================================================
%%% API
%%%===================================================================

%%%===================================================================
%%% dike_db_adapter callbacks
%%%===================================================================

%% opens a database at Path, returning {ok, Handle} where Handle will be passed to the other funs
open(Path) ->
    {ok, Handle} = emdb:open(Path, 10485760000),
    {ok, _Pid} = gen_server:start_link(?MODULE, [Handle], []).


get(Pid, Key) ->
%    %lager:debug([{class, dike}], "reading mdb: ~p~n", [Key]),
    gen_server:call(Pid, {get, now(), Key}, ?CALL_TIMEOUT_CLIENT).

put(Pid, Key, Val) ->
    gen_server:call(Pid, {put, now(), Key, Val}, ?CALL_TIMEOUT_CLIENT).

update(Pid, Key, Val) ->
 %   %lager:debug([{class, dike}], "updating mdb: (~p,~p)~n", [Key, Val]),
    gen_server:call(Pid, {update, now(), Key, Val}, ?CALL_TIMEOUT_CLIENT).

bulk_delete(Pid, Keys) when is_list(Keys) ->
    gen_server:call(Pid, {bulk_delete, now(), Keys}).

partial_apply(Pid, StartKey, MapFun, PredFun) ->
    gen_server:call(Pid, {partial_apply, now(), StartKey, MapFun, PredFun}, ?CALL_TIMEOUT_CLIENT).

stop(Pid) ->
    gen_server:call(Pid, stop).

persisting() ->
    true.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Handle]) ->
    {ok, #state{handle=Handle,
		callbacks=[],
		bulk=[],
		last_commit=now()},
     ?INCOMING_TIMEOUT}.

handle_call({get, T1, Key}, From, State=#state{handle=Handle, callbacks=Callbacks, bulk=B}) ->
    T2 = now(),
    TDiff = timer:now_diff(T2, T1),
    if TDiff > ?CALL_TIMEOUT_SERVER ->
	    {noreply, maybe_txn_restart(State), ?INCOMING_TIMEOUT};
       true ->
	    {noreply, maybe_txn_restart(State#state{callbacks=[From | Callbacks], 
						    bulk=[Handle:prepare_bulk({get, Key})| B]}), ?INCOMING_TIMEOUT}
    end;

handle_call({put, T1, Key, Val}, From, State=#state{handle=Handle, callbacks=Callbacks, bulk=B}) ->
    T2 = now(),
    TDiff = timer:now_diff(T2, T1),
    if TDiff > ?CALL_TIMEOUT_SERVER ->
	    {noreply, maybe_txn_restart(State), ?INCOMING_TIMEOUT};
       true ->
	    {noreply, maybe_txn_restart(State#state{callbacks=[From | Callbacks], 
						    bulk=[Handle:prepare_bulk({put, {Key, Val}})| B]}), ?INCOMING_TIMEOUT}
    end;    

    %% 	    case Handle:put(Key, Val) of
    %% 		ok ->
    %% 		    {noreply, maybe_txn_restart(State#state{callbacks=[{From, ok} | Callbacks]}), ?INCOMING_TIMEOUT};
    %% 		V -> 
    %% 		    {reply, V, maybe_txn_restart(State), ?INCOMING_TIMEOUT}
    %% 	    end
    %% end;

handle_call({update, T1, Key, Val}, From, State=#state{handle=Handle, callbacks=Callbacks, bulk=B}) ->
    T2 = now(),
    TDiff = timer:now_diff(T2, T1),
    if TDiff > ?CALL_TIMEOUT_SERVER ->
	    {noreply, maybe_txn_restart(State), ?INCOMING_TIMEOUT};
       true ->
	    {noreply, maybe_txn_restart(State#state{callbacks=[From | Callbacks], 
						    bulk=[Handle:prepare_bulk({update, {Key, Val}})| B]}), ?INCOMING_TIMEOUT}
    end;    

%% 	    case Handle:update(Key, Val) of
%% 		ok ->
%% 		    {noreply, maybe_txn_restart(State#state{callbacks=[{From, ok} | Callbacks]}), ?INCOMING_TIMEOUT};
%% 		V -> 
%% 		    {reply, V, maybe_txn_restart(State), ?INCOMING_TIMEOUT}
%% 	    end
%% end;


%% this function is asynchronous, Keys will be deleted
handle_call({bulk_delete, T1, Keys}, _From, State=#state{handle=Handle}) when is_list(Keys) ->
    T2 = now(),
    TDiff = timer:now_diff(T2, T1),
    if TDiff > ?CALL_TIMEOUT_SERVER ->
	    {noreply, maybe_txn_restart(State), ?INCOMING_TIMEOUT};
       true ->   
	    BulkList = [Handle:prepare_bulk({del, Key}) || Key <- Keys],
	    case Handle:bulk_txn_prepared(BulkList) of
		{ok, _ResponseList} ->
		    {reply, ok, maybe_txn_restart(State), ?INCOMING_TIMEOUT};
		V ->
		    {reply, {error, V}, maybe_txn_restart(State), ?INCOMING_TIMEOUT}
	    end
    end;

handle_call({partial_apply, T1, StartKey, MapFun, PredFun}, From, State=#state{handle=Handle, callbacks=Callbacks, txn_ops=TOps}) ->
    T2 = now(),
    TDiff = timer:now_diff(T2, T1),
    if TDiff > ?CALL_TIMEOUT_SERVER ->
	    {noreply, maybe_txn_restart(State), ?INCOMING_TIMEOUT};
       true ->   
	    case do_partial_apply(Handle, StartKey, MapFun, PredFun) of
		{ok, _OpCnt} ->
		    {noreply, maybe_txn_restart(State#state{callbacks=[{From, ok} | Callbacks], txn_ops=TOps}), ?INCOMING_TIMEOUT};
		V -> 
		    %%fail
		    {reply, V, maybe_txn_restart(State), ?INCOMING_TIMEOUT}
	    end
    end;

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State, ?INCOMING_TIMEOUT}.

handle_cast(_Msg, State) ->
    {noreply, State, ?INCOMING_TIMEOUT}.

handle_info(timeout, State) ->
    State2 = maybe_txn_restart(State),
    {noreply, State2, ?INCOMING_TIMEOUT};

handle_info(_Info, State) ->
    {noreply, State, ?INCOMING_TIMEOUT}.

terminate(_Reason, #state{handle=Handle}) ->
    %lager:debug([{class, dike}], "dike_db_adapter_mdb terminating -> stop called beforehand~n", []),
    Handle:txn_abort(),
    Handle:close(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_partial_apply(Handle, StartKey, MapFun, PredFun) ->
    ok = Handle:cursor_open(),
    {ok, OpCnt} = do_partial_apply_helper(Handle, Handle:cursor_set(StartKey), MapFun, PredFun, 0),
    ok = Handle:cursor_close(),
    {ok, OpCnt}.

do_partial_apply_helper(Handle, {ok, {K, V}}, MapFun, PredFun, OpCnt) ->
    case PredFun(K, V) of
	false ->
	    case MapFun(K, V) of
		delete ->
		    do_partial_apply_helper(Handle, Handle:cursor_del(), MapFun, PredFun, OpCnt + 1);
		_  ->
		    do_partial_apply_helper(Handle, Handle:cursor_next(), MapFun, PredFun, OpCnt + 1)
	    end;
	true ->
	    {ok, OpCnt}
    end;

do_partial_apply_helper(_Handle, {error, E}, _MapFun, _PredFun, OpCnt) when E==error_cursor_get ; E==error_cursor_set->
    % reached the end, without hitting the predicate, don't care right now
    %lager:debug([{class, dike}], "failure in do_partial_apply_helper ~p~n", [node()]),
    {ok, OpCnt}.


maybe_txn_restart(State=#state{callbacks=[], bulk=[], last_commit=LC, txn_ops=TOps}) ->
    TDiff = timer:now_diff(now(), LC), 
    if TDiff >= ?TXN_DELAY ; TOps >= ?TXN_MAX_OPS ->
	    State#state{last_commit=now()};
       true ->
	    State
    end;

maybe_txn_restart(State=#state{handle=Handle, last_commit=LC, txn_ops=TOps, callbacks=CBs, bulk=Bulks}) ->
    TDiff = timer:now_diff(now(), LC), 
    if TDiff >= ?TXN_DELAY ; TOps >= ?TXN_MAX_OPS ->
	    RBulks = lists:reverse(Bulks),
	    RCallbacks = lists:reverse(CBs),
	    case  Handle:bulk_txn_prepared(RBulks) of
		{ok, Responses} ->
				  CombinedList = lists:zip(Responses, RCallbacks),
				  [gen_server:reply(From, Resp) || {Resp, From} <- CombinedList];
		{error, Err} ->
		    [gen_server:reply(From, {error, Err}) || From <- RCallbacks]
	    end,
	    State#state{last_commit=now(), txn_ops=0, callbacks=[], bulk=[]};
       true ->
	    State
    end.
		 
    
