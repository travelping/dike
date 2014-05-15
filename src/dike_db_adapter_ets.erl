-module(dike_db_adapter_ets).

-behaviour(dike_db_adapter).


% dike_db_adapter callbacks
-export([open/1,
	 get/2,
	 put/3,
	 update/3,
	 partial_apply/4,
	 bulk_delete/2,
	 stop/1,
	 persisting/0]).

persisting() ->
    false.

open(_Path) ->
    Log = ets:new(paxos_log, [ordered_set, public, {keypos, 1}]),
    {ok, Log}.

get(Log, Key) ->
    case ets:lookup(Log, Key) of
	[] ->
	    {error, undefined};
	[{Key, V}] ->
	    {ok, V}
    end.

put(_Log, _Key, _Val) ->
    dummy.

update(Log, Key, Val) ->
    ets:insert(Log, {Key, Val}),
    ok.

partial_apply(Pid, StartKey, MapFun, PredFun) ->
    case do_partial_apply(Pid, ets:lookup(Pid, StartKey), MapFun, PredFun, 0) of
	{ok, _OpCnt} ->
	    ok;
	V ->
	    V
    end.

do_partial_apply(Pid, [{K, V}], MapFun, PredFun, OpCnt) ->
    case PredFun(K, V) of
	false ->
	    NextKey= ets:next(Pid, K),
	    case MapFun(K, V) of
		delete ->
		    ets:delete(Pid, K),
		    do_partial_apply(Pid, ets:lookup(Pid, NextKey), MapFun, PredFun, OpCnt + 1);
		_  ->
		    do_partial_apply(Pid, ets:lookup(Pid, NextKey), MapFun, PredFun, OpCnt + 1)
	    end;
	true ->
	    {ok, OpCnt}
    end;

do_partial_apply(_Pid, [], _MapFun, _PredFun, OpCnt) ->
    {ok, OpCnt}.

bulk_delete(Pid, Keys) when is_list(Keys) ->
    [ets:delete(Pid, Key) || Key <- Keys],
    ok.

stop(Pid) ->
    %% nothing to do, will be stopped when the creating process gets trashed
    ets:delete(Pid),
    ok.
