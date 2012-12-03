%    __                        __      _
%   / /__________ __   _____  / /___  (_)___  ____ _
%  / __/ ___/ __ `/ | / / _ \/ / __ \/ / __ \/ __ `/
% / /_/ /  / /_/ /| |/ /  __/ / /_/ / / / / / /_/ /
% \__/_/   \__,_/ |___/\___/_/ .___/_/_/ /_/\__, /
%                           /_/            /____/
%
% Copyright (c) Travelping GmbH <info@travelping.com>

-module(dike).

-include_lib("dike.hrl").

-behaviour(application).

-export([start/0, 
	 start/2,
	 stop/1, 
	 stop/0,
	 prep_stop/1]).

-export([]).
-define(APPS, [regine, dike]).


-spec start() -> [ok | {error, any()}].
start() ->
    [application:start(App) || App <- ?APPS].

stop() ->
    %lager:debug([{class, dike}], "in dike:stop/0 on ~p ~n", [node()]),
    [application:stop(App) || App <- lists:reverse(?APPS)].

-spec start(_,_) -> {ok, pid()}.
start(_Type, _Args) ->
    %lager:debug([{class, dike}], "dike starting on ~p ~n", [node()]),
    %% start database-transaction-handler according to settings in env
    %% (this may also happen in gen_paxos if a db per group is configured
    case application:get_env(dike, db_mode) of
	{ok, per_machine} ->
	    {ok, DBFolder} = application:get_env(dike, db_dir),
    	    DBFolder2 = DBFolder ++ "dike_emdb",
    	    {ok, DBAdapter} = application:get_env(dike, db_adapter),
    	    {ok, Pid} = DBAdapter:open(DBFolder2),
    	    true = register(?DB_TRANSACTION_HANDLER, Pid),
	    lager:debug([{class, dike}], "initialized db per_machine ~p ~n", [node()]);

    	{ok, per_vm} ->
	    {ok, DBFolder} = application:get_env(dike, db_dir),
    	    DBFolder2 = DBFolder ++ "emdb_" ++  atom_to_list(node()),
    	    {ok, DBAdapter} = application:get_env(dike, db_adapter),
    	    {ok, Pid} = DBAdapter:open(DBFolder2),
    	    true = register(?DB_TRANSACTION_HANDLER, Pid);
    	{ok, per_group} ->
    	    ok
    end,
    
    {ok, Masters} = application:get_env(dike, masters),
    paxos_registry:start(),
    
    
						%    %lager:debug([{class, dike}], "initialized db, checking if i am master ~p ~p~n", [node(), Masters]),
    case dike_lib:position(Masters, node()) of
        not_found ->
            %lager:debug([{class, dike}], "on non-master node, doing nothing~p [~p]~n", [node(), Masters]),
            im_no_master;
        _Pos ->
            %	    %lager:debug([{class, dike}], "on master node, starting master ~p ~n", [node()]),
            dike_master:start_link(Masters)
    end,
    %   %lager:debug([{class, dike}], "dike started on node ~p ~n", [node()]),
    {ok, _SupPid} = dike_sup:start_link(Masters).

prep_stop(State) ->
    State.
    

-spec stop(_) -> ok.
stop(_State) ->
    %lager:debug([{class, dike}], "in application stop for dike, on node ~p~n", [node()]),
    {ok, DBAdapter} = application:get_env(dike, db_adapter),
    case application:get_env(dike, db_mode) of
	{ok, per_machine} ->
    	    DBAdapter:stop(?DB_TRANSACTION_HANDLER);
	
    	{ok, per_vm} ->
    	    DBAdapter:stop(?DB_TRANSACTION_HANDLER);	    
    	
	{ok, per_group} ->
	    %%will be stopped by the gen_paxos instances
    	    ok
    end,    
    ok.

%%###################################################
%% internals                                        #
%%###################################################


