%    __                        __      _
%   / /__________ __   _____  / /___  (_)___  ____ _
%  / __/ ___/ __ `/ | / / _ \/ / __ \/ / __ \/ __ `/
% / /_/ /  / /_/ /| |/ /  __/ / /_/ / / / / / /_/ /
% \__/_/   \__,_/ |___/\___/_/ .___/_/_/ /_/\__, /
%                           /_/            /____/
%
% Copyright (c) Travelping GmbH <info@travelping.com>

-module(dike_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Masters) ->
%    process_flag(trap_exit, true),
    supervisor:start_link({local, ?SERVER}, ?MODULE, [Masters]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([Masters]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = transient,%permanent,
    Shutdown = 2000,
    Type = worker,

    Dispatcher = case length(Masters) of 
		     1 ->
			 {dike_dispatcher, {dike_stub_dispatcher, start_link, [hd(Masters)]},
			  Restart, Shutdown, Type, [dike_stub_dispatcher]};
		     5 ->
			 {dike_dispatcher, {dike_dispatcher, start_link, [Masters]},
			  Restart, Shutdown, Type, [dike_dispatcher]}
		 end,
    
    %% AChild = {'AName', {'AModule', start_link, []},
    %% 	      Restart, Shutdown, Type, ['AModule']},
    
    {ok, {SupFlags, [Dispatcher]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
