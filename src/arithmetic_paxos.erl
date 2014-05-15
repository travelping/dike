%    __                        __      _
%   / /__________ __   _____  / /___  (_)___  ____ _
%  / __/ ___/ __ `/ | / / _ \/ / __ \/ / __ \/ __ `/
% / /_/ /  / /_/ /| |/ /  __/ / /_/ / / / / / /_/ /
% \__/_/   \__,_/ |___/\___/_/ .___/_/_/ /_/\__, /
%                           /_/            /____/
%
% Copyright (c) Travelping GmbH <info@travelping.com>

-module(arithmetic_paxos).

-behaviour(paxos_server).

-export([handle_call/3, handle_cast/2, init/1, init/2, export_state/1]).

-export([start_link/1, random_operation/0, send_operation/2, send_operation/3, read/1]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link(Gname) ->
    paxos_server:start_link(Gname, ?MODULE).


send_operation(Node, Group, {Op, V}) ->
    paxos_server:call(Node, Group, {Op, V}).

send_operation(Gname, {Operator, Value}) ->
    dike_dispatcher:request(Gname, {Operator, Value}).

read(Gname) ->
    dike_dispatcher:request(Gname, read).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% bahaviour callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_Options) ->
    {ok, 0}.

init(State, _Options) ->
    {ok, State}.

export_state(State) ->
    State.

handle_call('read', From, State) ->
    {reply, fun() -> paxos_server:reply(From, State) end, State};

handle_call({'add', N}, From, State) ->
    {reply, fun() -> paxos_server:reply(From, State + N) end, State + N};

handle_call({'sub', N}, From, State) ->
    {reply, fun() -> paxos_server:reply(From, State - N) end, State - N};

handle_call({'mult', N}, From, State) ->
    {reply, fun() -> paxos_server:reply(From, State * N) end, State * N};

handle_call({'div', N}, From, State)  when N =/= 0 ->
    {reply, fun() -> paxos_server:reply(From, State div N) end, State div N};

handle_call({'div', 0}, From, State)  ->
    {reply, fun() -> paxos_server:reply(From, State) end, State};

handle_call(_Req, From, State) ->
    {reply, fun() -> paxos_server:reply(From, ignored) end, State}.

handle_cast(_, State) ->
    {noreply, State}.

random_operation() ->
    random_operation(random:uniform(4)).
random_operation(1) ->
    'add';
random_operation(2) ->
    'sub';
random_operation(3) ->
    'mult';
random_operation(4) ->
    'div'.
