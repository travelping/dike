%    __                        __      _
%   / /__________ __   _____  / /___  (_)___  ____ _
%  / __/ ___/ __ `/ | / / _ \/ / __ \/ / __ \/ __ `/
% / /_/ /  / /_/ /| |/ /  __/ / /_/ / / / / / /_/ /
% \__/_/   \__,_/ |___/\___/_/ .___/_/_/ /_/\__, /
%                           /_/            /____/
%
% Copyright (c) Travelping GmbH <info@travelping.com>

-module(dike_lib).

-include_lib("paxos_lib.hrl").
-include_lib("../include/dike.hrl").

-compile(export_all).
-define(DEFAULT_TIMEOUT, 300000).

%% Parrallel Map
pmap(FList) ->
    pmap(FList, ?DEFAULT_TIMEOUT).

pmap(FList, Timeout) when is_list(FList) ->
    Parent = self(),
    Pids = [proc_lib:spawn(fun() -> Parent ! {self(), catch F()} end) || F <- FList],
    receive_for_pids(Pids, Timeout);

pmap(F, L) when is_function(F) ->
    pmap(F, L, ?DEFAULT_TIMEOUT).

pmap(F, L, Timeout) ->
    Parent = self(),
    Pids = [proc_lib:spawn(fun() -> Parent ! {self(), catch F(X)} end) || X <- L],
    receive_for_pids(Pids, Timeout).

% Backwardscompatibility
pmap_funs(FList) -> pmap(FList).

receive_for_pids(Pids, Timeout) ->
    [receive {Pid, Result} ->
         Result
     after Timeout ->
         {error, timeout}
     end || Pid <- Pids].

position(List, Val) ->
    position(List, Val, 0).

position([Val|_T], Val, N) ->
    N;
position([_|T], Val, N) ->
    position(T,Val, N + 1);
position([], _Val, _N) ->
    not_found.

timestamp() ->
    calendar:datetime_to_gregorian_seconds(calendar:now_to_universal_time(now())).

masters() ->
    {ok, Masters} = application:get_env(dike, masters),
    Masters.

uniform_list([]) ->
    true;
uniform_list([H|T]) ->
    uniform_list(T, H).

uniform_list([],_) ->
    true;
uniform_list([H|T], H) ->
    uniform_list(T,H);
uniform_list([_|_], _) ->
    false.

replace(Old, New, List) -> replace(Old, New, List, []).
replace(_Old, _New, [],           Acc) -> lists:reverse(Acc);
replace(Old,  New,  [Old|List],   Acc) -> replace(Old, New, List, [New|Acc]);
replace(Old,  New,  [Other|List], Acc) -> replace(Old, New, List, [Other|Acc]).

replace_nth([_|T], 0, V) ->
    [V|T];

replace_nth([H|T], N, V) ->
    [H | replace_nth(T, N - 1, V)];

replace_nth([], _N, _V) ->
    not_found.

get_group_coordinator_name(GroupName) ->
    list_to_atom(atom_to_list(?COORDINATOR_TAG) ++ atom_to_list(GroupName) ).
