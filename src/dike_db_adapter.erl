-module(dike_db_adapter).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{open, 1},
     {get, 2},
     {put, 3},
     {bulk_delete, 2},
     {partial_apply, 4},
     {stop,1},
     {persisting,0}];

behaviour_info(_) ->
    undefinded.
