-module(dike_test).
-export([nodes_dike_init/2, nodes_dike_init/3, nodes_slave_init/2, nodes_slave_init/3,
         local_init/0, local_init/1, start_lager/1, start_lager/2, stop_nodes/1,
         init_self/1, nodes_set_dike_masters/1, apply_conf/1, apply_conf/2, config_lager/2,
         copy_local_conf/1]).
-define(LOG_LEVEL, debug).
-define(LOG_FILE, "/log/console.log").
-compile([{parse_transform, lager_transform}]).

local_init() ->
    local_init(?LOG_LEVEL).
local_init(LogLevel) ->
    start_lager(LogLevel),
    application:load(dike),
    application:set_env(dike, masters, [node()]).

nodes_dike_init(App, N) ->
    nodes_dike_init(App, N, ?LOG_LEVEL).
nodes_dike_init(App, N, LogLevel) ->
    init_self(App),
    SlaveNodes = nodes_slave_init(App, N - 1, LogLevel),
    nodes_set_dike_masters([node() | SlaveNodes]),
    [node() | SlaveNodes].

nodes_slave_init(App, N) ->
    nodes_slave_init(App, N, ?LOG_LEVEL).
nodes_slave_init(App, N, LogLevel) ->
    NodeNames  = node_names(App, N),
    SlaveNodes = nodes_start(NodeNames),
    add_test_code_path(SlaveNodes),
    start_lager(SlaveNodes, LogLevel),
    SlaveNodes.

nodes_set_dike_masters(Nodes) ->
    {_, []} = rpc:multicall(application, load, [dike]),
    {_, []} = rpc:multicall(application, set_env, [dike, masters, Nodes]).

stop_nodes(Nodes) ->
    [{ok, _} = ct_slave:stop(Host, Node) || {Node, Host} <- [split_hostname(FullName) || FullName <- Nodes]],
    ok = net_kernel:stop().

copy_local_conf(AllNodes) ->
    DikeEnv = application:get_all_env(dike),
    [[rpc:call(Node, application, set_env, [dike, Key, Value]) || {Key, Value} <- DikeEnv] || Node <- AllNodes].

apply_conf(Terms) ->
    apply_conf(node(), Terms).
apply_conf(Node, Terms) ->
    [begin
         application:load(App),
         [application:set_env(App, Env, Value) || {Env, Value} <- Config],
         lager:info("node ~s load config ~p", [Node, {App, Config}])
     end || {App, Config} <- Terms].

% --------------------------------------------------------------------------------------------------
% -- Helpers

init_self(App) ->
    os:cmd("epmd &"),
    % waiting, until epmd is started
    timer:sleep(1000),
    net_kernel:start([list_to_atom(App), shortnames]).

nodes_start(NodeNames) ->
    {ok, HostName} = inet:gethostname(),
    Host = list_to_atom(HostName),
    [begin
         lager:info("starting node ~s on host ~s", [NodeName, Host]),
         case ct_slave:start(Host, NodeName, [{monitor_master, true}]) of
             {error, started_not_connected, SlaveNode} ->
                 lager:error("node ~s started, but not connected, ping ~p", [SlaveNode, net_adm:ping(SlaveNode)]),
                 SlaveNode;
             {error, already_started, SlaveNode} ->
                 lager:error("node ~s already started, ping ~p", [SlaveNode, net_adm:ping(SlaveNode)]),
                 SlaveNode;
             {ok, SlaveNode} ->
                 lager:info("node ~s started as slavenode", [SlaveNode]),
                 SlaveNode;
             Result ->
                 lager:error("node ~s on host ~s can't be started, because ~p", [NodeName, Host, Result]),
                 throw({error, Result})
          end
     end || NodeName <- NodeNames].

add_test_code_path(SlaveNodes) ->
    lager:info("adding code pathes"),
    {ok, Cwd} = file:get_cwd(),
    rpc:multicall(SlaveNodes, code, set_path, [code:get_path()]).

node_names(App, N) ->
    [list_to_atom(App ++ integer_to_list(I)) || I <- lists:seq(1, N)].

start_lager(Nodes) when is_list(Nodes) ->
    start_lager(Nodes, ?LOG_LEVEL);
start_lager(LogLevel) when is_atom(LogLevel) ->
    start_lager(nodes(), LogLevel).
start_lager(Nodes, DebugLevel) ->
    {ok, CWD} = file:get_cwd(),
    [rpc:multicall(Nodes -- [node()], Module, Function, Args)
        || {Module, Function, Args} <- [{application, load, [lager]},
                                        {?MODULE, config_lager, [CWD, DebugLevel]},
                                        {crypto, start, []},
                                        {lager, start, []}]],
    application:load(lager),
    application:set_env(lager, handlers, [
        {lager_console_backend, info},
        {lager_file_backend, [{file, CWD ++ ?LOG_FILE}, {level, DebugLevel}]}
    ]),
    crypto:start(),
    lager:start().

config_lager(CWD, LogLevel) ->
    application:set_env(lager, handlers, [{lager_file_backend, [{file, CWD ++ ?LOG_FILE}, {level, LogLevel}]}]).

split_hostname(HN) ->
    [Node, Host] = [list_to_atom(binary_to_list(X)) || X <- re:split(atom_to_list(HN), "@")],
    {Node, Host}.
