%    __                        __      _
%   / /__________ __   _____  / /___  (_)___  ____ _
%  / __/ ___/ __ `/ | / / _ \/ / __ \/ / __ \/ __ `/
% / /_/ /  / /_/ /| |/ /  __/ / /_/ / / / / / /_/ /
% \__/_/   \__,_/ |___/\___/_/ .___/_/_/ /_/\__, /
%                           /_/            /____/
%
% Copyright (c) Travelping GmbH <info@travelping.com>

-module(dike_SUITE).

-compile(export_all).

-include_lib("../include/dike.hrl").

-define(LOG_LEVEL, debug).
-define(LOG_FILE, "/log/console.log").

init_per_suite(Config) ->
    {ok, CWD} = file:get_cwd(),
    error_logger:info_msg("Logging through lager, see ~p~n", [CWD ++ ?LOG_FILE]),
    dike_test:start_lager(?LOG_LEVEL),
    dike_test:init_self("coordinator"),
    TestVMS = dike_test:nodes_slave_init("test", 5, ?LOG_LEVEL),
    dike_test:nodes_set_dike_masters(TestVMS),
    TestVMS2 = TestVMS,%[list_to_atom("coordinator@" ++ MyHostname) |TestVMS],
    ClientVMS = dike_test:nodes_slave_init("non_master", 6, ?LOG_LEVEL),
    timer:sleep(1000),
    [{clients, ClientVMS} | [{nodes, TestVMS2} | Config]].

end_per_suite(Config) ->
    _Nodes = proplists:get_value(nodes, Config),
    dike_test:stop_nodes(names()).

init_per_testcase(TestCase, Config) ->
    case {TestCase, application:load(emdb)} of
        {dike_db_adapter_mdb, {error, {"no such file or directory", _}}} ->
            {skip, emdb_not_installed};
        _ ->
            Clients = proplists:get_value(clients, Config),
            Masters = proplists:get_value(nodes, Config),
            ClientNodes =  Masters ++ Clients,
            AllNodes = ClientNodes ++ [node()],

            lager:info("setting masters on ~p to ~p~n", [AllNodes, Masters]),
            [rpc:call(Node, application, set_env, [dike, masters, Masters]) || Node <- AllNodes],
            timer:sleep(5000),
            [rpc:call(SlaveNode, dike, start, []) || SlaveNode <- AllNodes],
            lager:info("started dike~n", []),
            ensure_loaded(dike, AllNodes),
            timer:sleep(5000),
            Config
    end.

end_per_testcase(_TestCase, Config) ->
    [rpc:call(SlaveNode, application, stop, [dike]) || SlaveNode <- proplists:get_value(clients, Config) ++ names()],
    ok.

all(doc) ->
    ["Describe the main purpose of this suite"];

all(suite) ->
    [ensure_dike_started].

all() ->
    [dike_db_adapter_mdb,
     dike_db_adapter_ets,
     ensure_dike_started,
     master_election,
     hashring].

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

dike_db_adapter_mdb() ->
    [{timetrap, {minutes, 5}}].
dike_db_adapter_mdb(doc) ->
    ["Tests a dike_db_adapter"];
dike_db_adapter_mdb(suite) ->
    [];
dike_db_adapter_mdb(Config) ->
    dike_db_adapter_helper(Config, dike_db_adapter_mdb).

dike_db_adapter_ets() ->
    [{timetrap, {minutes, 5}}].
dike_db_adapter_ets(doc) ->
    ["Tests a dike_db_adapter"];
dike_db_adapter_ets(suite) ->
    [];
dike_db_adapter_ets(Config) ->
    dike_db_adapter_helper(Config, dike_db_adapter_ets).

dike_db_adapter_helper(Config, DBMod) when is_list(Config) ->
    ParNum=10000,
    {ok, Adapter} = DBMod:open("./db_adapter_test_mdb"),
    DBMod:update(Adapter, test, geht),
    {ok, geht} = DBMod:get(Adapter, test),
    DBMod:update(Adapter, test2, geht),
    {ok, geht} = DBMod:get(Adapter, test2),
    db_adapter_par_requests(ParNum, fun(K) ->
					    {T, ok} = timer:tc(DBMod, update, [Adapter, {'vnode-1', K}, testval]),
					    T
				   end),
    db_adapter_par_requests(ParNum, fun(K) ->
					    {T, {ok, testval}} = timer:tc(DBMod, get, [Adapter, {'vnode-1', K}]),
					    T
				    end),
    DBMod:bulk_delete(Adapter,[{'vnode-1', N} ||  N <- lists:seq(1,ParNum - 1)]),
    {error, undefined} = DBMod:get(Adapter, {'vnode-1', 1}),
    {error, undefined} = DBMod:get(Adapter, {'vnode-1', ParNum - 1}),
    {ok, testval} = DBMod:get(Adapter, {'vnode-1', ParNum}).

db_adapter_par_requests(N, Fun) ->
    timer:tc(dike_lib, pmap, [Fun,
			      lists:seq(1,N)]).

ensure_dike_started() ->
    [{timetrap, {minutes, 5}}].
ensure_dike_started(doc) ->
    ["Most simple testcase, only checks if the environment is setup properly for the next testcases"];
ensure_dike_started(suite) ->
    [];
ensure_dike_started(Config) when is_list(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    [timer:tc(net_adm, ping, [N]) || N <- Nodes].

master_election() ->
    [{timetrap, {minutes, 60}}].

master_election(Config) when is_list(Config) ->
    timer:sleep(1000),
    Clients = proplists:get_value(clients, Config),
    [dike_master:join(Node) || Node <- Clients], %%adding new nodes to the dike
    already_known = dike_master:join(hd(Clients)), %% trying to readd the same node...
    lager:debug("added nodes~n", []),
    start_and_test_arithmetic(arithmetic_1),
    lager:debug("tested arithmetic~n", []),
    NodesWithPaxosServers = dike_dispatcher:get_nodes(arithmetic_1),
    5 = length(NodesWithPaxosServers),

    timer:sleep(5000),
    ok = gen_paxos:lock_log_complete(hd(NodesWithPaxosServers), arithmetic_1),
    arithmetic_paxos:send_operation(lists:last(NodesWithPaxosServers), arithmetic_1, {'div', 3}),
    CmpVal = arithmetic_paxos:send_operation(lists:last(NodesWithPaxosServers), arithmetic_1, {add, 100}),
    ok = gen_paxos:unlock_log_complete(hd(NodesWithPaxosServers), arithmetic_1),

    CmpVal = paxos_server:call(hd(NodesWithPaxosServers), arithmetic_1, read),
    {NodesToStop, _} = lists:split(2, NodesWithPaxosServers),
    timer:sleep(2000),
    H13 = rpc:call(lists:last(NodesWithPaxosServers), dike_dispatcher, check_remote_group, [arithmetic_1, arithmetic_paxos]),
    lager:debug("checked remote group: ~p~n", [H13]),
    [ok = gen_paxos:stop(HelperNode, arithmetic_1) || HelperNode <- NodesToStop],
    CmpVal2 = arithmetic_paxos:send_operation(lists:last(NodesWithPaxosServers), arithmetic_1, {'div', 3}),
    lager:debug("only checking consistency now~n", []),

%    dike_dispatcher:refresh_routing_table(),
    [CmpVal2 = paxos_server:call(HelperNode, arithmetic_1, read) || HelperNode <- NodesToStop], % these calls will be retried until the gen_paxos instances are back up....
    lager:debug("done checking consistency~n", []).

hashring(Config) ->
    Clients = proplists:get_value(clients, Config),
    [H|T] = Clients,
    [dike_master:join(Node) || Node <- T], %%adding new nodes to the dike
    paxos_hashring:start(),
    timer:sleep(10000),
    dike_dispatcher:refresh_routing_table(),
    requests_to_hashring(50, 100),
    lager:debug("hashring-test: first round of messages done, adding node ~p~n", [H]),
    dike_master:join(H),
    [NodeToRestart | _Rest] = T,
    dike_dispatcher:refresh_routing_table(),

    lager:debug("restarting one node~n", []),
    rpc:call(NodeToRestart, dike, stop, []),
    rpc:call(NodeToRestart, dike, start, []),
    lager:debug("done restarting~n", []),

    dike_dispatcher:refresh_routing_table(),

    lager:debug("hashring-test: second round of messages started~n", []),
    requests_to_hashring(50, 300),
    lager:debug("hashring-test: all requests proceeded~n", []),

    paxos_hashring:check_consistency(),
    lager:debug("checked consistency~n", []),
    Randoms = [crypto:rand_uniform(1, 200) || _ <- lists:seq(1, 100)],
    [paxos_hashring:cast(GroupToHash, {'div', 10}) || GroupToHash <- Randoms],
    timer:sleep(1000),
    RV = paxos_hashring:check_consistency(),
    lager:debug("checked consistency again after casting~n", []),
    RV.

requests_to_hashring(Par, Seq) ->
    dike_lib:pmap(fun(_) -> random:seed(now()),
			    [Helper(I) || I <- lists:seq(1,Seq)]
			              [paxos_hashring:send(random:uniform(5000), {arithmetic_paxos:random_operation(), random:uniform(200) - 100}) || I <- lists:seq(1,Seq)]
		  end,
		  lists:seq(1,Par)).

start_and_test_arithmetic(PaxosGroupName) ->
    #routing_table_entry{group_name=PaxosGroupName, nodes=ArithNodes} = dike_master:add_group(PaxosGroupName, arithmetic_paxos), %% adding a new paxos_group to the dike
    lager:debug("trying to refresh routing information on the common-test-node~n", []),
    dike_dispatcher:refresh_routing_table(),
    lager:debug(".....done~n", []),
    lager:debug("started arithmetic_paxos for group ~p, participating nodes: ~p~n", [PaxosGroupName, ArithNodes]),
    [dike_lib:pmap(fun(Node) ->
			   random:seed(now()),
			   dike_dispatcher:request(PaxosGroupName, {arithmetic_paxos:random_operation(), random:uniform(200) - 100})
		   end,
		   ArithNodes) || _V <- lists:seq(1,100)], %% issuing operations to the every member of the new paxos_group (always 5 in parallel)
    [H|T] = [paxos_server:call(Node, PaxosGroupName, read) || Node <- ArithNodes],
    [H=V || V <- T], %% making sure consistency has prevailed
    [H|T].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% utility functions                               %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

ensure_loaded(Module, Nodes) ->
    EnsureLoaded = fun(Node) ->
			   rpc:call(Node, code, ensure_loaded, [Module])
		   end,
    [{module, Module} = EnsureLoaded(Node) || Node <- Nodes].

appstop(Module) ->
    [rpc:call(Node, Module, stop, []) || Node <- names()],
    Module:stop().

names() ->
    {ok, MasterNodes} = application:get_env(dike, masters),
    MasterNodes.
