
# Dike

Dike is a framework based on paxos for implementing a distributed statemachine, consistency is enforced (based on Paxos) given some requirements.
It is possible to have each statemachine's state 1,3 or 5 times replicated in the cluster.
As Dike is based on Paxos it requires a quorum of members from a statemachine to be participating in handling a request.
Several statemachines of the same kind can be used in a cluster and building a hashring for horizontal scalability is encouraged.

For logging [lager](https://github.com/basho/lager) is used and may be configured in the environment.

## Installation

### with rebar

required: git, rebar

```bash
$ git clone https://github.com/travelping/dike.git
$ cd dike
$ rebar get-deps
$ rebar compile
```

### with mix

required: git, elixir, mix

```bash
$ git clone https://github.com/travelping/dike.git
$ cd dike
$ mix deps.get
$ mix compile
```

## Unit-tests

### with mix

```bash
$ mix ct
```

## Using dike

For using Dike an application must basically implement the paxos_server behaviour.
It is similar to gen_server or gen_fsm.

However, the return value of the message handling callback is different from the gen_ modules.
Instead of returning a reply message, an anonymous function with no argument is to be returned.
Dike will call this function once per request which is processed by the paxos_server.

### Example

A very simple example application can be found in `src/paxos_hashring.erl`, it is also being used in the testsuite.
The corresponding `paxos_server` implementation is in `src/arithmetic_paxos.erl`.
It is a server which has an integer as its state on which it can perform simple arithmetic operations (+, -, *, /).

The request handler callback in `src/arithmetic_paxos.erl` is defined as following:

```erlang
handle_call({'add', N}, From, State) ->
    {reply, fun() -> paxos_server:reply(From, State + N) end, State + N};

handle_call({'div', N}, From, State)  when N =/= 0 ->
    {reply, fun() -> paxos_server:reply(From, State div N) end, State div N};
...
```

Requests are spread in a hashring, every request must contain a key by which the related vnode is found (from `src/paxos_hashring.erl`):

```erlang
send(Entity, Req) ->
    Id = erlang:phash2(Entity) rem ?GROUP_COUNT,
    dike_dispatcher:request(list_to_atom("vnode-" ++ integer_to_list(Id+1)), Req).
```

Setting up several erlang nodes with correct naming, network links, cookies etc. may be messy (if just done for testing).
So the following snippet sets up a 5 node paxos_hashring cluster in an elixir repl for testing:

```elixir
iex -S mix run --no-start

  # setup nodes and config, start dike
  >:dike_test.nodes_dike_init 'dike-test-', 5

  >:dike_test.copy_local_conf :erlang.nodes

  >:rpc.multicall Application, :ensure_all_started, [:dike]

  # verify there are no vnodes running
  >:dike_dispatcher.get_routing_table
  []

  >:paxos_hashring.start
  [{:routing_table_entry, :"vnode-1", ...}, ...]

  # check for routing table entries again
  >:dike_dispatcher.get_routing_table
  [{:routing_table_entry, :"vnode-1", ...}, ...]

  # send some requests
  >:paxos_hashring.send "somekey", {:add, 15}
  15
  >:paxos_hashring.send "somekey", {:add, 1}
  16
  >:paxos_hashring.send "somekey", {:div, 2}
  8
```

## Configuration
   Configuration is done in the erlang environment, see comments in `config/config.exs`.

## Consistency requirements

Dike is build on the assumption that requests are serialized through Paxos and as such seen in the same order on every node.
This way, starting from an initial state which is hard-coded, every statemachine will see the same state after each request.
The `handle_call` functions must be designed in a way to guarantee this, otherwise inconsistencies will occur.

As every action is executed multiple times, all sideeffects (including the reply to caller), must be wrapped in an anonymous function with no argument.

In short form:
* make all actions only depend on the request data itself and the paxos_servers state.
* wrap all sideeffects in the handlers reply function.

export_state/1 and init/2 are used for serializing/desirializing of the paxos server's state and will be called if a node joins a group which has already processed requests.

## TODO

* add epoche support and corresponding epoche-leaders for `src/gen_paxos.erl`
* add transaction support for vnodes/paxos-groups
