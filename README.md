
# Dike

Dike is a framework for implementing a distributed, data-oriented application logic which enforces consistency given some requirements.

## Installation

For building Dike at least the following erlang applications and their dependencies are required:

* lager
* regine
* tetrapak

Tetrapak is the buildtool used for Dike and its dependencies.

building Dike:
tetrapak build

installing Dike:
tetrapak install


## Unit-tests
Currently, the common test suite does not work together with lager, but in past revisions lager was not used so you can go back to a revision where lager was not included and recover the test_log.erl file and replace calls to lager with calls to test_log.erl.
Or check out a past version and test this, only small changes due to the PCS implementation at Travelping have been done afterwards.

testing Dike:
tetrapak test

## Using dike

When using Dike you are basically implementing the paxos_server behaviour.
It is similar to gen_server or gen_fsm.

However, the return value of the message handling callback is a bit different from the gen_ modules.
Instead of returning a reply message, an anonymous function with no argument is to be returned.
Dike will call this function once per request that is processed by the application logic.

The following two points must be considered when implementing a paxos_server:
* The changes done by the Paxos server implementation to its state must only depend on the request and the current state of the application logic. External data like timestamps must be included in the requests.
* Sideeffects, like sending a message, must be wrapped in the returned function to make sure they are executed only once. 

export_state/1 and init/2 are used for serializing/desirializing of the paxos server's state.
An example implementation can be found in src/arithmetic_paxos.erl and in tposs_pcrf_vnode.erl (which may not be available outside Travelping).

When the paxos server should be started, first run:

>dike:start().

You will need to make sure 5 nodes are started with the same master list (containing these 5 nodes) which is configured via the masters key in the dike application.
Dike supports a single node mode now, for using only put the single node in the masters list.

Check if Dike works and which groups are currently running in it by executing:

>dike_dispatcher:get_routing_table().
and
>dike_dispatcher:refresh_routing_table().

adding groups (sets up paxos_server module under a specific name):
>dike_master:add_group(GroupName, PaxosServerModule).


adding nodes (node is registered at dike_master which may start group instances on the added node):
>dike_master:join(Node).

sending a message to a started paxos_server (for this to work dike needs to be started on the requesting node but the node must not have joined):
dike_dispatcher:request(GroupName, MSG).

## Failure tolerance

Dike guarantees for the system to be operational with up to two failing nodes, each group's state is replicated five times.

To achieve this Dike uses Paxos.
For more information have a look at the thesis which describes Dike.