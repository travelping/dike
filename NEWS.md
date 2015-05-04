dike
====

A framework for distributed computing and storage.

Version 1.3.0 - 25 Mar 2015
---------------------------

* add dike_dispatcher:get_min_node_set_all_groups/0 to find a small cover of all groups in the hashring (for efficient broadcasting)

Version 1.2.2 - 27 Nov 2014
---------------------------

* add generic helpers for building a test cases with dike-based applications (dike_test)
* use dike_test in dike_SUITE
* add lager logging for test-nodes
* remove sasl_syslog dependency

Version 1.2.1 - 27 Nov 2014
---------------------------

* garbage collect in gen_paxos and paxos_server processes in intervals defined by garbage_collect_interval

Version 1.2.0 - 26 Nov 2014
---------------------------

* cleanup, make dike publicly accessible

Version 1.1.2 - 20 Jun 2014
---------------------------

* add timeout to not overload a CPU

Version 1.1.1 - 20 Jun 2014
---------------------------

* Fix logging
* Add pmap for going throw a list of funs

Version 1.1.0 - 17 Sep 2013
---------------------------

* Support for single node and distributed mode
* Fixes multiple memory leaks leading to stable memory consumption
* Support for persistent and non-persistent storage of state through multiple storage backends
* Configurable internal group settings
