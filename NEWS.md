dike
====

A framework for distributed computing and storage.


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
