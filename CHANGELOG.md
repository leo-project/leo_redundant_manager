CHANGELOG
=========

1.8.1 (May 7, 2014)
--------------------

* Improved
    * Modified temrmination of the application


1.8.0 (Apr 2, 2014)
--------------------

* New features
    * Implemented functions of multi-dc-replication
* Improved
    * Renamed modules to be suitable


1.4.1 (Feb 18, 2014)
--------------------

* Improved
    * Improved the performance of membership and rebalance
    * Restricted membership of a node, the state of which is not running
    * Bump leo_rpc and leo_mq


1.4.0 (Jan 22, 2014)
--------------------

* New Features
    * Able to communicate remote-clusters with each manager

* Improved
    * Improved relbalance-fun and management of records
    * Revised timeout-value in the worker
    * Implemented interval of the consecutive procs


1.2.0 (Nov 8, 2013)
--------------------

* Improved
    * Able to manage current cluster members and previous cluster members
        * In order not to happen lost data, supported to manage current-ring(for write operation) and previous-ring(for read operation)
    * Support configurable output of log directory


0.9.0  (Jul 4, 2012)
--------------------

* Initial release
