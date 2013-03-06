count: A Riak Core Application
======================================

Prototyping playground for append only counters. Hope to lead into a idempotent counter,
and general append only CRDTs.

Basic idea is - on update append to an op log. On Read slurp op-log into a state based
CRDT.
