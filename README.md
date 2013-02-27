libcuckoo
=========

High-performance Concurrent Cuckoo Hashing Library

This library provides a compact hash table that allows multiple
concurrent reader threads, while allowing one thread at a time
to make inserts or updates.  It is particularly useful for
applications with high read-to-write ratios that store small key/value
pairs in the hash table.

Authors: Bin Fan (binfan@cs.cmu.edu) David G. Andersen (dga@cs.cmu.edu) and Michael Kaminsky (michael.e.kaminsky@intel.com)

For details about this algorithm and citations, please refer to [our paper in NSDI 2013][1].

   [1]: http://www.cs.cmu.edu/~dga/papers/memc3-nsdi20013.pdf "MemC3: Compact and Concurrent Memcache with Dumber Caching and Smarter Hashing"

Building
--------

    $ autoreconf -fis
    $ ./configure
    $ make
