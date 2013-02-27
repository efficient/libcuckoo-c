libcuckoo
=========

High-performance Concurrent Cuckoo Hashing Library

Authors: Bin Fan (binfan@cs.cmu.edu) Dave Andersen (dga@cs.cmu.edu) and Michael Kaminsky (michael.e.kaminsky@intel.com)

For details about this algorithm and citations, please refer to [our paper in NSDI 13][1].

   [1]: http://www.cs.cmu.edu/~dga/papers/memc3-nsdi20013.pdf "MemC3: Compact and Concurrent Memcache with Dumber Caching and Smarter Hashing"

Building
--------

    $ autoreconf -fis
    $ ./configure
    $ make
