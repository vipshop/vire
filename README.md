# vire

**vire** (pronounced "vip-redis") is a multithread redis maintains in vipshop.

## Build

To build vire from source with _debug logs enabled_ and _assertions enabled_:

    $ git clone  https://github.com/vipshop/vire.git
    $ cd vire
    $ autoreconf -fvi
    $ ./configure --enable-debug=full
    $ make
    $ src/vire -h

A quick checklist:

+ Use newer version of gcc (older version of gcc has problems)
+ Use CFLAGS="-O1" ./configure && make
+ Use CFLAGS="-O3 -fno-strict-aliasing" ./configure && make
+ `autoreconf -fvi && ./configure` needs `automake` and `libtool` to be installed

## Run

    $ src/vire -c conf/vire.conf -o log -T 6 -d

## Features

+ Multithread.
+ Fast.
+ Works with Linux, *BSD, OS X and SmartOS (Solaris)

## Help

    Usage: vire [-?hVdt] [-v verbosity level] [-o output file]
                [-c conf file] [-p pid file]
                [-T worker threads number]
    
    Options:
    -h, --help             : this help
    -V, --version          : show version and exit
    -t, --test-conf        : test configuration for syntax errors and exit
    -d, --daemonize        : run as a daemon
    -v, --verbose=N        : set logging level (default: 5, min: 0, max: 11)
    -o, --output=S         : set logging file (default: stderr)
    -c, --conf-file=S      : set configuration file (default: conf/vire.conf)
    -p, --pid-file=S       : set pid file (default: off)
    -T, --thread_num=N     : set the worker threads number (default: 6)

## Support redis command so far

#### Connection

+ ping
+ quit
+ echo

#### Server

+ info
+ flushall
+ time

#### Key

+ del
+ exists
+ ttl
+ pttl
+ expire
+ expireat
+ pexpire
+ pexpireat
+ persist
+ randomkey

#### String

+ get
+ set
+ setnx
+ setex
+ psetex
+ incr
+ decr
+ incrby
+ decrby
+ append
+ strlen

## License

Copyright 2016 DEEP, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
