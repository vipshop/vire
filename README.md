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

## Support redis command so far

### Connection

+ ping

### Server

+ info

### Key

+ del

### String

+ get
+ set
+ setnx
+ setex
+ psetex

## Help

    Usage: vire [-?hVdDt] [-v verbosity level] [-o output file]
                [-c conf file] [-s manage port] [-a manage addr]
                [-i interval] [-p pid file] [-T worker threads number]
    
    Options:
    -h, --help             : this help
    -V, --version          : show version and exit
    -t, --test-conf        : test configuration for syntax errors and exit
    -d, --daemonize        : run as a daemon
    -D, --describe-stats   : print stats description and exit
    -v, --verbose=N        : set logging level (default: 5, min: 0, max: 11)
    -o, --output=S         : set logging file (default: stderr)
    -c, --conf-file=S      : set configuration file (default: conf/vire.conf)
    -s, --port=N           : set manage port (default: 8889)
    -a, --addr=S           : set manage ip (default: 0.0.0.0)
    -i, --interval=N       : set interval in msec (default: 30000 msec)
    -p, --pid-file=S       : set pid file (default: off)
    -T, --thread_num=N     : set the worker threads number (default: 6)

## License

Copyright 2016 DEEP, Inc.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
