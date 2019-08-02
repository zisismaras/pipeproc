# PipeProc

> Multi-process log processing for nodejs

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Intro](#intro)
- [Example](#example)
- [Installing](#installing)
- [Status](#status)
- [Process management](#process-management)
  - [spawn](#spawn)
  - [connect](#connect)
  - [shutdown](#shutdown)
- [Committing logs](#committing-logs)
  - [commit examples](#commit-examples)
- [Read API](#read-api)
  - [range](#range)
    - [range signature](#range-signature)
    - [range examples](#range-examples)
  - [revrange](#revrange)
  - [length](#length)
- [Procs](#procs)
  - [offsets](#offsets)
  - [ack](#ack)
  - [ackCommit](#ackcommit)
  - [reclaim](#reclaim)
    - [reclaim settings](#reclaim-settings)
  - [destroying procs](#destroying-procs)
  - [inspecting procs](#inspecting-procs)
  - [resuming/disabling procs](#resumingdisabling-procs)
- [SystemProcs](#systemprocs)
  - [systemProc example](#systemproc-example)
  - [Inline processors](#inline-processors)
- [LiveProcs](#liveprocs)
  - [liveProc signature](#liveproc-signature)
  - [liveProc example](#liveproc-example)
- [Waiting for procs to complete](#waiting-for-procs-to-complete)
- [GC](#gc)
  - [Caveats/problems](#caveatsproblems)
- [Typings](#typings)
- [Tests](#tests)
- [Meta](#meta)
- [Contributing](#contributing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Intro

PipeProc is a data processing system that can be embedded in nodejs applications (eg. electron).  
It will be run in a separate process and can be used to off-load processing logic from the main “thread” in a structured manner.

Underneath it uses a [structured commit log](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) and a “topic” abstraction to categorize logs.

Inspired by [Apache Kafka](https://kafka.apache.org/) and [Redis streams](https://redis.io/topics/streams-intro).

In practice it is a totally different kind of system since it is meant to be run embedded in the main application as a single instance node.  
Another key difference is that it also handles the execution of the processing logic by itself and not only the stream pipelining.  
It does this by using processors which are custom-written modules/functions that can be plugged to the system, consume topic streams, execute custom logic and push the results to another topic, thus creating a processing pipeline.

## Example

```javascript
const {PipeProc} = require("pipeproc");
const pipeProcClient = PipeProc();

pipeProcClient.spawn().then(function() {
    //commit a log to topic "my_topic_1"
    //the topic is created if it does not exists
    pipeProcClient.commit({
        topic: "my_topic",
        body: {greeting: "hello"}
    }).then(function(id) {
        console.log(id);
        //1518951480106-0
        //{timestamp}-{sequenceNumber}
    });
});
```

## Installing

```bash
npm install --save pipeproc
```

## Status

> Linux  
[![Build Status](https://dev.azure.com/zisismaras/pipeproc/_apis/build/status/zisismaras.pipeproc.linux)](https://dev.azure.com/zisismaras/pipeproc/_build/latest?definitionId=4)  
> OSX  
[![Build Status](https://dev.azure.com/zisismaras/pipeproc/_apis/build/status/zisismaras.pipeproc.mac)](https://dev.azure.com/zisismaras/pipeproc/_build/latest?definitionId=5)  
> Windows  
[![Build Status](https://dev.azure.com/zisismaras/pipeproc/_apis/build/status/zisismaras.pipeproc.windows)](https://dev.azure.com/zisismaras/pipeproc/_build/latest?definitionId=6)  
> npm  
[![npm version](https://badge.fury.io/js/pipeproc.svg)](https://badge.fury.io/js/pipeproc)

## Process management

### spawn

Spawn the node and connect to it.  
If there is a need to spawn multiple nodes on the same host you can use the `namespace` option with a custom name.  
If a custom `namespace` is used, all clients that will `connect()` to it will need to provide it.  
The node can also use TCP connections instead by setting the host and the port in the `tcp` settings.  
Clients (local and remote) can then `connect()` to it by providing the same host and port.  
A socket address can also be used instead of a namespace or tcp options, for example:

- `ipc:///tmp/mysocket`
- `tcp://127.0.0.1:9999`

TLS is also available when using TCP. A cert, key, and ca should be provided for both server and client.  
Any client that also needs to `connect()` should provide its client keys.  
If the node is spawned with TLS, only secure connections will be allowed.

```typescript
spawn(
    options?: {
        //use a different ipc namespace
        namespace?: string,
        //use a tcp socket
        tcp?: {
            host: string,
            port: number
        },
        //tls settings
        tls?: {
            server: {
                key: string;
                cert: string;
                ca: string;
            },
            client: {
                key: string;
                cert: string;
                ca: string;
            }
        }
        //use a socket address directly
        socket?: string,
        //use an in-memory store instead of the disk adapter
        memory?: boolean,
        //set the location of the underlying store (if memory is false)
        location?: string,
        //the number of workers(processes) to use (check the systemProc section below), set to 0 for no workers, defaults to 1
        workers?: number,
        //the number of processors that can be run concurrently by each worker, defaults to 1
        workerConcurrency?: number,
        //restart any worker that reaches X systemProc executions, useful to mitigate memory leaks, defaults to 0 (no restarts)
        workerRestartAfter?: number,
        //tune the garbage collector settings (check the gc section below)
        gc?: {minPruneTime?: number, interval?: number} | boolean
    }
): Promise<string>;
```

### connect

Connect to an already spawned node.  

Usecase: Connect to the same PipeProc instance from a different process (eg. electron renderer) or to a remote instance (only when using TCP)

```typescript
connect(
    options?: {
        //use a different ipc namespace
        namespace?: string,
        //connect to a tcp socket
        tcp?: {
            host: string,
            port: number
        },
        //tls settings
        tls?: {
            key: string;
            cert: string;
            ca: string;
        } | false,
        //use a socket address directly
        socket?: string,
        //specify a connection timeout, defaults to 1000ms
        timeout?: number
    }
): Promise<string>;
```

### shutdown

Gracefully close the PipeProc instance.

```typescript
shutdown(): Promise<string>;
```

## Committing logs

This is how you add logs to a topic.  
The topic will be created implicitly when its first log is committed.  
Multiple logs can be committed in a batch, either in the same topic or to different topics, in that case
the write will be an atomic operation and either all logs will be successfully written or all will fail.

### commit examples

Add a single log to a topic:

```javascript
pipeProcClient.commit({
    topic: "my_topic",
    body: {greeting: "hello"}
}).then(function(id) {
    console.log(id);
    //=> 1518951480106-0
});
```

`commit()` will return the id(s) of the log(s) committed.  
Ids follow a format of `{timestamp}-{sequenceNumber}` where timestamp is the time the log was committed in milliseconds
and the sequence number is an auto-incrementing integer (starting from 0) indicating the log's position in its topic.  
The log's body can be an arbitrarily nested javascript object.

Adding multiple logs to the same topic:

```javascript
pipeProcClient.commit([{
    topic: "my_topic",
    body: {
        myData: "some data"
    }
}, {
    topic: "my_topic",
    body: {
        myData: "more data"
    }
}]).then(function(ids) {
    console.log(ids);
    //=> ["1518951480106-0", "1518951480106-1"]
});
```

Notice the timestamps are the same since the two logs where inserted at the same time but the sequence number is different and auto-increments.  

Adding multiple logs to different topics:

```javascript
pipeProcClient.commit([{
    topic: "my_topic",
    body: {
        myData: "some data"
    }
}, {
    topic: "another_topic",
    body: {
        myData: "some data for another topic"
    }
}]).then(function(ids) {
    console.log(ids);
    //=> ["1518951480106-0", "1518951480106-0"]
});
```

As before, the timestamps are the same (since they were committed at the same time) but the sequence numbers are both `0` since these two logs are the first logs committed in their respective topics.

## Read API

### range

Get a slice of a topic.

#### range signature

```typescript
range(
    topic: string,
    options?: {
        start?: string,
        end?: string,
        limit?: number,
        exclusive?: boolean
    }
): Promise<{id: string, body: object}[]>;
```

#### range examples

```javascript
pipeProcClient.range("my_topic", {
  start: "1518951480106-0",
  end: "1518951480107-10"
})

//timestamps only
pipeProcClient.range("my_topic", {
  start: "1518951480106",
  end: "1518951480107"
})


//from beginning to end
pipeProcClient.range("my_topic")

//from specific timestamp to the end
pipeProcClient.range("my_topic", {
  start: "1518951480106"
})

//with a limit
pipeProcClient.range("my_topic", {
  start: "1518951480106",
  limit: 5
})

//by sequence id
pipeProcClient.range("my_topic", {
  start: ":5",
  end: ":15"
}) //=> [5..15]

//by sequence id exclusive
pipeProcClient.range("my_topic", {
  start: ":5",
  end: ":15"
  exclusive: true
}) //=> [6..14]

//returns a Promise that resolves to an array of logs
[{
  id: "1518951480106-0",
  body: {
    myData: "hello"
  }
}]
```

### revrange

Ranges through the topic in an inverted order.  
`start` and `end` should also be inverted. (start >= end).  
The API is the same as `range()`.  
eg. to get the latest log

```javascript
pipeProcClient.revrange("my_topic", {
  limit: 1
})
```

### length

get the total logs in a topic

```javascript
pipeProcClient.length("my_topic").then(function(length) {
  console.log(length);
});
```

## Procs

Procs are the way to consistently process logs of a topic.  
Let's start with an example and explain as we go along.

```javascript
//lets add some logs
await pipeProcClient.commit([{
    topic: "numbers",
    body: {myNumber: 1}
}, {
    topic: "numbers",
    body: {myNumber: 2}
}]);

//run a proc on the "numbers" topic
const log = await pipeProcClient.proc("numbers", {
  name: "my_proc",
  offset: ">"
});
//=> log = {id: "1518951480106-0", body: {myNumber: 1}}

try {
    //process the log
    const incrementedNumber = log.data.myNumber + 1;
    //ack the operation and commit the result to a different topic
    await pipeProcClient.ackCommit("my_proc", {
        topic: "incremented_numbers",
        body: {myIncrementedNumber: incrementedNumber}
    });
} catch (err) {
    //something went wrong on our processing, the proc should be reclaimed
    console.error(err);
    pipeProcClient.reclaim("my_proc");
}
```

Procs are the way to consistently fetch logs from a topic, process them and commit the results in a safe and serial manner.  
So, what's going on in the above example?  

- first we add a log to our "numbers" topic
- then we create a proc named "my_proc" with an offset of ">" (it means start fetching from the very beginning of the topic, see more below) for the "numbers" topic
- the proc returns a log (the log we added on the first commit)
- we do some processing (incrementing the number)
- we then acknowledge the operation and commit our result to a different topic
- we are also catching errors in our processing and the ack, in that case the proc must be reclaimed.

If everything goes well, the next time we call the proc it will fetch us our second log `1518951480106-1`.  
If something goes wrong and `reclaim()` is called the proc will be "reset" and will fetch the first log again.  
Until we call `ack()` (or `ackCommit()` in this case) to move on or `reclaim()` to reset, the proc will not fetch us any new logs.  

Here is the whole proc signature:

```typescript
proc(
    //for what topic this proc is for
    topic: string,
    options: {
        //the proc name
        name: string,
        //the proc offset (See below)
        offset: string,
        //how many logs to fetch
        count?: number,
        //reclaim settings, see below
        maxReclaims?: number,
        reclaimTimeout?: number,
        onMaxReclaimsReached?: "disable" | "continue"
    }
): Promise<null | {id: string, body: object} | {id: string, body: object}[]>;
```

### offsets

offsets are how you position the proc to a specific point in the topic.  

- `>` fetch the next log after the latest acked log for this proc. If no logs have been acked yet, it will start from the beginning of the topic.
- `$>` like `>` but it will start from new logs and not from the beginning (logs created after the proc’s creation)
- `{{specific_log/timestamp}}` - follows the `range()` syntax. It can be a full log name, a partial timestamp or a sequence id(`:{{id}}`). The next non-acked log AFTER the match will be returned.

### ack

Acking the proc is an explicit operation and should be run after the log has successfully been processed by calling `ack()` or `ackCommit()`.

```typescript
ack(
    procName: string
): Promise<string>;
```

Returns the logId of the log we just acked. If our proc fetched multiple logs (using count > 1) all of the logs will be acknowledged as processed and the call instead of an id will return a range (`1518951480106-0..1518951480106-1`).  
The next time the proc is executed it will fetch the next log after the above logId(or range).

### ackCommit

`ackCommit()` combines an `ack()` and a `commit()` in an atomic operation. If either of these fail, both will fail.

### reclaim

If something goes wrong while we are processing our log(s) or a PipeProc error is raised when we ack/commit our result, we should call `reclaim`.  
This will reset the proc, allowing to retry the operation.

#### reclaim settings

In the proc's signature there are some settings for the reclaims, allowing us to control how reclaims work and not retrying failed operations forever or getting stuck.

- maxReclaims - how many times we can call reclaim on a proc before the `onMaxReclaimsReached` strategy is triggered (defaults to 10, set to -1 for no limit)
- reclaimTimeout - In order not to get stuck by a bad processing (failing to call `ack()` or `reclaim()`), the proc will be automatically be reclaimed after a certain amount of time by the system, this value sets the time.
- onMaxReclaimsReached - what to do when the maxReclaims are reached. By default it will "disable" the proc which will raise an error if we try to use the proc. Can be set to "continue" so we can keep reclaiming forever.

### destroying procs

Since procs are persisted and are not meant to be used as an once-off operation (use a simple range() for that) they need to be explicitly destroyed.

```javascript
pipeProcClient.destroyProc("my_proc") // throws if it doesn't exist
.then(function(status) {
  console.log(status);
});
.catch(function(err) {
  console.error(err);
});
```

If a destroyed proc is re-run it will be re-created anew without maintaining the previous state.

### inspecting procs

Inspect the internal proc's state (last claimed/acked ranges etc). Useful for debugging.

```javascript
pipeProcClient.inspectProc("my_proc") // throws if it doesn't exist
.then(function(proc) {
  console.log(proc);
});
.catch(function(err) {
  console.error(err);
});
```

### resuming/disabling procs

Manually disable the proc or resume it (eg. after reaching `maxReclaims`)

```javascript
pipeProcClient.disableProc("my_proc") // throws if it doesn't exist
//.resumeProc("my_proc") - throws if already active or doesn't exist
.then(function(proc) {
  //proc = same as inspectProc output
})
.catch(function(err) {
  console.error(err);
});
```

## SystemProcs

Manually executing and managing a proc can be tiresome.  
SystemProcs will take care of all creation/execution/management of procs while also distributing the load to multiple workers, let's take a look using the above proc example with incrementing numbers but now using a `systemProc` and a processor module.

### systemProc example

```javascript
pipeProcClient.systemProc({
  name: "my_system_proc",
  offset: ">",
  from: "numbers",
  processor: "/path/to/myProcessor.js",
  to: "incremented_numbers"
  //all the other standard Proc options
});
```

```javascript
//myProcessor.js
module.exports = function(log, done) {
  //log = {id: "1518951480106-0", body: {myNumber: 1}}
  done(null, {myIncrementedNumber: log.body.myNumber+ 1});
};
```

Processors can publish to multiple topics by setting the `to` field to an array of topics.  
If the `to` field is omitted, the processor will not publish any logs (eg. get a log, process it, write the result to a database)
Instead of using a `done` callback, you can also return a promise.  
If an error is returned in the done callback (or a rejected promise is returned) the proc will be reclaimed.

### Inline processors

Processors can also be inlined:

```javascript
pipeProcClient.systemProc({
    name: "number_writer",
    offset: ">",
    count: 1,
    maxReclaims: -1,
    reclaimTimeout: 5000,
    from: "numbers",
    to: "incremented_numbers",
    processor: (log, done) => {
        done(null, {n: log.body.myNumber + 1});
    }
});
```

## LiveProcs

With liveprocs you can react to topic changes while not having to keep executing the underlying proc.  
liveprocs are run in the process in which they are called and are not distributed to the workers like systemProcs.  

### liveProc signature

```typescript
liveProc(
    options: {
        topic: string,
        //"all" will point the proc to the beginning of the topic (">" offset)
        //"live" will start fetching logs created after the liveProc's creation ("$>" offset)
        mode: "live" | "all",
        //how many logs to fetch each time
        count?: number
    }
): ILiveProc;
```

### liveProc example

```javascript
pipeProcClient.liveProc({
    topic: "my_topic",
    mode: "all"
}).changes(function(err, logs, next) {
    if (err) {
        //reclaim has to be called manually
        return this.reclaim();
    } else if (logs) {
        //do something with the logs
        //ack() is also manual
        this.ack().then(function() {
          next();
        });
    }
});
```

Inside the `changes` function you can either return a promise or use the `next` callback to keep listening for changes.  

liveProc instances also have simpler versions of all of the proc's methods (that implicitly point to the underlying proc)

```typescript
interface ILiveProc {
    changes: (cb: ChangesCb) => ILiveProc;
    inspect: () => Promise<IProc>;
    destroy: () => Promise<IProc>;
    disable: () => Promise<IProc>;
    resume: () => Promise<IProc>;
    reclaim: () => Promise<string>;
    ack: () => Promise<string>;
    ackCommit: (commitLog: ICommitLog) => void;
    cancel: () => Promise<void>;
}
```

## Waiting for procs to complete

When you have multiple systemProcs and/or liveProcs running it is sometimes needed to know when all logs in their topics have been acked.  
For example when we need to shutdown and exit the application:  

```javascript
pipeProcClient.waitForProcs().then(function() {
  pipeProcClient.shutdown().then(function() {
    process.exit(0);
  });
});
```

`waitForProcs()` can take a proc name or an array of proc names and it will wait only for those to complete.  
If nothing is passed then it will wait for every active proc.

## GC

Logs are immutable and cannot be edited or deleted after creation, so a garbage collector is needed to make sure our topics don't grow too large.

Every time it runs it performs the following:

- for topics that have no procs attached it will collect all logs that have passed the `minPruneTime`
- for topics that have procs attached, it will collect all logs 2 positions behind the last claimed log range, but only if they have also passed the `minPruneTime`

You can configure the `minPruneTime` and gc `interval` when you `spawn` the PipeProc node.  
By default they are both set to 30000ms.  

**By default the gc is disabled.** It can be enabled by passing `true` on the `spawn`'s gc options or an `object` with prune time and interval settings.

### Caveats/problems

- topic, proc and systemProc metadata are left behind even if the topic is empty and/or no longer used
- the `length()` function will return an incorrect number if a part of the topic is collected
- there seems to be a problem with the gc timers on OSX, causing the tests to sometimes fail

## Typings

Since PipeProc is written in typescript all public interfaces are properly typed and should be loaded automatically in your editor.

## Tests

You can run the test suite with:

```bash
npm install --save-dev
npm run test
```

## Meta

Distributed under the The 3-Clause BSD License. See ``LICENSE`` for more information.

## Contributing

1. Fork it (<https://github.com/zisismaras/pipeproc/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request