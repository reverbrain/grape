Grape is a stepping stone to building data processing systems on top of Elliptics routing and [server-side code execution](http://doc.reverbrain.com/elliptics:serverside).

Its main goal is to provide an active example of elliptics data processing capabilities and also to provide ready-to-use building blocks for such systems.

Grape, as for now, consist of 2 components:

* fault-tolerant persistent queue
* and a connector that allows to direct queue output into user application running on elliptics cluster (see [event driver concept](http://doc.reverbrain.com/stub:cocaine-event-driver) in Cocaine docs)

### Queue
Queue is a [cocaine](https://github.com/cocaine/cocaine-core) application running on elliptics node. Its deployment process follows [general process](http://doc.reverbrain.com/stub:cocaine-app-deployment-process) for cocaine applications.

Once deployed and started queue accepts data entries pushed into it, stores them among nodes of elliptics cluster its working on, and gives data entries back on consumer request, maintaining entries original order.

Queue supports fault-tolerance by using data replication and by implementing fault-replay mechanics: consumer must acknowledge processing status of every data entry that it retrieved from the queue - failing to do so will result in entry "replay", over and over again up until it'll be confirmed.

(For further details about how this works internally see [TODO: How queue works]().) 

#### API
Queue's API basically consist of three methods: `push`, `peek`, `ack`:

 * `push` pushes data entry to the top of the queue
 * `peek` gets data entry from the bottom of the queue
 * `ack` confirms that entry has been processed and could be dropped

(Latter two are combined in additional short-circuit method `pop`.)

These methods are implemented in two sets: simple one operates in single queue entries and more complex one operates in multi-entry blocks.

##### queue.push
```
dnet_id key;
session->exec(&key, "queue@push", ioremap::elliptics::data_pointer::from_raw("abcd")).wait();
```
Pushes data entry ("abcd") to the queue running under the base name "queue" at node responsible for the specified `dnet_id`.

There is no multi-entry variant for this method.

##### queue.peek
```
dnet_id key;
ioremap::elliptics::exec_context context = session->exec(
        &key, "queue@peek", ioremap::elliptics::data_pointer()
        ).get_one().context();
ioremap::elliptics::data_pointer entry_data = context.data();
ioremap::grape::entry_id entry_id = ioremap::grape::entry_id::from_dnet_raw_id(context.src_id());
```
Peeks data entry from the queue running under the base name "queue" at node responsible for the specified `dnet_id`.

Returns entry id embedded in `src_id` field of the response. Also returns queue's supplemental subid in the `src_key` field (that subid makes possible to acknowledge entry back and thus must be preserved). Both fields are accessible through `exec_context`.

(Details of the [TODO: request and response fields](https://github.com/reverbrain/elliptics/blob/master/include/elliptics/srw.h#L30) of the exec command explained separately.)

##### queue.ack
```
session->exec(context, "queue@ack", ioremap::elliptics::data_pointer()).wait();
```
or equivalent:
```
session->exec(context.src_id(), context.src_key(), "queue@ack", ioremap::elliptics::data_pointer()).wait();
```
Acknowledges entry received by a previous `peek`.

Entry id must be sent embedded in `dnet_id` of the request. `src_key` must be set to that received by a previous `peek`.

##### queue.peek-multi
```
dnet_id key;
ioremap::elliptics::exec_context context = session->exec(
        &key, "queue@peek-multi", ioremap::elliptics::data_pointer("100")
        ).get_one().context();
auto array = ioremap::grape::deserialize<ioremap::grape::data_array>(context.data());
ioremap::elliptics::data_pointer d = array.data();
size_t offset = 0;
for (size_t i = 0; i < array.sizes().size(); ++i) {
    int bytesize = array.sizes()[i];
    // process data: (d.data() + offset, bytesize)
    offset += bytesize;
}
```
Peeks multiple data entries from the queue running under the base name "queue" at node responsible for the specified `dnet_id`.

Peek-multi has an argument: hint about number of entries, which must be presented in a string form.

Returns serialized `ioremap::grape::data_array` structure which holds entries' data packed into byte array and array with entries' byte sizes and array with entries' ids.

`ioremap::grape::data_array` is declared in a header file `include/grape/data_array.hpp`.

##### queue.ack-multi
```
ioremap::grape::data_array array = ...;
session->exec(context, "queue@ack-multi", ioremap::grape::serialize(array.ids())).wait();
```
Acknowledges entries received by a previous `peek` (may be several).

##### queue.pop and queue.pop-multi
Short circuit methods `pop` and `pop-multi` has a combined effect of `peek` and `ack` called in one go. They are simple to use but also lose acking and replaying properties.

#### Additional methods
Queue also implements few techical methods (in addition to common [TODO: Cocaine and Elliptics app managment]() capabilities):

 * `ping` can be used to see if queue is currently active (or activate it for that matter)
 * `stats` shows internal state and statistics queue gathers about itself

#### Configuration

Queue reads its configuration from the file `queue.conf`. This file must be included in deployment tarball along with an app executable (see following section on Deployment).

`queue.conf` must contain configuration for the elliptics client (used to return replies on inbound events) and can include queue configuration options.

There is only one configuration option for now:

 * `chunk-max-size` (int) - specifies how many entries will contain single chunk in the queue (default value: 10000)

#### Deployment
Deployment process of the queue follows [general process](http://doc.reverbrain.com/stub:cocaine-app-deployment-process) for cocaine applications. For launching the queue user needs three files:

 * `queue` application file (which is an executable)
 * `queue.conf` config file (which is also a manifest file)
 * `queue.profile` execution profile file

`queue` app could be taken from the binary package `grape-components` or built from the sources. Config and profile files also exist both in source repository and included in the same package.

Here we presume that user have installation of elliptics running on `localhost:1025` in group `2` (how to do it see [Elliptics: Server setup tutorial](http://doc.reverbrain.com/elliptics:server-tutorial)).

`queue.conf` content:
```
{
    "type": "binary",
    "slave": "queue",

    "remotes": [
        "localhost:1025:2"
     ],
    "groups": [2]
}
```

`queue.profile` content:
```
{
    "heartbeat-timeout" : 60,
    "pool-limit" : 1,
    "queue-limit" : 1000,
    "grow-threshold" : 1,
    "concurrency" : 10,
    "idle-timeout": 0
}
```

Steps to launch a queue:

1. Create tarball with queue executable and config files:

 ```
tar cvjf queue.tar.bz2 queue queue.conf
```

2. Upload tarball, manifest (same as config) and profile

 ```
cocaine-tool -n queue -m queue.conf -p queue.tar.bz2 app:upload
cocaine-tool -n queue -m queue.profile profile:upload
```

3. Deploy the app (get it ready to run)

 ```
dnet_ioclient -r localhost:1025:2 -g 2 -c "queue@start-multiple-task local"
```

(More details about what these commands do exactly see in [TODO: Cocaine: application deployment]() and [TODO: Elliptics task management]().)

Now queue is deployed (on every node that this elliptics installation includes, most possible that would be a single node here) and will actually start as soon as it'll receive its first command (or event).

Activate the queue:
```
dnet_ioclient -r localhost:1025:2 -g 2 -c "queue@ping"
```
Queue is up and running if reply would be:
```
127.0.0.1:1025: queue@ping "ok"
```

---

Links:

 * Elliptics: http://www.reverbrain.com/elliptics/
 * Cocaine: https://github.com/cocaine/cocaine-core
 * Google group: https://groups.google.com/forum/?fromgroups=#!forum/reverbrain
