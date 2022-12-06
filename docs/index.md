# Oak ZeroMQ Nodestore

The ZeroMQ nodestore is a distributed nodestore for Apache Oak using ZeroMQ as its communication layer.

Nodestates contain properties and references to their child nodestates. They are serialised and identified by the hash of their serialisation (content-addressable storage). Therefore they can be treated like any other blob.

A reference to the root nodestate is stored separately in a journal and defines a repository. Several root nodestates and therefore journals/repositories can share the same blobstore bucket. These root nodestates can even belong to different tenants if the serialised nodestates are encrypted. A repository can be cloned in no time by creating a new journal referencing the same root nodestate.

Changes are written to a log, which gives them a global order. When a commit is merged, the nodestates are written first, then the new journal entry. All participating Oak instances listen to and process the log and its commits, including their own, in the same manner, and therefore eventually have the same state. In the case of conflicts, all instances use the same resolution mechanism. 

In addition to Oak instances, other processes listen to the log:
- a blobstore writer writes nodestates and blobs to the blob store, where they are available for reads
- some process updates the indexes
- some process removes unreferenced blobs from the blob store
- some process gathers statistics and sends them to a separate queue
- and so on.

## PoC implementation
The "comm-hub" process is the communication backbone. It opens two sockets, one for writing messages to the queue, and one for reading.

The message format is a bit verbose but makes debugging easier. Fields are separated by a space. The first field starts with a service identifier (currently "read" or "write"), a dash "-", and a type ("req" for request, "rep" for reply). The second field identifies the requesting thread (<pid>@<host>-<threadId>). The third field should be a message id set by the requester and echoed in the reply, but I think that's not implemented yet. The remaining fields are service dependent.

The reader service 
## Build it

```shell
git clone https://github.com/ahanikel/jackrabbit-oak
cd jackrabbit-oak
mvn -DskipTests clean install
```
## Run the backend processes
```shell
cd oak-run/target
```
### Communication hub
Open a new terminal window and run
```shell
java -jar oak-run-1.42.0-zeromq.jar comm-hub tcp://localhost:8000 tcp://localhost:8001
```
### Reader service
Open a new terminal window and run
```shell
java -jar oak-run-1.42.0-zeromq.jar simple-blob-reader-service simple:///tmp/blobstore tcp://localhost:8000 tcp://localhost:8001
```
assuming that `/tmp/blobstore` does not exist.
### Writer service
Open a new terminal window and run
```shell
java -jar oak-run-1.42.0-zeromq.jar simple-blob-writer-service simple:///tmp/blobstore tcp://localhost:8000 tcp://localhost:8001
```
### ZeroMQ Listener for debugging
If you want to see live what messages are being sent back and forth, open a new terminal and run
```shell
java -jar oak-run-1.42.0-zeromq.jar simple-queue-listener tcp://localhost:8001
```
but beware that this will produce lots of output and will slow down the initial sling start
considerably. Press `CTRL-C` when you've seen enough.
## Run the frontend it with Sling
```shell
cd org-apache-sling-starter
mvn -DskipTests clean install
cd target
# edit slingfeature-tmp/feature-oak_tar.json (TBD)
JAVA_HOME=/usr/local/opt/openjdk@8 $JAVA_HOME/bin/java -jar dependency/org.apache.sling.fe
ature.launcher.jar -f slingfeature-tmp/feature-oak_tar.json
```
