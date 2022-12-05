# Oak ZeroMQ Nodestore

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
