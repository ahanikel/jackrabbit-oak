FROM openjdk:11
ENV slingarchive=cq-quickstart-6.6.0-1.38.0.nosegment.jar
#ENV zeromqarchive=oak-store-zeromq-1.38.0.jar
ENV configfile=org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeStore.config
ENV license=license.properties
ENV javaargs="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=*:5005"
ENV jarargs="-nofork -nobrowser -r author,crx3,crx3tar-nofds"
ENV backendPrefix="ahanikel-log-svc-"
ENV clusterInstances="1"
ENV writeBackJournal="false"
ENV writeBackNodes="false"
ENV remoteReads="true"
ENV logNodeStates="false"

EXPOSE 4502
EXPOSE 5005
WORKDIR /root
#RUN mkdir -p crx-quickstart/install/15
RUN apt update && apt install -y libzmq-jni && apt clean
COPY ${license} ./
ENV delay=0
ENTRYPOINT sleep $delay && echo admin | java ${javaargs} -jar ${slingarchive} ${jarargs}
COPY ${slingarchive} ./
#COPY ${zeromqarchive} crx-quickstart/install/15/
#COPY ${configfile} crx-quickstart/install/
