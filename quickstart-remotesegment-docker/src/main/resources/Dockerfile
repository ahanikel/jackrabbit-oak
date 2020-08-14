FROM openjdk:11
ENV slingarchive=cq-quickstart-6.6.0-1.30.0.nosegment.jar
ENV zeromqarchive=oak-store-zeromq-1.30.0.jar
ENV configfile=org.apache.jackrabbit.oak.store.zeromq.ZeroMQNodeStore.config
ENV license=license.properties
ENV javaargs="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=*:5005"
ENV jarargs=""
ENV backendPrefix="backend"
ENV journalPrefix="journal"
ENV blobendPrefix="blobend"
ENV clusterInstances="1"
EXPOSE 4502
EXPOSE 5005
WORKDIR /root
RUN mkdir -p crx-quickstart/install/15
COPY ${slingarchive} ./
COPY ${zeromqarchive} crx-quickstart/install/15/
COPY ${configfile} crx-quickstart/install/
COPY ${license} ./
ENTRYPOINT echo admin | java ${javaargs} -DclusterInstances=${clusterInstances} -DbackendPrefix=${backendPrefix} -DjournalPrefix=${journalPrefix} -DblobendPrefix=${blobendPrefix} -jar ${slingarchive} ${jarargs}
