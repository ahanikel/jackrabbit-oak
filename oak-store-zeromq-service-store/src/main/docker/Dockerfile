FROM openjdk:11

WORKDIR /root
COPY test-diff-1.38.0.log ./

ENV debug="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=*:6006"
ENV javaOpts="-Xmx16g -Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.dateTimeFormat=yyyy-MM-dd'T'HH:mm:ss.SSSZ"
ENV jarOpts="log test-diff-1.38.0.log"
ENV KAFKA_TOPIC=nodestates
ENV KAFKA_SERVERS=localhost:9092

ENTRYPOINT java ${debug} ${javaOpts} -jar oak-store-zeromq-service-store-1.38.0-zeromq-jar-with-dependencies.jar ${jarOpts}

EXPOSE 8000
EXPOSE 8001
EXPOSE 6006

COPY oak-store-zeromq-service-store-1.38.0-zeromq-jar-with-dependencies.jar ./
