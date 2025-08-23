FROM openjdk:23

ARG VERSION
ENV JAR_FILE=oak-store-zeromq-$VERSION-uberjar.jar

WORKDIR /

RUN rm -rf /sbin /usr/sbin /usr/libexec

COPY oak-store-zeromq-*-uberjar.jar .

ENTRYPOINT exec java -jar $JAR_FILE $*

EXPOSE 8000
EXPOSE 8001
