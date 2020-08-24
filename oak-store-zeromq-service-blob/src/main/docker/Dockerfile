FROM openjdk:11
ENV slingarchive=@project.artifactId@-@project.version@.jar
ENV javaargs=""
ENV jarargs=""
EXPOSE 10000-10079
WORKDIR /root
COPY ${slingarchive} ./
ENTRYPOINT java ${javaargs} -jar ${slingarchive} ${jarargs}
