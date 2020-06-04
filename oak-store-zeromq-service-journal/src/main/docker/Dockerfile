FROM openjdk:11
ENV slingarchive=@project.artifactId@-@project.version@.jar
ENV javaargs=""
ENV jarargs=""
EXPOSE 9000
EXPOSE 9001
WORKDIR /root
COPY ${slingarchive} ./
ENTRYPOINT java ${javaargs} -jar ${slingarchive} ${jarargs}
