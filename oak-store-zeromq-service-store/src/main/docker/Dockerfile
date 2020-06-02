FROM docker-asr-release.dr.corp.adobe.com/asr/java_v11:1.7
ENV slingarchive=@project.artifactId@-@project.version@.jar
ENV javaargs=""
ENV jarargs=""
EXPOSE 8000-8079
WORKDIR /root
COPY ${slingarchive} ./
ENTRYPOINT java ${javaargs} -jar ${slingarchive} ${jarargs}
