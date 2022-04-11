FROM maven:3-jdk-8 AS build
COPY com_walmart_reportservice/src /home/app/src
COPY com_walmart_reportservice/pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean package -DskipTests



FROM bitnami/spark:latest
WORKDIR /opt/app/
RUN mkdir jar
COPY --from=build /home/app/target/report-0.0.1-SNAPSHOT-shaded.jar /opt/app/jar/report-service.jar
RUN mkdir input
COPY data/* input/
RUN mkdir output
CMD /opt/bitnami/spark/bin/spark-submit --class com.walmart.App /opt/app/jar/report-service.jar $CONTRACT