FROM apache/flink:1.20.5-java17

ARG MAVEN=https://repo1.maven.org/maven2

ADD --chown=flink:flink ${MAVEN}/org/apache/fluss/fluss-flink-1.20/1.0.0/fluss-flink-1.20-1.0.0.jar /opt/flink/lib/
ADD --chown=flink:flink ${MAVEN}/org/apache/flink/flink-sql-connector-oracle-cdc/3.6.0-1.20/flink-sql-connector-oracle-cdc-3.6.0-1.20.jar /opt/flink/lib/
ADD --chown=flink:flink ${MAVEN}/com/oracle/database/jdbc/ojdbc8/19.3.0.0/ojdbc8-19.3.0.0.jar /opt/flink/lib/
ADD --chown=flink:flink ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/
