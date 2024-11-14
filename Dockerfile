# Use the Flink base image for compatibility
FROM flink:1.19.1-scala_2.12

# Copy the JAR file from the host to the Docker image
COPY target/take-home-1.0-SNAPSHOT.jar /opt/flink/jobs/read-from-kafka.jar

# Specify default command to start job
CMD ["jobmanager"]
