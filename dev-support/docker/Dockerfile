# Use an official OpenJDK runtime as a parent image
FROM openjdk:17-jdk-slim

# Set the working directory in the container
WORKDIR /home/datagen

# Default env values
ENV MIN_MEM="512m"
ENV MAX_MEM="2G"
ENV PROFILE="standalone"
ENV LOGGING_FILE="/home/datagen/logback-spring.xml"
ENV CONFIG_FILE="/home/datagen/application-standalone.properties"

# Create a user and group for running the application
RUN groupadd -r datagen && useradd -r -g datagen -d /home/datagen -s /sbin/nologin -c "Docker image user" datagen

# Copy the Java application JAR file into the container
COPY datagen.jar datagen.jar
# Copy config files
COPY application-standalone.properties application-standalone.properties
COPY logback-spring.xml logback-spring.xml

# Create a directory for logs
RUN mkdir -p /var/log/datagen && chown datagen:datagen /var/log/datagen

# Change ownership of the working directory to the new user
RUN chown -R datagen:datagen /home/datagen

# Switch to the newly created user
USER datagen

# Expose the port your application will run on
EXPOSE 4242

# Command to run the Java application
CMD ["sh", "-c", "java -Dnashorn.args=--no-deprecation-warning --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED -Xms$MIN_MEM -Xmx$MAX_MEM -Dlogging.config=file:$LOGGING_FILE-jar datagen.jar --spring.profiles.active=$PROFILE --spring.config.location=file:$CONFIG_FILE"]