# --- Build Stage ---
# Use an official Gradle image with a compatible JDK
FROM gradle:8.5-jdk21 AS builder
WORKDIR /app

# Copy gradle wrapper files and build script first
# This leverages Docker layer caching for dependencies
COPY build.gradle settings.gradle gradlew ./
# If you have a settings.gradle, copy it too, otherwise ignore
# COPY settings.gradle ./
COPY gradle ./gradle

# Grant execute permission to the gradlew script
RUN chmod +x ./gradlew

# Optionally, download dependencies first (can speed up subsequent builds if source hasn't changed)
# ./gradlew build --no-daemon || return 0
# The "|| return 0" handles cases where the build might fail early but dependencies are downloaded

# Copy the source code
COPY src ./src

# Build the application using the shadowJar task
# Use --no-daemon for cleaner Docker/CI builds
RUN ./gradlew shadowJar --no-daemon

# --- Runtime Stage ---
# Use a slim JRE image for the final container
FROM openjdk:21-jdk-slim
WORKDIR /app

# Copy the packaged JAR from the builder stage
# The path corresponds to the output of the shadowJar task
# Check the name configured in build.gradle (archiveBaseName, archiveVersion)
COPY --from=builder /app/build/libs/kafka-producer-poc-1.0-SNAPSHOT.jar app.jar

# Command to run the application
CMD ["java", "-jar", "app.jar"]