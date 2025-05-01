# --- Build Stage ---
FROM gradle:8.5-jdk21 AS builder
WORKDIR /app

COPY build.gradle settings.gradle gradlew ./
# COPY settings.gradle ./ # If exists
COPY gradle ./gradle
RUN chmod +x ./gradlew

# Optionally download dependencies
# ./gradlew build --no-daemon || return 0

COPY src ./src

# Build the application
RUN ./gradlew shadowJar --no-daemon

# --- Runtime Stage ---
FROM openjdk:21-jdk-slim
WORKDIR /app

# Copy the consumer's JAR from the builder stage
# Ensure the name matches the output from build.gradle
COPY --from=builder /app/build/libs/kafka-consumer-poc-1.0-SNAPSHOT.jar app.jar

# Command to run the application
CMD ["java", "-jar", "app.jar"]