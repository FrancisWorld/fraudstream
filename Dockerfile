# syntax=docker/dockerfile:1
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
ARG MAVEN_PROFILES=""
ENV MAVEN_OPTS="-Xmx512m"
COPY .mvn .mvn
COPY mvnw pom.xml ./
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -q dependency:go-offline -Dspotless.skip=true -P"${MAVEN_PROFILES}"
COPY src src
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -q package -DskipTests -Dspotless.skip=true -Djacoco.skip=true -P"${MAVEN_PROFILES}"

FROM eclipse-temurin:21-jre
RUN groupadd --system --gid 10001 fraud && useradd --system --uid 10001 --gid fraud --home-dir /app --shell /usr/sbin/nologin fraud \
 && mkdir -p /app/state && chown -R fraud:fraud /app
WORKDIR /app
COPY --from=build --chown=fraud:fraud /src/target/fraudstream.jar /app/fraudstream.jar
COPY --from=build --chown=fraud:fraud /src/target/lib /app/lib
USER 10001:10001
ENV FRAUDSTREAM_STATE_DIR=/app/state \
    JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError"
EXPOSE 8080 8081
VOLUME /app/state
HEALTHCHECK --interval=15s --timeout=3s --start-period=40s --retries=3 \
  CMD ["bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8081 && printf 'GET /healthz HTTP/1.0\\r\\n\\r\\n' >&3 && head -n1 <&3 | grep -q 200"]
ENTRYPOINT ["java", "-cp", "/app/fraudstream.jar:/app/lib/*", "dev.francisworld.fraudstream.Main"]
