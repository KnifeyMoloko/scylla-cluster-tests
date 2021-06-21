FROM openjdk:8 as builder
EXPOSE 8080
EXPOSE 7001
RUN apt -y update && apt -y install git && \
    git clone --branch v0.5.1 https://github.com/Netflix/ndbench.git && \
    cd ndbench          && \
    ./gradlew build

FROM openjdk:8 as app
COPY --from=builder /root/.gradle /root/.gradle
COPY --from=builder /ndbench /ndbench
