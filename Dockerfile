FROM eclipse-temurin:21 AS build
WORKDIR /build

COPY mvnw .
ADD .mvn ./.mvn

COPY pom.xml .
COPY src src

RUN --mount=type=cache,target=/root/.m2 ./mvnw -f pom.xml clean package -DskipTests


FROM eclipse-temurin:21

WORKDIR /

COPY --from=build /build/target/*.jar cca-backend.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "cca-backend.jar"]