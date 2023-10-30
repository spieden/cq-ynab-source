FROM clojure:temurin-20-tools-deps-bullseye-slim AS deps
ARG GITHUB_ACTOR
ARG GITHUB_TOKEN

COPY deps.edn deps.edn
COPY settings.xml /root/.m2/

RUN clojure -Stree

FROM clojure:temurin-20-tools-deps-bullseye-slim

COPY deps.edn deps.edn
COPY src src
COPY --from=deps /root/.m2/repository /root/.m2/repository

EXPOSE 7777

ENTRYPOINT ["clojure", "-J--add-opens=java.base/java.nio=ALL-UNNAMED", "-M:run-cq"]
