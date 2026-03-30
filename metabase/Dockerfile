# Debian-basiertes Metabase-Image (statt Alpine)
# Grund: DuckDB's jemalloc crasht auf Alpine/musl (malloc_init_hard)
# Mit glibc (Ubuntu/Debian) funktioniert DuckDB nativ ohne Workarounds

FROM eclipse-temurin:21-jre

ENV MB_PLUGINS_DIR=/plugins
ENV FC_LANG=en-US
ENV LC_CTYPE=en_US.UTF-8

RUN apt-get update && apt-get install -y --no-install-recommends \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /plugins /metabase-data

# Metabase JAR aus dem offiziellen Alpine-Image kopieren
COPY --from=metabase/metabase:latest /app/metabase.jar /app/metabase.jar

EXPOSE 3000

ENTRYPOINT ["java", "-jar", "/app/metabase.jar"]
