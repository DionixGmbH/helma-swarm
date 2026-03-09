ARG VERSION=26.3.9
ARG VARIANT=""

FROM ghcr.io/dionixgmbh/helma:${VERSION}${VARIANT}

ARG VERSION=26.3.9
ARG VARIANT=""

COPY lib/*.jar /opt/helma/lib/ext/
