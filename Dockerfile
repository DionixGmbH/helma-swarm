ARG VERSION=26.4.24
ARG VARIANT=""

FROM ghcr.io/dionixgmbh/helma:${VERSION}${VARIANT}

ARG VERSION=26.4.24
ARG VARIANT=""

COPY lib/*.jar /opt/helma/lib/ext/
