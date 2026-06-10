ARG VERSION=26.6.10.5
ARG VARIANT=""

FROM ghcr.io/dionixgmbh/helma:${VERSION}${VARIANT}

ARG VERSION=26.6.10.5
ARG VARIANT=""

COPY lib/*.jar /opt/helma/lib/ext/
