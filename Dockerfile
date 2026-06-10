ARG VERSION=26.6.10.8
ARG VARIANT=""

FROM ghcr.io/dionixgmbh/helma:${VERSION}${VARIANT}

ARG VERSION=26.6.10.8
ARG VARIANT=""

COPY lib/*.jar /opt/helma/lib/ext/
