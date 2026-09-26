# sample-im container image.
#
# Build context MUST be the pushlet-workspace root (not this directory):
# sample-im's go.mod replaces github.com/usual2970/pushlet with the sibling
# ../pushlet checkout, which the Docker build preserves as /src/pushlet.
#
#   docker build -f sample-im/Dockerfile -t sample-im .
#   docker run --rm -p 8080:8080 sample-im
#
# Railway: deploy the pushlet-workspace repo (submodules enabled); this
# Dockerfile binds to $PORT and stores both SQLite databases under
# SAMPLE_IM_DATA_DIR (default /data — mount a Railway Volume there).

FROM golang:1.26-alpine AS build
# Module proxy is a build arg so restricted networks can override it:
#   docker build --build-arg GOPROXY=https://goproxy.cn,direct ...
# Railway's builders reach the default proxy directly.
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}
WORKDIR /src

# Manifests first so dependency downloads cache independently of source edits.
COPY pushlet/go.mod pushlet/go.sum ./pushlet/
COPY sample-im/go.mod sample-im/go.sum ./sample-im/
WORKDIR /src/sample-im
RUN go mod download

# Full sources; the replace directive resolves ../pushlet inside the image.
WORKDIR /src
COPY pushlet/ ./pushlet/
COPY sample-im/ ./sample-im/
WORKDIR /src/sample-im
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/sample-im .

FROM alpine:3.20
RUN adduser -D -H -u 10001 app && mkdir -p /data && chown -R app:app /data
USER app
ENV SAMPLE_IM_DATA_DIR=/data
COPY --from=build /out/sample-im /usr/local/bin/sample-im
EXPOSE 8080

# Railway injects PORT; expand it in the shell so the binary binds correctly,
# and exec so SIGTERM reaches the process for the graceful-shutdown ordering.
CMD SAMPLE_IM_ADDR=":${PORT:-8080}" exec /usr/local/bin/sample-im
