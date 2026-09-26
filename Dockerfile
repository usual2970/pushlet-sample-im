# sample-im container image — standalone build (deploy repo = sample-im alone).
#
# sample-im's go.mod replaces github.com/usual2970/pushlet with the sibling
# ../pushlet checkout. A standalone clone of this repo has no sibling, so the
# first stage fetches the pinned pushlet source and the build stage lays it
# out at /src/pushlet next to /src/sample-im — the replace resolves inside
# the image exactly like it does in the workspace.
#
#   docker build -t sample-im .          (from inside sample-im/)
#   docker run --rm -p 8080:8080 sample-im
#
# Railway: deploy this repo; the container binds $PORT and stores both
# SQLite databases under SAMPLE_IM_DATA_DIR (default /data — mount a
# Railway Volume there).
#
# PUSHLET_REF is a build arg (Railway passes service variables as build
# args). It MUST point at a pushlet revision that includes the WebSocket
# command-parser arity guard — anything at or after the fix landing on main
# (v0.0.20 tags do NOT have it).

FROM golang:1.26-alpine AS pushlet
ARG PUSHLET_REF=main
RUN apk add --no-cache git
WORKDIR /src
RUN git clone --depth 1 --branch ${PUSHLET_REF} https://github.com/usual2970/pushlet pushlet

FROM golang:1.26-alpine AS build
# Module proxy is a build arg so restricted networks can override it:
#   docker build --build-arg GOPROXY=https://goproxy.cn,direct ...
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}
WORKDIR /src

# Manifests first so dependency downloads cache independently of source edits.
COPY go.mod go.sum ./sample-im/
COPY --from=pushlet /src/pushlet/go.mod /src/pushlet/go.sum ./pushlet/
WORKDIR /src/sample-im
RUN go mod download

# Full sources; ../pushlet resolves to the fetched checkout.
WORKDIR /src
COPY --from=pushlet /src/pushlet ./pushlet/
COPY . ./sample-im/
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
