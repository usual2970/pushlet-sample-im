#!/bin/sh
# sample-im container entrypoint.
#
# Railway volumes mount with root ownership, while the app is meant to run as
# the unprivileged "app" user. Start as root, normalize ownership of the data
# directory, then drop privileges before exec'ing the server (exec so signals
# reach the process for the graceful-shutdown ordering). When the container
# already starts unprivileged, skip the chown and run directly.
set -e

export SAMPLE_IM_ADDR=":${PORT:-8080}"

if [ "$(id -u)" = "0" ]; then
  chown -R app:app /data 2>/dev/null || true
  exec su-exec app:app /usr/local/bin/sample-im
fi

exec /usr/local/bin/sample-im
