# groxy
Graphite Proxy in Golang

## Features
- Send metrics to multiple destinations (only host and port may differ but not TLS options).
- TLS for input & output connections.
- Add configurable tenant.
- Add configurable prefix.
- Don't add prefix to metrics start with configurable strings.
- Send and receive metrics with **zlib** compression.
- Publish its own metrics by HTTP.
- Send its own metics to configurable tenant and prefix.

## Build
`go get -d`

`go build`

## Run
Get help

`groxy -h`

Run with some options

`./groxy -port 2004 -graphiteAddress localhost:2003 -tenant techops -prefix "groxy" -immutablePrefix "test." -TLS -ignoreCert true`

## Test
Send metric like this

``echo "test1.bash.stats 42 `date +%s`" | nc localhost 2004``
