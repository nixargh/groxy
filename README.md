# groxy
Graphite Proxy in Golang

## Build
`go get -d`

`go build`

## Run
Get help

`groxy -h`

Run with some options

`./groxy -port 2004 -graphiteAddress localhost -tenant techops -prefix "groxy" -immutablePrefix "test." -ignoreCert true`

## Test
Send metric like this

`echo "test1.bash.stats 42 `` ` ``date +%s`` ` ``" | nc localhost 2004`
