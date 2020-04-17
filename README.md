# radar-go-client

This is the go version of radar client.

## TODO

- [ ] currently, all of the parameters below are hardcode
    - bufferSize: the receive buffer size of the connection to radar server
    - radarServerTimeout: when is radar server to be considered connection lost
    - heartBeatInterval: the interval of heartbeat check
    - dialTimeout: the timeout value of dialing to radar server
- [ ] "action": this is the key word in transport protocol, but find no specific definition
- [ ] protocal extension
    - only support heartbeat/getAppMethod/watchAppStatus methods right now
- [ ] update Version and Date manually right now, need a proper way to track changes
