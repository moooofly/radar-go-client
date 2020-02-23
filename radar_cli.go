package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/sirupsen/logrus"
)

/*var (
	sync_actions = []string{"get", "set", "get_children", "update", "set_app_status", "get_app_status", "set_app_control",
		"get_app_control", "set_machine_room_control", "get_machine_room_control", "set_machine_room_status",
		"get_machine_room_status", "get_app_list", "get_group_list", "get_machine_room_list"}

	async_actions = []string{"watch", "watch_children", "watch_config"}
)*/

var (
	// ErrRadarServerLost shows the connection lost err
	ErrRadarServerLost = errors.New("radar server lost")

	// ErrRadarClientStopped shows the client has been stopped
	ErrRadarClientStopped = errors.New("radar client stopped")
)

const (
	hdrSize            = 4        // transaction protocol header size
	bufferSize         = 1024 * 4 // receive buffer
	radarServerTimeout = 30       // radar server connection lost timeout
	heartbeatInterval  = 5        // heartbeat send interval
	dialTimeout        = 5        // dial radar server timeout
)

/////////////////////////////////////////////////////////////
// App status and event
/////////////////////////////////////////////////////////////

// AppStatus represent the status of an app
type AppStatus uint8

const (
	// WuKai defined in C/S trans protocol that, ok == 0, else == 1

	// AppStatusRunning shows the app is running
	AppStatusRunning AppStatus = 0

	// AppStatusFailed shows the app has failed
	AppStatusFailed AppStatus = 1
)

var status2str = map[AppStatus]string{
	AppStatusFailed:  "FAILED",
	AppStatusRunning: "RUNNING",
}

// String return the string of app status
func (s AppStatus) String() string {
	return status2str[s]
}

// StatusReq is the request args which user should pass into client API
type StatusReq struct {
	DomainMoid   string `json:"domain_moid"`
	ResourceMoid string `json:"resource_moid"`
	GroupMoid    string `json:"group_moid"`
	ServerMoid   string `json:"server_moid"`
	ServerName   string `json:"server_name"`
}

// StatusRsp is the reply of client API which request app status
type StatusRsp struct {
	ServerMoid string
	ServerName string
	Status     AppStatus
}

// EventReq is the event which happened
type EventReq uint8

const (
	/*
		NOTE: the original definition is from radarserver.py

		CREATED_EVENT_DEF = 1
		DELETED_EVENT_DEF = 2
		CHANGED_EVENT_DEF = 3
	*/

	// AppStatusEventCreated shows the app has been registered in radar server
	AppStatusEventCreated EventReq = 1

	// AppStatusEventDeleted shows the app has been de-registered in radar server
	AppStatusEventDeleted EventReq = 2

	// AppStatusEventChanged shows the app status has been changed
	AppStatusEventChanged EventReq = 3
)

// EventRsp is returned by radar watch API, which will be passed into callback
type EventRsp struct {
	event EventReq
	data  interface{}
}

// mark one-time interaction
type transaction struct {
	token    string // unique random string used in sync.Map as key
	sendData pdata

	// NOTE: fd, 20190109
	// watch operation may got more than one data
	recvDataC chan pdata
}

/////////////////////////////////////////////////////////////
// Protocol definition
/////////////////////////////////////////////////////////////
type protoMethod uint

// TODO: 协议扩展位置
const (
	heartbeatMethod      protoMethod = 0
	getAppStatusMethod   protoMethod = 1
	watchAppStatusMethod protoMethod = 2

	// TODO: add more
)

type pdata struct {
	pm   protoMethod
	data map[string]interface{}
}

func (d *pdata) encode() ([]byte, error) {
	return json.Marshal(d.data)
}

func (d *pdata) decode(data []byte) (*pdata, error) {
	var buf interface{}
	var err error

	if err := json.Unmarshal(data, &buf); err != nil {
		return nil, err
	}

	v, ok := buf.(map[string]interface{})
	if !ok {
		err = fmt.Errorf("[radar-client] convert [%v] to map[string]interface{} failed!", buf)
		return nil, err
	}

	// FIXME: find no definition of "action"
	act, ok := v["action"]
	if !ok {
		err = fmt.Errorf("[radar-client] no 'action' field in this data(%v)", v)
		return nil, err
	}

	// TODO: 协议扩展位置
	switch act {
	case "heart_beat":
		d.pm = heartbeatMethod
	case "get_app_status":
		d.pm = getAppStatusMethod
	case "watch":
		d.pm = watchAppStatusMethod
	}

	d.data = v
	return d, nil
}

func heartBeatData(token string) pdata {
	return pdata{
		pm: heartbeatMethod,
		data: map[string]interface{}{
			"key":    token,
			"action": "heart_beat",
		},
	}
}

func getAppStatusData(token string, args []StatusReq) pdata {
	return pdata{
		pm: getAppStatusMethod,
		data: map[string]interface{}{
			"key":    token,
			"action": "get_app_status",
			"args":   map[string][]StatusReq{"servers": args},
		},
	}
}

func watchAppStatusData(token string, args StatusReq) pdata {
	return pdata{
		pm: watchAppStatusMethod,
		data: map[string]interface{}{
			"key":    token,
			"action": "watch",
			"args": map[string]string{
				"domain_moid":   args.DomainMoid,
				"resource_moid": args.ResourceMoid,
				"group_moid":    args.GroupMoid,
				"server_moid":   args.ServerName + "_" + args.ServerMoid,
				"node":          "status",
			},
		},
	}
}

// RadarClient represent a radar client
type RadarClient struct {
	host   string
	client net.Conn

	connected      bool // connection state of radar server
	radarLostTimer *time.Timer

	connectedCh    chan struct{} // mark establishment of a connection
	disconnectedCh chan struct{} // mark teardown of a connection

	stopped bool          // is this client stopped?
	stopCh  chan struct{} // stop signal channel

	smap sync.Map // should be a map[string]*transaction

	logger *logrus.Logger

	sync.Mutex
}

func (rc *RadarClient) send(t *transaction) error {
	var bytes []byte

	b, err := t.sendData.encode()
	if err != nil {
		return err
	}

	cnt := uint32(len(b))
	bytes = append(bytes, intToBytes(cnt)...)
	bytes = append(bytes, b...)

	rc.logger.Debugf("[radar-client] send => %s", bytes)
	rc.client.SetWriteDeadline(time.Now().Add(radarServerTimeout * time.Second))
	_, err = rc.client.Write(bytes)
	if err != nil {
		rc.logger.Errorf("[radar-client] send failed: %v", err)
		return err
	}

	return nil
}

func (rc *RadarClient) receive() {
	var raw, buf []byte

	raw = []byte{}
	buf = make([]byte, bufferSize)

	for !rc.stopped {
		buf = buf[0:] // clear buffer

		rc.client.SetReadDeadline(time.Now().Add(radarServerTimeout * time.Second))
		n, err := rc.client.Read(buf)
		if err != nil {
			rc.logger.Errorf("[radar-client] receive failed: %v", err)
			if err == io.EOF {
				select {
				case <-rc.disconnectedCh:
					return
				default:
					close(rc.disconnectedCh)
					rc.updateConnStatus(false, "connection closed by the other side")
					rc.closeConn("connection closed by the other side")
				}
				return
			}
			time.Sleep(time.Second)
			continue
		}

		rc.logger.Debugf("[radar-client] recv <= %s", buf[:n])
		raw = append(raw, buf[:n]...)

		for len(raw) > hdrSize {
			dataLen := int(bytesToInt(raw[:hdrSize])) // get the length of the reply
			lastIndex := dataLen + hdrSize            // end of the reply

			if lastIndex <= len(raw) {
				// got enough data, try to handle it
				go rc.handleData(raw[hdrSize:lastIndex])

				// abandon data which has been used
				raw = raw[lastIndex:]
			} else {
				// not enough, need to read from server again
				break
			}
		}
	}

	rc.logger.Debugf("[radar-client] stop receiving data...")
}

func (rc *RadarClient) handleData(data []byte) {
	var pdata pdata

	_, err := pdata.decode(data)
	if err != nil {
		rc.logger.Errorf("[radar-client] handle data failed: %v", err)
		return
	}

	switch pdata.pm {
	case heartbeatMethod:
		// update connection based on heart_beat reply
		rc.updateConnStatus(true, "heartbeat reply confirm")

	default:
		token, ok := pdata.data["key"].(string)
		if !ok {
			logrus.Errorf("[radar-client] the key value (for sync.Map) must be of string type")
			return
		}

		if v, ok := rc.smap.Load(token); ok {
			v.(*transaction).recvDataC <- pdata
		}
	}
}

func (rc *RadarClient) sendHeartbeat() error {
	var t = &transaction{}

	t.token = randStrGenerator()
	t.sendData = heartBeatData(t.token)

	// NOTE: fd, 20181227
	// skip init the transaction's err channel, cuz it would not be used
	// t.c = make(chan error)
	return rc.send(t)
}

func (rc *RadarClient) healthCheck() {
	// FIXME: really needed?
	if err := rc.sendHeartbeat(); err != nil {
		rc.logger.Errorf("[radar-client] heartbeat send failed: %v", err)
	}

	ticker := time.NewTicker(heartbeatInterval * time.Second)

	for {
		select {
		case <-rc.stopCh:
			rc.logger.Infof("[radar-client] client stopped, stop sending heartbeat")
			return

		case <-ticker.C:
			select {
			case <-rc.disconnectedCh:
				return
			default:
				if err := rc.sendHeartbeat(); err != nil {
					rc.logger.Errorf("[radar-client] heartbeat send failed: %v", err)
				}
			}

		case <-rc.radarLostTimer.C:
			// NOTE:
			// 1. signal the state of disconnection
			// 2. update connection status
			// 3. close connection to make sure sync i/o function `read/send' failed
			select {
			case <-rc.disconnectedCh:
				return
			default:
				close(rc.disconnectedCh)
				rc.updateConnStatus(false, "healthcheck timeout")
				rc.closeConn("healthcheck timeout")
			}
		}
	}
}

func (rc *RadarClient) disconnect(reason string) {
	rc.Lock()
	defer rc.Unlock()

	if rc.stopped {
		return
	}

	rc.stopped = true
	rc.connected = false
	rc.closeConn(reason)

	close(rc.stopCh)

	rc.logger.Infof("[radar-client] connection to radar is [DOWN] (reason: '%s')", reason)
}

func (rc *RadarClient) updateConnStatus(stat bool, reason string) {
	rc.Lock()
	defer rc.Unlock()

	if !rc.connected && stat {
		// unconnected --> connected
		close(rc.connectedCh)
	}
	rc.connected = stat

	if rc.connected {
		// NOTE: the usage below is illustrated inside Stop() function
		if !rc.radarLostTimer.Stop() {
			<-rc.radarLostTimer.C
		}
		rc.radarLostTimer.Reset(radarServerTimeout * time.Second)
		rc.logger.Debugf("[radar-client] connection to radar is [UP] ('%s')", reason)
	} else {
		rc.logger.Warnf("[radar-client] connection to radar is [DOWN] (reason: '%s')", reason)
	}
}

func (rc *RadarClient) closeConn(reason string) error {
	rc.logger.Debugf("[radar-client] shutdown radar connection locally, as of '%s'", reason)

	if rc.client != nil {
		return rc.client.Close()
	}
	return nil
}

func (rc *RadarClient) regTransaction(t *transaction) {
	rc.smap.Store(t.token, t)
}

func (rc *RadarClient) unregTransaction(t *transaction) {
	rc.smap.Delete(t.token)
}

/////////////////////////////////////////////////////////////////////////////
// USER APIs
/////////////////////////////////////////////////////////////////////////////

// NewRadarClient returns a instance of radar client
func NewRadarClient(host string, logger *logrus.Logger) *RadarClient {
	return &RadarClient{
		host:   host,
		logger: logger,
	}
}

// Connect tries to connect with radar server
// This API will not return until the REAL connection has been established
func (rc *RadarClient) Connect() error {
	var err error

	rc.stopCh = make(chan struct{})
	rc.connectedCh = make(chan struct{})
	rc.disconnectedCh = make(chan struct{})

	c, err := net.DialTimeout("tcp", rc.host, dialTimeout*time.Second)
	if err != nil {
		rc.logger.Errorf("[radar-client] error when connecting: %v", err)
		return err
	}

	rc.radarLostTimer = time.NewTimer(radarServerTimeout * time.Second)

	rc.client = c
	rc.smap = sync.Map{}

	go rc.healthCheck()
	go rc.receive()

	// NOTE: fd, 20190111
	// we will not set `connected' to true until we got
	// the first heartbeat reply, cuz when haproxy is involved,
	// the `net.DialTimeout' will try to connect to ha, instead of
	// the server we wanted, which might mislead us to
	// think the connection is established, while the server status
	// is not clear
	//rc.updateConnStatus(true, "connection established")
	select {
	case <-time.NewTimer(dialTimeout * time.Second).C:
		err = ErrRadarServerLost
	case <-rc.connectedCh:
		rc.stopped = false
	}

	return err
}

// Connected returns the current connection state
func (rc *RadarClient) Connected() bool {
	rc.Lock()
	defer rc.Unlock()

	return rc.connected
}

// Disconnect the client, together will connection will radar server
func (rc *RadarClient) Disconnect() {
	rc.disconnect("trigger disconnection from upper call")
}

// Version returns the current radar client version
func (rc *RadarClient) Version() string {
	return fmt.Sprintf("version %s %s", VERSION, DATE)
}

// GetAppStatus returns the app status
func (rc *RadarClient) GetAppStatus(args []StatusReq) ([]StatusRsp, error) {
	if !rc.Connected() {
		return nil, ErrRadarServerLost
	}

	if rc.stopped {
		return nil, ErrRadarClientStopped
	}

	var t = &transaction{}
	var reply []StatusRsp
	var result struct {
		Result struct {
			Data []struct {
				Moid   string
				Status int
				Name   string
			}
		}
	}

	var tokenTemp string
	for {
		tokenTemp = randStrGenerator()
		if _, ok := rc.smap.Load(tokenTemp); ok {
			continue
		}
		break
	}
	t.token = tokenTemp
	t.sendData = getAppStatusData(t.token, args)
	t.recvDataC = make(chan pdata)

	rc.regTransaction(t)
	defer rc.unregTransaction(t)

	err := rc.send(t)
	if err != nil {
		return nil, err
	}

	// wait for the reply
	select {
	case d := <-t.recvDataC:
		// start to parse reply data from server
		// checkout protocol.md
		err = mapstructure.Decode(d.data, &result)
		if err != nil {
			return nil, err
		}

		for _, r := range result.Result.Data {
			var s AppStatus

			switch r.Status {
			case 0:
				s = AppStatusRunning
			case 1:
				s = AppStatusFailed
			default:
				return reply, fmt.Errorf("cannot parse status: %v", r)
			}

			reply = append(reply, StatusRsp{ServerMoid: r.Moid, ServerName: r.Name, Status: s})
		}

		return reply, nil

	case <-rc.stopCh:
		return nil, ErrRadarClientStopped

	case <-rc.disconnectedCh:
		return nil, ErrRadarServerLost
	}
}

// WatchAppStatus watches the specific app status
func (rc *RadarClient) WatchAppStatus(args StatusReq, cb func(rpl EventRsp, err error)) error {
	if !rc.Connected() {
		return ErrRadarServerLost
	}

	if rc.stopped {
		return ErrRadarClientStopped
	}

	var t = &transaction{}
	var tokenTemp string
	for {
		tokenTemp = randStrGenerator()
		if _, ok := rc.smap.Load(tokenTemp); ok {
			continue
		}
		break
	}
	t.token = tokenTemp
	t.sendData = watchAppStatusData(t.token, args)
	t.recvDataC = make(chan pdata)

	rc.regTransaction(t)

	err := rc.send(t)
	if err != nil {
		return err
	}

	// wait for the reply in another routine
	go func() {
		defer rc.unregTransaction(t)

		for !rc.stopped {
			var rpl EventRsp
			var err error
			var result struct {
				Result struct {
					Status int
					Data   string
				}
			}

			select {
			case d := <-t.recvDataC:
				// start to parse reply data from server
				// checkout protocol.md
				err = mapstructure.Decode(d.data, &result)
				if err != nil {
					cb(rpl, err)
					continue
				}

				switch result.Result.Status {
				case 1:
					rpl.event = AppStatusEventCreated
				case 2:
					rpl.event = AppStatusEventDeleted
				case 3:
					rpl.event = AppStatusEventChanged
				case -101:
					err = ErrRadarServerLost
				default:
					err = fmt.Errorf("cannot get right event: %v", result)
				}

				rpl.data = result.Result.Data
				cb(rpl, err)

			case <-rc.stopCh:
				cb(rpl, ErrRadarClientStopped)
				return

			case <-rc.disconnectedCh:
				cb(rpl, ErrRadarServerLost)
				return
			}
		}
	}()

	return nil
}
