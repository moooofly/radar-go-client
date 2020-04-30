package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/sirupsen/logrus"
)

var (
	sync_actions = []string{
		"get",
		"set",
		"get_children",
		"update",
		"set_app_status",
		"get_app_status",
		"set_app_control",
		"get_app_control",
		"set_machine_room_control",
		"get_machine_room_control",
		"set_machine_room_status",
		"get_machine_room_status",
		"get_app_list",
		"get_group_list",
		"get_machine_room_list",
	}

	async_actions = []string{
		"watch",
		"watch_children",
		"watch_config",
	}
)

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

// ###############################
// App status and event
// ###############################

// AppStatus represent the status of an app
type AppStatus uint8

const (
	// defined in radar server C/S transport protocol, ok == 0, else == 1

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

// AppControl represent the state of an app whether is enabled
type AppControl uint8

const (
	// defined in radar server C/S transport protocol, ok == 0, else == 1

	// AppDisabled shows the app is disabled
	AppDisabled AppControl = 0

	// AppEnabled shows the app is enabled
	AppEnabled AppControl = 1
)

var control2str = map[AppControl]string{
	AppDisabled: "disabled",
	AppEnabled:  "enabled",
}

// String return the string of app status
func (s AppControl) String() string {
	return control2str[s]
}

// ReqArgs is the common request args which is set into client API
type ReqArgs struct {
	DomainMoid   string `json:"domain_moid"`
	ResourceMoid string `json:"resource_moid"`
	GroupMoid    string `json:"group_moid"`
	ServerMoid   string `json:"server_moid"`
	ServerName   string `json:"server_name"`
}

type RspComm struct {
	ServerMoid string
	ServerName string
}

// StatusRsp is used by GetAppStatus()
type StatusRsp struct {
	RspComm
	Status AppStatus
}

//
// ControlRsp is used by GetAppControl()
type ControlRsp struct {
	RspComm
	Control AppControl
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

// define one-time interaction
type transaction struct {
	token    string // unique random string used in sync.Map as key
	sendData pdata

	// NOTE: fd, 20190109
	// watch operation may got more than one data
	recvDataC chan pdata
}

// ###############################
// Protocol definition
// ###############################

type protoMethod uint

// TODO: 协议扩展位置
const (
	heartbeatMethod      protoMethod = 0
	getAppStatusMethod   protoMethod = 1
	watchAppStatusMethod protoMethod = 2
	getAppControlMethod  protoMethod = 3
	setAppControlMethod  protoMethod = 4

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
	case "get_app_control":
		d.pm = getAppControlMethod
	case "set_app_control":
		d.pm = setAppControlMethod
	}

	d.data = v
	return d, nil
}

// RadarClient represent a radar client
type RadarClient struct {
	ip   string
	port string

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

	//rc.logger.Debugf("[radar-client] send => %s", bytes)
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
			rc.logger.Warnf("[radar-client] receive failed: %v", err)
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

		//rc.logger.Debugf("[radar-client] recv <= %s", buf[:n])
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

	rc.logger.Warnf("[radar-client] connection to radar is [DOWN] (reason: '%s')", reason)
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
	rc.logger.Warnf("[radar-client] shutdown radar connection locally, as of '%s'", reason)

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

// ###############
//   General API
// ###############

// NewRadarClient returns a instance of radar client
func NewRadarClient(ip, port string, logger *logrus.Logger) *RadarClient {
	return &RadarClient{
		ip:     ip,
		port:   port,
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

	var addr string
	if strings.Contains(rc.ip, ":") {
		addr = fmt.Sprintf("[%s]:%s", rc.ip, rc.port)
	} else {
		addr = fmt.Sprintf("%s:%s", rc.ip, rc.port)
	}

	c, err := net.DialTimeout("tcp", addr, dialTimeout*time.Second)
	if err != nil {
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

// #############################
//   protocol interaction
// #############################

func heartBeatData(token string) pdata {
	return pdata{
		pm: heartbeatMethod,
		data: map[string]interface{}{
			"key":    token,
			"action": "heart_beat",
		},
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

// #############################
//   User sync/async RPC API
// #############################

func (rc *RadarClient) prepare() (error, string) {
	if !rc.Connected() {
		return ErrRadarServerLost, ""
	}

	if rc.stopped {
		return ErrRadarClientStopped, ""
	}

	var tokenTemp string
	for {
		tokenTemp = randStrGenerator()
		if _, ok := rc.smap.Load(tokenTemp); ok {
			continue
		}
		break
	}
	return nil, tokenTemp
}

func getAppControlData(token string, args []ReqArgs) pdata {
	return pdata{
		pm: getAppControlMethod,
		data: map[string]interface{}{
			"key":    token,
			"action": "get_app_control",
			"args":   map[string][]ReqArgs{"servers": args},
		},
	}
}

// GetAppControl returns the app control state
func (rc *RadarClient) GetAppControl(args []ReqArgs) ([]ControlRsp, error) {

	/*
		rc.logger.Debugf("[radar-client] [[ GetAppControl ]] -- args --")
		rc.logger.Debugf("[radar-client] [[ GetAppControl ]] %v", args)
		rc.logger.Debugf("[radar-client] [[ GetAppControl ]] -- args --")
	*/

	err, tk := rc.prepare()
	if err != nil {
		return nil, err
	}

	var t = &transaction{}
	t.token = tk
	t.sendData = getAppControlData(t.token, args)
	t.recvDataC = make(chan pdata)

	rc.regTransaction(t)
	defer rc.unregTransaction(t)

	if err = rc.send(t); err != nil {
		return nil, err
	}

	var GetAppControlRsp struct {
		Result struct {
			Data []struct {
				Moid    string
				Name    string
				Control string
			}
		}
	}

	var reply []ControlRsp
	select {
	case d := <-t.recvDataC:

		/*
			rc.logger.Debugf("[radar-client] [[ GetAppControl ]] -- d.data --")
			rc.logger.Debugf("[radar-client] [[ GetAppControl ]] %v", d.data)
			rc.logger.Debugf("[radar-client] [[ GetAppControl ]] -- d.data --")
		*/

		// start to parse reply data from server checkout protocol.md
		err = mapstructure.Decode(d.data, &GetAppControlRsp)
		if err != nil {
			return nil, err
		}

		/*
			rc.logger.Debugf("[radar-client] [[ GetAppControl ]] -- GetAppControlRsp --")
			rc.logger.Debugf("[radar-client] [[ GetAppControl ]] %v", GetAppControlRsp)
			rc.logger.Debugf("[radar-client] [[ GetAppControl ]] -- GetAppControlRsp --")
		*/

		for _, r := range GetAppControlRsp.Result.Data {
			var c AppControl

			switch r.Control {
			case "0":
				c = AppDisabled
			case "1":
				c = AppEnabled
			default:
				return reply, fmt.Errorf("cannot parse status: %v", r)
			}

			reply = append(reply, ControlRsp{
				RspComm: RspComm{
					ServerMoid: r.Moid,
					ServerName: r.Name,
				},
				Control: c,
			})
		}

		return reply, nil

	case <-rc.stopCh:
		return nil, ErrRadarClientStopped

	case <-rc.disconnectedCh:
		return nil, ErrRadarServerLost
	}
}

// FIXME: 是否需要将 map 的 interface 直接改成 string
func setAppControlData(token string, op, dm, rm, gm, sm, sn string) pdata {
	return pdata{
		pm: setAppControlMethod,
		data: map[string]interface{}{
			"key":    token,
			"action": "set_app_control",
			"args": map[string]interface{}{
				"operation":     op,
				"domain_moid":   dm,
				"resource_moid": rm,
				"group_moid":    gm,
				"server_moid":   sm,
				"server_name":   sn,
			},
		},
	}
}

// NOTE: resourceMoid is equal to machineRoomMoid
// SetAppControl
func (rc *RadarClient) SetAppControl(operation, domainMoid, resourceMoid, groupMoid, serverMoid, serverName string) (bool, error) {

	/*
		rc.logger.Debugf("[radar-client] [[ SetAppControl ]] -- args --")
		rc.logger.Debugf("[radar-client] [[ SetAppControl ]] operation   : %v", operation)
		rc.logger.Debugf("[radar-client] [[ SetAppControl ]] domainMoid  : %v", domainMoid)
		rc.logger.Debugf("[radar-client] [[ SetAppControl ]] resourceMoid: %v", resourceMoid)
		rc.logger.Debugf("[radar-client] [[ SetAppControl ]] groupMoid   : %v", groupMoid)
		rc.logger.Debugf("[radar-client] [[ SetAppControl ]] serverMoid  : %v", serverMoid)
		rc.logger.Debugf("[radar-client] [[ SetAppControl ]] serverName  : %v", serverName)
		rc.logger.Debugf("[radar-client] [[ SetAppControl ]] -- args --")
	*/

	err, tk := rc.prepare()
	if err != nil {
		return false, err
	}

	var t = &transaction{}
	t.token = tk
	t.sendData = setAppControlData(t.token, operation, domainMoid, resourceMoid, groupMoid, serverMoid, serverName)
	t.recvDataC = make(chan pdata)

	rc.regTransaction(t)
	defer rc.unregTransaction(t)

	if err = rc.send(t); err != nil {
		return false, err
	}

	var setAppControlRsp struct {
		Result struct {
			Data string
		}
	}

	select {
	case d := <-t.recvDataC:

		/*
			rc.logger.Debugf("[radar-client] [[ SetAppControl ]] -- d.data --")
			rc.logger.Debugf("[radar-client] [[ SetAppControl ]] %v", d.data)
			rc.logger.Debugf("[radar-client] [[ SetAppControl ]] -- d.data --")
		*/

		err = mapstructure.Decode(d.data, &setAppControlRsp)
		if err != nil {
			return false, err
		}

		/*
			rc.logger.Debugf("[radar-client] [[ SetAppControl ]] -- setAppControlRsp --")
			rc.logger.Debugf("[radar-client] [[ SetAppControl ]] %v", setAppControlRsp)
			rc.logger.Debugf("[radar-client] [[ SetAppControl ]] -- setAppControlRsp --")
		*/

		// NOTE: Data can only be "Success" or "Failed"
		if setAppControlRsp.Result.Data == "Success" {
			return true, nil
		} else {
			return false, nil
		}

	case <-rc.stopCh:
		return false, ErrRadarClientStopped

	case <-rc.disconnectedCh:
		return false, ErrRadarServerLost
	}
}

func getAppStatusData(token string, args []ReqArgs) pdata {
	return pdata{
		pm: getAppStatusMethod,
		data: map[string]interface{}{
			"key":    token,
			"action": "get_app_status",
			"args":   map[string][]ReqArgs{"servers": args},
		},
	}
}

// GetAppStatus returns the app status
func (rc *RadarClient) GetAppStatus(args []ReqArgs) ([]StatusRsp, error) {

	/*
		rc.logger.Debugf("[radar-client] [[ GetAppStatus ]] -- args --")
		rc.logger.Debugf("[radar-client] [[ GetAppStatus ]] %v", args)
		rc.logger.Debugf("[radar-client] [[ GetAppStatus ]] -- args --")
	*/

	err, tk := rc.prepare()
	if err != nil {
		return nil, err
	}

	var t = &transaction{}
	t.token = tk
	t.sendData = getAppStatusData(t.token, args)
	t.recvDataC = make(chan pdata)

	rc.regTransaction(t)
	defer rc.unregTransaction(t)

	if err := rc.send(t); err != nil {
		return nil, err
	}

	var GetAppStatusRsp struct {
		Result struct {
			Data []struct {
				Moid   string
				Status int
				Name   string
			}
		}
	}

	var reply []StatusRsp
	select {
	case d := <-t.recvDataC:

		/*
			rc.logger.Debugf("[radar-client] [[ GetAppStatus ]] -- d.data --")
			rc.logger.Debugf("[radar-client] [[ GetAppStatus ]] %v", d.data)
			rc.logger.Debugf("[radar-client] [[ GetAppStatus ]] -- d.data --")
		*/

		err = mapstructure.Decode(d.data, &GetAppStatusRsp)
		if err != nil {
			return nil, err
		}

		/*
			rc.logger.Debugf("[radar-client] [[ GetAppStatus ]] -- GetAppStatusRsp --")
			rc.logger.Debugf("[radar-client] [[ GetAppStatus ]] %v", GetAppStatusRsp)
			rc.logger.Debugf("[radar-client] [[ GetAppStatus ]] -- GetAppStatusRsp --")
		*/

		for _, r := range GetAppStatusRsp.Result.Data {
			var s AppStatus

			switch r.Status {
			case 0:
				s = AppStatusRunning
			case 1:
				s = AppStatusFailed
			default:
				return reply, fmt.Errorf("cannot parse status: %v", r)
			}

			reply = append(reply, StatusRsp{
				RspComm: RspComm{
					ServerMoid: r.Moid,
					ServerName: r.Name,
				},
				Status: s,
			})
		}

		return reply, nil

	case <-rc.stopCh:
		return nil, ErrRadarClientStopped

	case <-rc.disconnectedCh:
		return nil, ErrRadarServerLost
	}
}

func watchAppStatusData(token string, args ReqArgs) pdata {
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

// WatchAppStatus watches the specific app status
func (rc *RadarClient) WatchAppStatus(args ReqArgs, cb func(rpl EventRsp, err error)) error {

	err, tk := rc.prepare()
	if err != nil {
		return err
	}

	var t = &transaction{}
	t.token = tk
	t.sendData = watchAppStatusData(t.token, args)
	t.recvDataC = make(chan pdata)

	rc.regTransaction(t)

	if err := rc.send(t); err != nil {
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
