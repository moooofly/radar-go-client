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
	hdrSize            = 4        // tranx protocol header size
	bufferSize         = 1024 * 4 // receive buffer
	radarServerTimeout = 30       // radar server connection lost timeout
	heartBeatInterval  = 5        // heart beat check interval
	dialTimeout        = 5        // dial radar server timeout
)

/////////////////////////////////////////////////////////////
// App status and event
/////////////////////////////////////////////////////////////

// AppStatus represent the app's status
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

// AppStatusArgs is the request args which user should pass into client API
type AppStatusArgs struct {
	DomainMoid   string `json:"domain_moid"`
	ResourceMoid string `json:"resource_moid"`
	GroupMoid    string `json:"group_moid"`
	ServerMoid   string `json:"server_moid"`
	ServerName   string `json:"server_name"`
}

// AppStatusReply is the reply of client API which request app status
type AppStatusReply struct {
	ServerMoid string
	ServerName string
	Status     AppStatus
}

// AppStatusEvent is the event which happened
type AppStatusEvent uint8

const (
	/*
		wukai defined in radarserver.py:

		CREATED_EVENT_DEF = 1
		DELETED_EVENT_DEF = 2
		CHANGED_EVENT_DEF = 3
	*/

	// AppStatusEventCreated shows the app has been registered in radar server
	AppStatusEventCreated = 1

	// AppStatusEventDeleted shows the app has been de-registered in radar server
	AppStatusEventDeleted = 2

	// AppStatusEventChanged shows the app status has been changed
	AppStatusEventChanged = 3
)

// AppStatusEventReply is one of the args filled by radar watch API, which will
// be passed into callback
type AppStatusEventReply struct {
	event AppStatusEvent
	data  interface{}
}

/////////////////////////////////////////////////////////////
// Transaction
/////////////////////////////////////////////////////////////
type tranx struct {
	key      string
	sendData pdata

	// NOTE: fd, 20190109
	// `watch' operation may got more than one data
	recvDataC chan pdata
}

/////////////////////////////////////////////////////////////
// Protocol data struct
/////////////////////////////////////////////////////////////
type t uint

const (
	tHeartBeat = iota
	tGetAppStatus
	tWatchAppStatus
)

type pdata struct {
	t    t
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
		err = fmt.Errorf("cannot convert [%v] to type map[string]interface{}", buf)
		return nil, err
	}

	act, ok := v["action"]
	if !ok {
		err = fmt.Errorf("no [action] field in data: %v", v)
		return nil, err
	}

	switch act {
	case "heart_beat":
		d.t = tHeartBeat
	case "get_app_status":
		d.t = tGetAppStatus
	case "watch":
		d.t = tWatchAppStatus
	}

	d.data = v
	return d, nil
}

func heartBeatData(key string) pdata {
	return pdata{
		t: tHeartBeat,
		data: map[string]interface{}{
			"key":    key,
			"action": "heart_beat"},
	}
}

func getAppStatusData(key string, args []AppStatusArgs) pdata {
	return pdata{
		t: tGetAppStatus,
		data: map[string]interface{}{
			"key":    key,
			"action": "get_app_status",
			"args":   map[string][]AppStatusArgs{"servers": args},
		}}
}

func watchAppStatusData(key string, args AppStatusArgs) pdata {
	return pdata{
		t: tWatchAppStatus,
		data: map[string]interface{}{
			"key":    key,
			"action": "watch",
			"args": map[string]string{
				"domain_moid":   args.DomainMoid,
				"resource_moid": args.ResourceMoid,
				"group_moid":    args.GroupMoid,
				"server_moid":   args.ServerName + "_" + args.ServerMoid,
				"node":          "status"},
		}}
}

// RadarClient represent a radar client
type RadarClient struct {
	host   string
	client net.Conn

	connected      bool          // is radar server connected
	radarLostTimer *time.Timer   // timer of radar server connection lost
	connectedCh    chan struct{} // signal chan of connection established
	connLostCh     chan struct{} // signal chan of connection lost

	stopped bool          // is this client stopped?
	stopCh  chan struct{} // stop signal channel

	tranxs sync.Map // should be a map[string]*tranx

	logger *logrus.Logger

	sync.Mutex
}

func (rc *RadarClient) send(t *tranx) error {
	var bytes []byte

	b, err := t.sendData.encode()
	if err != nil {
		return err
	}

	cnt := uint32(len(b))
	bytes = append(bytes, intToBytes(cnt)...)
	bytes = append(bytes, b...)

	rc.logger.Debugf("[radar-client] will send data: %s", bytes)
	rc.client.SetWriteDeadline(time.Now().Add(radarServerTimeout * time.Second))
	_, err = rc.client.Write(bytes)
	if err != nil {
		rc.logger.Errorf("[radar-client] data send failed: %v", err)
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
			rc.logger.Errorf("[radar-client] receiving failed: %v", err)
			if err == io.EOF {
				select {
				case <-rc.connLostCh:
					return
				default:
					close(rc.connLostCh)
					rc.updateConnStatus(false, "close by the other side")
					rc.closeConn("close by the other side")
				}
				return
			}
			time.Sleep(time.Second)
			continue
		}

		rc.logger.Debugf("[radar-client] data received: %s", buf[:n])
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

	switch pdata.t {
	case tHeartBeat:
		// update connection based on heart_beat reply
		rc.updateConnStatus(true, "heart beat got")

	default:
		key, ok := pdata.data["key"].(string)
		if !ok {
			logrus.Errorf("[radar-client] the key must be a type of string")
			return
		}

		if v, ok := rc.tranxs.Load(key); ok {
			v.(*tranx).recvDataC <- pdata
		}
	}
}

func (rc *RadarClient) sendHeartBeat() error {
	var t = &tranx{}

	t.key = randStrGenerator()
	t.sendData = heartBeatData(t.key)

	// NOTE: fd, 20181227
	// skip init the tranx's err channel, cuz it would not be used
	// t.c = make(chan error)
	return rc.send(t)
}

func (rc *RadarClient) healthCheck() {
	if err := rc.sendHeartBeat(); err != nil {
		rc.logger.Errorf("[radar-client] send beat failed: %v", err)
	}

	ticker := time.NewTicker(heartBeatInterval * time.Second)

	for {
		select {
		case <-rc.stopCh:
			rc.logger.Infof("[radar-client] client stopped, stop sending heartbeat")
			return

		case <-ticker.C:
			select {
			case <-rc.connLostCh:
				return
			default:
				if err := rc.sendHeartBeat(); err != nil {
					rc.logger.Errorf("[radar-client] send beat failed: %v", err)
				}
			}

		case <-rc.radarLostTimer.C:
			// connection lost found
			// 1. send out connection lost signal
			// 2. update connection status
			// 3. close connection to make sure sync io function `read/send' failed
			select {
			case <-rc.connLostCh:
				return
			default:
				close(rc.connLostCh)
				rc.updateConnStatus(false, "timeout")
				rc.closeConn("timeout")
			}
		}
	}
}

func (rc *RadarClient) close(reason string) {
	rc.Lock()
	defer rc.Unlock()

	if rc.stopped {
		return
	}

	rc.stopped = true
	rc.connected = false
	rc.closeConn(reason)

	close(rc.stopCh)

	rc.logger.Infof("[radar-client] client closed: %v", reason)
}

func (rc *RadarClient) updateConnStatus(yes bool, reason string) {
	rc.Lock()
	defer rc.Unlock()

	if !rc.connected && yes {
		close(rc.connectedCh)
	}

	rc.connected = yes

	if rc.connected {
		if !rc.radarLostTimer.Stop() {
			<-rc.radarLostTimer.C
		}
		rc.radarLostTimer.Reset(radarServerTimeout * time.Second)
		rc.logger.Debugf("[radar-client] radar server timer has been reset! on account of: %s", reason)
	} else {
		rc.logger.Warnf("[radar-client] radar server disconnected! on account of: %s", reason)
	}
}

func (rc *RadarClient) closeConn(reason string) error {
	rc.logger.Debugf("[radar-client] radar client connection will be closed, on account of %s", reason)
	if rc.client != nil {
		return rc.client.Close()
	}
	return nil
}

func (rc *RadarClient) registerTranx(t *tranx) {
	rc.tranxs.Store(t.key, t)
}

func (rc *RadarClient) deregisterTranx(t *tranx) {
	rc.tranxs.Delete(t.key)
}

/////////////////////////////////////////////////////////////////////////////
// USER APIs
/////////////////////////////////////////////////////////////////////////////

// NewRadarClient returns a inst of radar client data structure
func NewRadarClient(host string, logger *logrus.Logger) *RadarClient {
	return &RadarClient{host: host, logger: logger}
}

// Connect tries to connect with radar server
// This API will not return until the REAL connection has been established
func (rc *RadarClient) Connect() error {
	var err error

	rc.stopCh = make(chan struct{})
	rc.connectedCh = make(chan struct{})
	rc.connLostCh = make(chan struct{})

	c, err := net.DialTimeout("tcp", rc.host, dialTimeout*time.Second)
	if err != nil {
		rc.logger.Errorf("[radar-client] error when connecting: %v", err)
		return err
	}

	rc.radarLostTimer = time.NewTimer(radarServerTimeout * time.Second)

	rc.client = c
	rc.tranxs = sync.Map{}

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

// Close the client, together will connection will radar server
func (rc *RadarClient) Close() {
	rc.close("user request")
}

// Version returns the current radar client version
func (rc *RadarClient) Version() string {
	return fmt.Sprintf("version %s %s", VERSION, DATE)
}

// GetAppStatus returns the app status
func (rc *RadarClient) GetAppStatus(args []AppStatusArgs) ([]AppStatusReply, error) {
	if !rc.Connected() {
		return nil, ErrRadarServerLost
	}

	if rc.stopped {
		return nil, ErrRadarClientStopped
	}

	var t = &tranx{}
	var reply []AppStatusReply
	var result struct {
		Result struct {
			Data []struct {
				Moid   string
				Status int
				Name   string
			}
		}
	}

	var key string
	for {
		key = randStrGenerator()
		if _, ok := rc.tranxs.Load(key); ok {
			continue
		}
		break
	}
	t.key = key
	t.sendData = getAppStatusData(t.key, args)
	t.recvDataC = make(chan pdata)

	rc.registerTranx(t)
	defer rc.deregisterTranx(t)

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

			reply = append(reply, AppStatusReply{ServerMoid: r.Moid, ServerName: r.Name, Status: s})
		}

		return reply, nil

	case <-rc.stopCh:
		return nil, ErrRadarClientStopped

	case <-rc.connLostCh:
		return nil, ErrRadarServerLost
	}
}

// WatchAppStatus watches the specific app status
func (rc *RadarClient) WatchAppStatus(args AppStatusArgs, f func(rpl AppStatusEventReply, err error)) error {
	if !rc.Connected() {
		return ErrRadarServerLost
	}

	if rc.stopped {
		return ErrRadarClientStopped
	}

	var t = &tranx{}
	var key string
	for {
		key = randStrGenerator()
		if _, ok := rc.tranxs.Load(key); ok {
			continue
		}
		break
	}
	t.key = key
	t.sendData = watchAppStatusData(t.key, args)
	t.recvDataC = make(chan pdata)

	rc.registerTranx(t)

	err := rc.send(t)
	if err != nil {
		return err
	}

	// wait for the reply in another routine
	go func() {
		defer rc.deregisterTranx(t)

		for !rc.stopped {
			var rpl AppStatusEventReply
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
					f(rpl, err)
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
				f(rpl, err)

			case <-rc.stopCh:
				f(rpl, ErrRadarClientStopped)
				return

			case <-rc.connLostCh:
				f(rpl, ErrRadarServerLost)
				return
			}
		}
	}()

	return nil
}
