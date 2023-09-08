package logproxy

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/logproxy/config"
)

const SSLREQ = 80877103
const GSSREQ = 80877104

type Proxy struct {
	toHost      string
	toPort      string
	proxyPort   string
	logFileName string
	tlsConf     *tls.Config
}

// used to make defer with changing byte arrays
type InterceptedData struct {
	data []byte
}

type TimedMessage struct {
	timestamp time.Time
	msg       pgproto3.FrontendMessage
	session   int
}

func NewProxy(toHost string, toPort string, file string, proxyPort string, configPath string) Proxy {
	return Proxy{
		toHost:      toHost,
		toPort:      toPort,
		logFileName: file,
		proxyPort:   proxyPort,
		tlsConf:     initTls(configPath),
	}
}

func (p *Proxy) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp6", fmt.Sprintf("[::1]:%s", p.proxyPort))
	if err != nil {
		return fmt.Errorf("failed to start proxy %w", err)
	}
	defer listener.Close()

	cChan := make(chan net.Conn)

	accept := func(l net.Listener, cChan chan net.Conn) {
		for {
			c, err := l.Accept()
			if err != nil {
				// handle error (and then for example indicate acceptor is down)
				cChan <- nil
				return
			}
			cChan <- c
		}
	}

	go accept(listener, cChan)

	log.Printf("Proxy is up and listening at %s", p.proxyPort)

	sessionNum := 0
	wg := sync.WaitGroup{}
	for {
		select {
		case <-ctx.Done():
			log.Println("global context done")
			wg.Wait()
			//os.Exit(1)
			return nil
		case c := <-cChan:
			wg.Add(1)
			go func() {
				log.Printf("recieved new session")
				defer wg.Done()
				sessionNum++
				if err := p.serv(c, sessionNum, ctx); err != nil {
					log.Fatal(err)
				}
			}()
		}
	}
}

func initTls(path string) *tls.Config {
	conf, err := config.LoadTlsCfg(path)
	if err != nil {
		return nil
	}
	return conf
}

/*
Parses file fith stored queries to fist Terminate message and sends it to db
*/
func ReplayLogs(host string, port string, user string, db string, file string) error {
	f, err := os.OpenFile(file, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	sessionsMessageBuffer := map[int](chan TimedMessage){}

	for {
		//read next
		msg, err := parseFile(f)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		log.Printf("session %d\n", msg.session)
		// if session not exist, create
		if _, ok := sessionsMessageBuffer[msg.session]; !ok {
			sessionsMessageBuffer[msg.session] = make(chan TimedMessage)
			go startNewSession(host, port, user, db, sessionsMessageBuffer[msg.session])
		}

		//send to session
		sessionsMessageBuffer[msg.session] <- msg
	}
}

func startNewSession(host string, port string, user string, db string, ch chan TimedMessage) error {
	ctx := context.Background()

	startupMessage := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":     user,
			"database": db,
		},
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		return fmt.Errorf("failed to establish connection to host %s - %w", fmt.Sprintf("%s:%s", host, port), err)
	}

	frontend := pgproto3.NewFrontend(bufio.NewReader(conn), conn)

	frontend.Send(startupMessage)
	if err := frontend.Flush(); err != nil {
		return fmt.Errorf("failed to send msg to bd %w", err)
	}
	err = recieveBackend(frontend, func(msg pgproto3.BackendMessage) error { return nil })
	if err != nil {
		return err
	}

	var tm TimedMessage
	var prevSentTime, prevMsgTime time.Time
	for {
		select {
		case <-ctx.Done():
			os.Exit(1)
		case tm = <-ch:
			timeNow := time.Now()
			timer := time.NewTimer(tm.timestamp.Sub(prevMsgTime) - timeNow.Sub(prevSentTime))
			prevSentTime = timeNow
			prevMsgTime = tm.timestamp
			<-timer.C

			log.Printf("msg %+v ", tm.msg)
			frontend.Send(tm.msg)
			if err := frontend.Flush(); err != nil {
				return fmt.Errorf("failed to send msg to bd %w", err)
			}
			switch tm.msg.(type) {
			case *pgproto3.Terminate:
				return nil
			default:
				err = recieveBackend(frontend, func(msg pgproto3.BackendMessage) error { return nil })
				if err != nil {
					return err
				}
			}
		}
	}
}

func (p *Proxy) serv(netconn net.Conn, session int, ctx context.Context) error {
	interData := &InterceptedData{
		data: []byte{},
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", p.toHost, p.toPort))
	if err != nil {
		return err
	}

	frontend := pgproto3.NewFrontend(bufio.NewReader(conn), conn)

	//handle startup messages
	cl, err := p.startup(netconn, frontend)
	if err != nil {
		return err
	}

	defer p.Flush(interData)

	for {
		select {
		case <-ctx.Done():
			log.Println("context done")
			return nil
		default:
		}
		msg, err := cl.Receive()
		if err != nil {
			return fmt.Errorf("failed to receive msg from client %w", err)
		}

		log.Printf("client message: %+v", msg)

		//writing request data to buffer
		byt, err := encodeMessage(msg, session)
		if err != nil {
			return fmt.Errorf("failed to convert %w", err)
		}
		interData.data = append(interData.data, byt...)
		if len(interData.data) > 1000000 {
			err = p.Flush(interData)
			if err != nil {
				return fmt.Errorf("failed to write to file %w", err)
			}
			interData.data = []byte{}
		}

		//send to frontend
		frontend.Send(msg)
		if err := frontend.Flush(); err != nil {
			return fmt.Errorf("failed to send msg to bd %w", err)
		}

		switch msg.(type) {
		case *pgproto3.Terminate:
			return nil
		}

		proc := func(msg pgproto3.BackendMessage) error {
			log.Printf("ans: %+v", msg)
			cl.Send(msg)
			if err := cl.Flush(); err != nil {
				return fmt.Errorf("failed to send msg to client %w", err)
			}
			return nil
		}
		err = recieveBackend(frontend, proc)
		if err != nil {
			return err
		}
	}
}

func (p *Proxy) Flush(interceptedData *InterceptedData) error {
	log.Println("flush")

	f, err := os.OpenFile(p.logFileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(interceptedData.data)
	if err != nil {
		return err
	}

	return nil
}

func parseFile(f *os.File) (TimedMessage, error) {
	// 15 byte - timestamp
	// 4 bytes - session number
	// 1 byte - message header
	// 4 bytes - message length (except header)
	// ?? bytes - message bytes

	tm := TimedMessage{
		timestamp: time.Now(),
		msg:       nil,
	}

	//timestamp
	timeb := make([]byte, 15)
	_, err := f.Read(timeb)
	if err != nil {
		return tm, err
	}
	var ti time.Time
	err = ti.UnmarshalBinary(timeb)
	if err != nil {
		return tm, err
	}

	//session
	rawSes := make([]byte, 4)

	_, err = f.Read(rawSes)
	if err != nil {
		return tm, err
	}

	sesNum := int(binary.BigEndian.Uint32(rawSes) - 4)

	//header
	tip := make([]byte, 1)

	_, err = f.Read(tip)
	if err != nil {
		return tm, err
	}

	//size
	rawSize := make([]byte, 4)

	_, err = f.Read(rawSize)
	if err != nil {
		return tm, err
	}

	msgSize := int(binary.BigEndian.Uint32(rawSize) - 4)

	//message
	msg := make([]byte, msgSize)
	_, err = f.Read(msg)
	if err != nil {
		return tm, err
	}

	var fm pgproto3.FrontendMessage
	switch string(tip) {
	case "Q":
		fm = &pgproto3.Query{}
	case "X":
		fm = &pgproto3.Terminate{}
	}
	err = fm.Decode(msg)
	if err != nil {
		return tm, err
	}

	tm.timestamp = ti
	tm.msg = fm
	tm.session = sesNum

	return tm, nil
}

func (p *Proxy) startup(netconn net.Conn, frontend *pgproto3.Frontend) (*pgproto3.Backend, error) {
	for {
		headerRaw := make([]byte, 4)
		_, err := netconn.Read(headerRaw)
		if err != nil {
			return nil, err
		}

		msgSize := int(binary.BigEndian.Uint32(headerRaw) - 4)
		msg := make([]byte, msgSize)

		_, err = netconn.Read(msg)
		if err != nil {
			return nil, err
		}

		protoVer := binary.BigEndian.Uint32(msg)

		switch protoVer {
		case GSSREQ:
			log.Println("negotiate gss enc request")
			_, err := netconn.Write([]byte{'N'})
			if err != nil {
				return nil, err
			}
			// proceed next iter, for protocol version number or GSSAPI interaction
			continue

		case SSLREQ:
			if p.tlsConf == nil {
				log.Default().Println("no tls provided")
				_, err := netconn.Write([]byte{'N'})
				if err != nil {
					return nil, err
				}
				// proceed next iter, for protocol version number or GSSAPI interaction
				continue
			}

			_, err := netconn.Write([]byte{'S'})
			if err != nil {
				return nil, err
			}

			netconn = tls.Server(netconn, p.tlsConf)

			backend := pgproto3.NewBackend(bufio.NewReader(netconn), netconn)

			startupMsg, err := backend.ReceiveStartupMessage()
			if err != nil {
				return nil, err
			}

			switch msg := startupMsg.(type) {
			case *pgproto3.StartupMessage:
				frontend.Send(msg)
				if err := frontend.Flush(); err != nil {
					return nil, fmt.Errorf("failed to send msg to bd %w", err)
				}
				proc := func(msg pgproto3.BackendMessage) error {
					backend.Send(msg)
					if err := backend.Flush(); err != nil {
						return fmt.Errorf("failed to send msg to client %w", err)
					}
					return nil
				}
				err = recieveBackend(frontend, proc)
				return backend, err
			default:
				return nil, fmt.Errorf("received unexpected message type %T", startupMsg)
			}

		case pgproto3.ProtocolVersionNumber:
			cl := pgproto3.NewBackend(bufio.NewReader(netconn), netconn)
			startMsg := &pgproto3.StartupMessage{}
			err = startMsg.Decode(msg)
			if err != nil {
				return nil, err
			}

			frontend.Send(startMsg)
			if err := frontend.Flush(); err != nil {
				return nil, fmt.Errorf("failed to send msg to bd %w", err)
			}
			proc := func(msg pgproto3.BackendMessage) error {
				cl.Send(msg)
				if err := cl.Flush(); err != nil {
					return fmt.Errorf("failed to send msg to client %w", err)
				}
				return nil
			}
			err = recieveBackend(frontend, proc)
			return cl, err

		default:
			return nil, fmt.Errorf("protocol number %d not supported", protoVer)
		}
	}
}

func recieveBackend(frontend *pgproto3.Frontend, process func(pgproto3.BackendMessage) error) error {
	for {
		retmsg, err := frontend.Receive()
		if err != nil {
			return fmt.Errorf("failed to receive msg from db %w", err)
		}

		err = process(retmsg)
		if err != nil {
			return err
		}

		if shouldStop(retmsg) {
			return nil
		}
	}
}

func shouldStop(msg pgproto3.BackendMessage) bool {
	switch msg.(type) {
	case *pgproto3.ReadyForQuery:
		return true
	default:
		return false
	}
}

/*
Gets pgproto3.FrontendMessage and encodes it in binary with timestamp.
15 byte - timestamp
4 bytes - session number
1 byte - message header
4 bytes - message length (except header)
?? bytes - message bytes
*/
func encodeMessage(msg pgproto3.FrontendMessage, session int) ([]byte, error) {
	binMsg := msg.Encode(nil)

	t := time.Now()
	binTime, err := t.MarshalBinary()
	if err != nil {
		return nil, err
	}

	binSessionNum := make([]byte, 4)
	binary.BigEndian.PutUint32(binSessionNum, uint32(session))

	compl := append(binTime, binSessionNum...)
	compl = append(compl, binMsg...)
	return compl, nil
}
