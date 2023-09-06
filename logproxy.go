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
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/logproxy/config"
	con "github.com/pg-sharding/spqr/pkg/conn"
)

type Proxy struct {
	toHost          string
	toPort          string
	proxyPort       string
	logFileName     string
	interceptedData []byte
	tlsConf         *tls.Config
}

func NewProxy(toHost string, toPort string, file string, proxyPort string, configPath string) Proxy {
	return Proxy{
		toHost:          toHost,
		toPort:          toPort,
		logFileName:     file,
		proxyPort:       proxyPort,
		interceptedData: []byte{},
		tlsConf:         initTls(configPath),
	}
}

func (p *Proxy) Run() error {
	ctx := context.Background()

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

	for {
		select {
		case <-ctx.Done():
			p.Flush()
			os.Exit(1)
		case c := <-cChan:
			go func() {
				if err := p.serv(c); err != nil {
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

func ReplayLogs(host string, port string, user string, db string, file string) error {
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

	f, err := os.OpenFile(file, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	var curt time.Timer
	tim, msg, err := parseFile(f)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	prevT := tim
	curt = *time.NewTimer(tim.Sub(tim))
	for {
		select {
		case <-ctx.Done():
			os.Exit(1)
		case <-curt.C:
			log.Printf("msg %+v ", msg)
		}

		frontend.Send(msg)
		if err := frontend.Flush(); err != nil {
			return fmt.Errorf("failed to send msg to bd %w", err)
		}
		err = recieveBackend(frontend, func(msg pgproto3.BackendMessage) error { return nil })
		if err != nil {
			return err
		}

		tim, msg, err = parseFile(f)
		if err != nil {
			if err == io.EOF {
				frontend.SendClose(&pgproto3.Close{})
				return nil
			}
			return err
		}

		curt = *time.NewTimer(tim.Sub(prevT))
		prevT = tim
	}
}

func (p *Proxy) serv(netconn net.Conn) error {
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

	defer p.Flush()

	for {
		msg, err := cl.Receive()
		if err != nil {
			return fmt.Errorf("failed to receive msg from client %w", err)
		}

		//writing request data to buffer
		byt, err := encodeMessage(msg)
		if err != nil {
			return fmt.Errorf("failed to convert %w", err)
		}
		p.interceptedData = append(p.interceptedData, byt...)
		if len(p.interceptedData) > 1000000 {
			log.Println("flushing buffer")
			err = p.Flush()
			if err != nil {
				return fmt.Errorf("failed to write to file %w", err)
			}
		}

		//send to frontend
		frontend.Send(msg)
		if err := frontend.Flush(); err != nil {
			return fmt.Errorf("failed to send msg to bd %w", err)
		}

		proc := func(msg pgproto3.BackendMessage) error {
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

func (p *Proxy) Flush() error {
	log.Println("flush")

	f, err := os.OpenFile(p.logFileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(p.interceptedData)
	if err != nil {
		return err
	}

	return nil
}

func parseFile(f *os.File) (time.Time, pgproto3.FrontendMessage, error) {
	// 15 byte - timestamp
	// 1 byte - message header
	// 4 bytes - message length (except header)
	// ?? bytes - message bytes

	//timestamp
	timeb := make([]byte, 15)
	_, err := f.Read(timeb) //err
	if err != nil {
		return time.Now(), nil, err
	}
	var ti time.Time
	err = ti.UnmarshalBinary(timeb)
	if err != nil {
		return time.Now(), nil, err
	}

	//header
	tip := make([]byte, 1)

	_, err = f.Read(tip)
	if err != nil {
		return time.Now(), nil, err
	}

	//size
	rawSize := make([]byte, 4)

	_, err = f.Read(rawSize)
	if err != nil {
		return time.Now(), nil, err
	}

	msgSize := int(binary.BigEndian.Uint32(rawSize) - 4)

	//message
	msg := make([]byte, msgSize)
	_, err = f.Read(msg)
	if err != nil {
		return time.Now(), nil, err
	}

	var fm pgproto3.FrontendMessage
	switch string(tip) {
	case "Q":
		fm = &pgproto3.Query{}
	case "X":
		return time.Now(), nil, io.EOF
	}
	err = fm.Decode(msg)
	if err != nil {
		return time.Now(), nil, err
	}

	return ti, fm, nil
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
		case con.GSSREQ:
			log.Println("negotiate gss enc request")
			_, err := netconn.Write([]byte{'N'})
			if err != nil {
				return nil, err
			}
			// proceed next iter, for protocol version number or GSSAPI interaction
			continue

		case con.SSLREQ: //TODO tls config
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

			frsm, err := backend.ReceiveStartupMessage()
			if err != nil {
				return nil, err
			}

			switch msg := frsm.(type) {
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
				return nil, fmt.Errorf("received unexpected message type %T", frsm)
			}

		case pgproto3.ProtocolVersionNumber:
			cl := pgproto3.NewBackend(bufio.NewReader(netconn), netconn)
			sm := &pgproto3.StartupMessage{}
			err = sm.Decode(msg)
			if err != nil {
				return nil, err
			}

			frontend.Send(sm)
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
1 byte - message header
4 bytes - message length (except header)
?? bytes - message bytes
*/
func encodeMessage(msg pgproto3.FrontendMessage) ([]byte, error) {
	b2 := msg.Encode(nil)

	t := time.Now()
	tb, err := t.MarshalBinary()
	if err != nil {
		return nil, err
	}

	compl := append(tb, b2...)
	return compl, nil
}
