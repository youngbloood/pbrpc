package pbrpc

import (
	"bufio"
	"errors"
	"io"
	"net/rpc"
	"sync"

	msg "github.com/youngbloood/pbrpc/internal"
	"google.golang.org/protobuf/proto"
)

type serverCodec struct {
	conn io.ReadWriteCloser
	dec  io.Reader
	enc  io.Writer

	req  *msg.Request
	resp *msg.Response

	mutex   sync.Mutex // protects seq, pending
	seq     uint64
	pending map[uint64]uint64
}

// NewServerCodec returns a new rpc.ServerCodec using PROTO-RPC on conn.
func NewServerCodec(conn io.ReadWriteCloser) rpc.ServerCodec {
	return &serverCodec{
		conn:    conn,
		dec:     bufio.NewReader(conn),
		enc:     bufio.NewWriter(conn),
		req:     new(msg.Request),
		resp:    new(msg.Response),
		pending: make(map[uint64]uint64),
	}
}

func (s *serverCodec) ReadRequestHeader(r *rpc.Request) error {
	s.req.Reset()
	bts, err := io.ReadAll(s.dec)
	if err != nil {
		return err
	}
	if err = proto.Unmarshal(bts, s.req); err != nil {
		return err
	}
	r.ServiceMethod = s.req.Method

	s.mutex.Lock()
	s.seq++
	s.pending[s.seq] = s.req.Id
	r.Seq = s.seq
	s.mutex.Unlock()

	return nil
}

func (s *serverCodec) ReadRequestBody(v interface{}) error {
	if v == nil {
		return nil
	}
	pbmsg, ok := v.(proto.Message)
	if !ok {
		return errNotProtobuf
	}
	if s.req.Params == nil {
		return errMissingParams
	}
	return proto.Unmarshal(s.req.Params, pbmsg)
}

func (s *serverCodec) WriteResponse(r *rpc.Response, v interface{}) (err error) {
	pbmsg, ok := v.(proto.Message)
	if !ok {
		return errNotProtobuf
	}
	s.resp.Reset()
	s.mutex.Lock()
	id, ok := s.pending[r.Seq]
	if !ok {
		s.mutex.Unlock()
		return errors.New("invalid sequence number in response")
	}
	delete(s.pending, r.Seq)
	s.mutex.Unlock()
	s.resp.Id = id

	if r.Error == "" {
		s.resp.Result, err = proto.Marshal(pbmsg)
		if err != nil {
			return
		}
	} else {
		s.resp.Error = r.Error
	}

	var bts []byte
	bts, err = proto.Marshal(s.resp)
	if err != nil {
		return
	}
	_, err = s.enc.Write(bts)
	return
}

func (s *serverCodec) Close() error {
	return s.conn.Close()
}

// ServeConn runs the PROTO-RPC server on a single connection.
// ServeConn blocks, serving the connection until the client hangs up.
// The caller typically invokes ServeConn in a go statement.
func ServeConn(conn io.ReadWriteCloser) {
	rpc.ServeCodec(NewServerCodec(conn))
}
