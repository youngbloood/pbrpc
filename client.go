package pbrpc

import (
	"bufio"
	"io"
	"net"
	"net/rpc"
	"sync"

	msg "github.com/youngbloood/pbrpc/internal"
	"google.golang.org/protobuf/proto"
)

type clientCodec struct {
	conn io.ReadWriteCloser

	enc io.Writer
	dec io.Reader
	// temporary work space
	req  *msg.Request
	resp *msg.Response

	mutex   sync.Mutex        // protects pending
	pending map[uint64]string // map request id to method name
}

func NewClientCodec(conn io.ReadWriteCloser) rpc.ClientCodec {
	return &clientCodec{
		conn:    conn,
		enc:     bufio.NewWriter(conn),
		dec:     bufio.NewReader(conn),
		req:     new(msg.Request),
		resp:    new(msg.Response),
		pending: make(map[uint64]string),
	}
}

func (c *clientCodec) WriteRequest(r *rpc.Request, param interface{}) (err error) {
	pbmsg, ok := param.(proto.Message)
	if !ok {
		return errNotProtobuf
	}

	var bts []byte
	bts, err = proto.Marshal(pbmsg)
	if err != nil {
		return
	}
	c.mutex.Lock()
	c.pending[r.Seq] = r.ServiceMethod
	c.mutex.Unlock()
	c.req.Method = r.ServiceMethod
	c.req.Params = bts
	c.req.Id = r.Seq

	bts, err = proto.Marshal(c.req)
	if err != nil {
		return
	}
	_, err = c.enc.Write(bts)
	return
}

func (c *clientCodec) ReadResponseHeader(r *rpc.Response) (err error) {
	c.resp.Reset()

	var bts []byte
	bts, err = io.ReadAll(c.dec)
	if err != nil {
		return
	}
	if err = proto.Unmarshal(bts, c.resp); err != nil {
		return
	}

	c.mutex.Lock()
	r.ServiceMethod = c.pending[c.resp.Id]
	delete(c.pending, c.resp.Id)
	c.mutex.Unlock()
	r.Seq = c.resp.Id
	r.Error = c.resp.Error

	return nil
}

func (c *clientCodec) ReadResponseBody(v interface{}) error {
	if v == nil {
		return nil
	}
	pbmsg, ok := v.(proto.Message)
	if !ok {
		return errNotProtobuf
	}
	return proto.Unmarshal(c.resp.Result, pbmsg)
}

func (c *clientCodec) Close() error {
	return c.conn.Close()
}

// NewClient returns a new rpc.Client to handle requests to the
// set of services at the other end of the connection.
func NewClient(conn io.ReadWriteCloser) *rpc.Client {
	return rpc.NewClientWithCodec(NewClientCodec(conn))
}

// Dial connects to a PROTO-RPC server at the specified network address.
func Dial(network, address string) (*rpc.Client, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return NewClient(conn), err
}
