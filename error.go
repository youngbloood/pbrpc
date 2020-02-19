package pbrpc

import (
	"errors"
)

var errNotProtobuf = errors.New("args not implement proto.Message{}")

var errMissingParams = errors.New("pbrpc: request body missing params")
