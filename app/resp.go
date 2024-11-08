package main

import (
	"errors"
	"slices"
	"strconv"
)

type Type byte

const (
	Integer = ':'
	String  = '+'
	Bulk    = '$'
	Array   = '*'
	Error   = '-'
)

var errInvalidArrayAst = errors.New("Invalid array, expected *")
var errInvalidArrayCRLF = errors.New("Invalid array, expected \r\n")

type RESP struct {
	Type  Type
	Raw   []byte
	Data  []byte
	Count int
}

func ParseRESP(b []byte) (int, RESP) {
	var resp = RESP{}
	if len(b) == 0 {
		return 0, RESP{}
	}
	resp.Type = Type(b[0])
	if !slices.Contains([]Type{Integer, String, Bulk, Array, Error}, resp.Type) {
		return 0, RESP{}
	}

	i := 1
	for ; ; i++ {
		if i == len(b) {
			return 0, RESP{}
		}
		if b[i] == '\n' {
			if b[i-1] != '\r' {
				return 0, RESP{}
			}
			i++
			break
		}
	}
	resp.Raw = b[0:i]
	resp.Data = b[1 : i-2]
	if resp.Type == Integer {
		if len(resp.Data) == 0 {
			return 0, RESP{}
		}
		var j int
		if resp.Data[0] == '-' {
			if len(resp.Data) == 1 {
				return 0, RESP{}
			}
			j++
		}
		for ; j < len(resp.Data); j++ {
			if resp.Data[j] < '0' || resp.Data[j] > '9' {
				return 0, RESP{}
			}
		}
		return len(resp.Raw), resp
	}
	if resp.Type == String || resp.Type == Error {
		return len(resp.Raw), resp
	}
	var err error
	resp.Count, err = strconv.Atoi(string(resp.Data))
	if err != nil {
		return 0, RESP{}
	}
	if resp.Type == Bulk {
		if resp.Count < 0 {
			resp.Data = nil
			resp.Count = 0
			return len(resp.Raw), resp
		}
		if len(b) < i+resp.Count+2 {
			return 0, RESP{}
		}
		if b[i+resp.Count] != '\r' || b[i+resp.Count+1] != '\n' {
			return 0, RESP{}
		}
		resp.Data = b[i : i+resp.Count]
		resp.Raw = b[0 : i+resp.Count+2]
		resp.Count = 0
		return len(resp.Raw), resp
	}
	var tn int
	sdata := b[i:]
	for j := 0; j < resp.Count; j++ {
		rn, rresp := ParseRESP(sdata)
		if rresp.Type == 0 {
			return 0, RESP{}
		}
		tn += rn
		sdata = sdata[rn:]
	}
	resp.Data = b[i : i+tn]
	resp.Raw = b[0 : i+tn]
	return len(resp.Raw), resp
}

func ParseCommand(packet []byte) ([][]byte, error) {
	if len(packet) == 0 {
		return nil, nil
	}
	if packet[0] != Array {
		return nil, errInvalidArrayAst
	}
	args := make([][]byte, 0)
	for i := 1; i < len(packet); i++ {
		if packet[i] == '\n' {
			if packet[i-1] != '\r' {
				return nil, errInvalidArrayCRLF
			}
			count, err := strconv.Atoi(string(packet[1 : i-1]))
			if err != nil || count < 0 {
				return nil, errors.New("Invalid bulk count: '" + string(packet[1:i-1]) + "' - " + err.Error())
			}
			if count == 0 {
				return nil, nil
			}
			i++
		nextArg:
			for j := 0; j < count; j++ {
				if i >= len(packet) {
					break
				}
				if packet[i] != '$' {
					return nil, errors.New("expected '$', got '" + string(packet[i]) + "'")
				}
				for s := i + 1; i < len(packet); i++ {
					if packet[i] == '\n' {
						if packet[i-1] != '\r' {
							return nil, errInvalidArrayCRLF
						}
						n, err := strconv.Atoi(string(packet[s : i-1]))
						if err != nil || count < 0 {
							return nil, errors.New("Invalid bulk count: '" + string(packet[1:i-1]) + "' - " + err.Error())
						}
						i++
						if len(packet)-i >= n+2 {
							if packet[i+n] != '\r' || packet[i+n+1] != '\n' {
								return nil, errInvalidArrayCRLF
							}
							args = append(args, packet[i:i+n])
							i += n + 2
							if j == count-1 {
								return args, nil
							}
							continue nextArg
						}

					}
				}
			}
		}
	}
	return nil, nil
}
