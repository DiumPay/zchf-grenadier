package chain

import (
	"encoding/binary"
	"errors"
)

type mcCall struct {
	target       string // 0x address
	allowFailure bool
	callData     []byte // typically 4 bytes (just the selector)
}

type mcResult struct {
	Success    bool
	ReturnData []byte
}

// encodeAggregate3 packs `aggregate3((address,bool,bytes)[])` for N calls.
//
// Layout:
//
//	selAggregate3 (4 bytes)
//	offset to array data = 0x20 (32 bytes)
//	array length (32 bytes)
//	N tuple offsets (each 32 bytes), relative to the start of the array data
//	N tuples, each:
//	  - address (32 bytes)
//	  - allowFailure bool (32 bytes)
//	  - offset to bytes (32 bytes)
//	  - bytes length (32 bytes)
//	  - bytes data (padded to 32)
func encodeAggregate3(calls []mcCall) ([]byte, error) {
	out := make([]byte, 0, 256+len(calls)*200)
	out = append(out, selAggregate3[:]...)

	// offset to the array (always 0x20 since there's just one outer arg)
	out = append(out, encodeUint64(0x20)...)
	// array length
	out = append(out, encodeUint64(uint64(len(calls)))...)

	// First pass: serialize each tuple body separately
	tupleBodies := make([][]byte, len(calls))
	for i, c := range calls {
		addr, err := encodeAddress(c.target)
		if err != nil {
			return nil, err
		}
		body := make([]byte, 0, 128+len(c.callData))
		body = append(body, addr...)
		body = append(body, encodeBool(c.allowFailure)...)
		// offset to bytes within this tuple: 3 words = 0x60
		body = append(body, encodeUint64(0x60)...)
		// bytes length
		body = append(body, encodeUint64(uint64(len(c.callData)))...)
		// bytes data (padded)
		body = append(body, padTo32(c.callData)...)
		tupleBodies[i] = body
	}

	// Tuple offsets section
	// Each offset is relative to the start of the array data section
	// (which begins right after the offsets section).
	offsetsSize := len(calls) * 32
	cursor := offsetsSize
	for _, b := range tupleBodies {
		out = append(out, encodeUint64(uint64(cursor))...)
		cursor += len(b)
	}
	for _, b := range tupleBodies {
		out = append(out, b...)
	}
	return out, nil
}

func encodeUint64(v uint64) []byte {
	out := make([]byte, 32)
	binary.BigEndian.PutUint64(out[24:], v)
	return out
}

func encodeBool(b bool) []byte {
	out := make([]byte, 32)
	if b {
		out[31] = 1
	}
	return out
}

// decodeAggregate3Result parses the return of aggregate3.
//
// Layout:
//
//	offset to results array (32 bytes, always 0x20)
//	results length (32 bytes)
//	N offsets (each 32, relative to start of results section)
//	N tuples, each:
//	  - success bool (32 bytes)
//	  - offset to returnData bytes (32 bytes)
//	  - returnData length (32 bytes)
//	  - returnData (padded to 32)
func decodeAggregate3Result(data []byte) ([]mcResult, error) {
	if len(data) < 64 {
		return nil, errors.New("aggregate3: response too short")
	}
	n := binary.BigEndian.Uint64(data[24:32]) // results length
	if n == 0 {
		return nil, nil
	}

	resultsStart := 64 // skip the outer offset (32) + length (32)
	offsetsTableStart := resultsStart
	out := make([]mcResult, n)

	for i := uint64(0); i < n; i++ {
		tupleOffsetPos := offsetsTableStart + int(i)*32
		if len(data) < tupleOffsetPos+32 {
			return nil, errors.New("aggregate3: short tuple offset")
		}
		tupleRelOff := binary.BigEndian.Uint64(data[tupleOffsetPos+24 : tupleOffsetPos+32])
		tupleStart := resultsStart + int(tupleRelOff)
		if len(data) < tupleStart+96 {
			return nil, errors.New("aggregate3: short tuple body")
		}
		success := data[tupleStart+31] != 0
		// bytes offset is relative to tuple start
		bytesOff := binary.BigEndian.Uint64(data[tupleStart+32+24 : tupleStart+64])
		bytesPos := tupleStart + int(bytesOff)
		if len(data) < bytesPos+32 {
			return nil, errors.New("aggregate3: short bytes len")
		}
		bytesLen := binary.BigEndian.Uint64(data[bytesPos+24 : bytesPos+32])
		if uint64(len(data)) < uint64(bytesPos)+32+bytesLen {
			return nil, errors.New("aggregate3: short bytes body")
		}
		ret := make([]byte, bytesLen)
		copy(ret, data[bytesPos+32:uint64(bytesPos)+32+bytesLen])
		out[i] = mcResult{Success: success, ReturnData: ret}
	}
	return out, nil
}
