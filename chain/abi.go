package chain

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"golang.org/x/crypto/sha3"
)

// Compute first 4 bytes of keccak256("functionName()") at startup.
func selector(sig string) [4]byte {
	h := sha3.NewLegacyKeccak256()
	h.Write([]byte(sig))
	var out [4]byte
	copy(out[:], h.Sum(nil)[:4])
	return out
}

// Precomputed once at startup. Verify any time with `cast sig "price()"`.
var (
	selOwner               = selector("owner()")
	selOriginal            = selector("original()")
	selCollateral          = selector("collateral()")
	selZchf                = selector("zchf()")
	selPrice               = selector("price()")
	selMinted              = selector("minted()")
	selMinimumCollateral   = selector("minimumCollateral()")
	selChallengedAmount    = selector("challengedAmount()")
	selLimit               = selector("limit()")
	selAvailableForClones  = selector("availableForClones()")
	selAvailableForMinting = selector("availableForMinting()")
	selStart               = selector("start()")
	selExpiration          = selector("expiration()")
	selCooldown            = selector("cooldown()")
	selChallengePeriod     = selector("challengePeriod()")
	selRiskPremiumPPM      = selector("riskPremiumPPM()")
	selReserveContribution = selector("reserveContribution()")
	selIsClosed            = selector("isClosed()")
	selName                = selector("name()")
	selSymbol              = selector("symbol()")
	selDecimals            = selector("decimals()")
	selBalanceOf           = selector("balanceOf(address)")

	// aggregate3((address,bool,bytes)[])
	selAggregate3 = selector("aggregate3((address,bool,bytes)[])")
)

// ---- decoders: read a 32-byte word at offset and turn it into a Go type ----

func decodeUint256(data []byte, offset int) (*big.Int, error) {
	if len(data) < offset+32 {
		return nil, errors.New("uint256: short data")
	}
	return new(big.Int).SetBytes(data[offset : offset+32]), nil
}

func decodeUint64(data []byte, offset int) (uint64, error) {
	if len(data) < offset+32 {
		return 0, errors.New("uint64: short data")
	}
	// only the last 8 bytes of the 32-byte word matter for uint64-fits values
	return binary.BigEndian.Uint64(data[offset+24 : offset+32]), nil
}

func decodeAddress(data []byte, offset int) (string, error) {
	if len(data) < offset+32 {
		return "", errors.New("address: short data")
	}
	// address lives in the last 20 bytes of the 32-byte word
	return "0x" + hex.EncodeToString(data[offset+12:offset+32]), nil
}

func decodeBool(data []byte, offset int) (bool, error) {
	if len(data) < offset+32 {
		return false, errors.New("bool: short data")
	}
	return data[offset+31] != 0, nil
}

// decodeString: dynamic type. First 32 bytes are an offset pointing to where
// the string data starts. At that location: 32 bytes of length, then the bytes.
func decodeString(data []byte, offset int) (string, error) {
	off, err := decodeUint64(data, offset)
	if err != nil {
		return "", err
	}
	if uint64(len(data)) < off+32 {
		return "", errors.New("string: short data for length")
	}
	length := binary.BigEndian.Uint64(data[off+24 : off+32])
	if uint64(len(data)) < off+32+length {
		return "", errors.New("string: short data for body")
	}
	return string(data[off+32 : off+32+length]), nil
}

// ---- encoder for multicall (the only thing that takes inputs) ----

// encodeAddress pads a 20-byte address into a 32-byte word.
func encodeAddress(addr string) ([]byte, error) {
	addr = strings.TrimPrefix(strings.ToLower(addr), "0x")
	if len(addr) != 40 {
		return nil, fmt.Errorf("bad address: %s", addr)
	}
	b, err := hex.DecodeString(addr)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 32)
	copy(out[12:], b)
	return out, nil
}

// encodeUint256 pads a value into 32 bytes big-endian.
func encodeUint256(v *big.Int) []byte {
	out := make([]byte, 32)
	b := v.Bytes()
	copy(out[32-len(b):], b)
	return out
}

// padTo32 right-pads bytes with zeros to a multiple of 32.
func padTo32(b []byte) []byte {
	rem := len(b) % 32
	if rem == 0 {
		return b
	}
	return append(b, make([]byte, 32-rem)...)
}
