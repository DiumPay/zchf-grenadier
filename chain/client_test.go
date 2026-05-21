package chain

import (
	"encoding/hex"
	"strings"
	"testing"

	"golang.org/x/crypto/sha3"
)

// TestTopicHashes verifies every event topic hash declared in client.go
// matches keccak256(eventSignature) of the corresponding event in the
// official Frankencoin V2 contracts. The signatures below are copied
// verbatim from @frankencoin/zchf:
//   - contracts/minting/v2/MintingHubV2.sol
//   - contracts/minting/v2/PositionV2.sol
//
// This is a guard against the class of bug where a wrong topic hash
// silently breaks indexing — eth_getLogs returns nothing on mismatch,
// no error, no warning, the indexer just stops seeing those events.
// If the contract ever changes an event signature, this test fails and
// prints the right hash so you can update the constant.
func TestTopicHashes(t *testing.T) {
	cases := []struct {
		name      string
		signature string
		want      string
	}{
		// MintingHubV2.sol
		{"TopicPositionOpened", "PositionOpened(address,address,address,address)", TopicPositionOpened},
		{"TopicChallengeStarted", "ChallengeStarted(address,address,uint256,uint256)", TopicChallengeStarted},
		{"TopicChallengeAverted", "ChallengeAverted(address,uint256,uint256)", TopicChallengeAverted},
		{"TopicChallengeSucceed", "ChallengeSucceeded(address,uint256,uint256,uint256,uint256)", TopicChallengeSucceed},
		{"TopicPostponedReturn", "PostPonedReturn(address,address,uint256)", TopicPostponedReturn},
		{"TopicForcedSale", "ForcedSale(address,uint256,uint256)", TopicForcedSale},
		// PositionV2.sol
		{"TopicMintingUpdate", "MintingUpdate(uint256,uint256,uint256)", TopicMintingUpdate},
		{"TopicPositionDenied", "PositionDenied(address,string)", TopicPositionDenied},
		// OpenZeppelin Ownable
		{"TopicOwnership", "OwnershipTransferred(address,address)", TopicOwnership},
	}

	for _, c := range cases {
		h := sha3.NewLegacyKeccak256()
		h.Write([]byte(c.signature))
		got := "0x" + hex.EncodeToString(h.Sum(nil))
		if !strings.EqualFold(got, c.want) {
			t.Errorf("\n  %s\n  sig:  %s\n  want: %s\n  got:  %s",
				c.name, c.signature, c.want, got)
		}
	}
}
