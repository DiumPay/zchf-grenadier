package config

import "strings"

// Ethereum mainnet — only chain that has positions
var (
	MintingHubV2 = strings.ToLower("0xDe12B620A8a714476A97EfD14E6F7180Ca653557")
	CloneHelper  = strings.ToLower("0x55cD2820735Db56ca0965BE224D71994265F8bee")
	ZCHF         = strings.ToLower("0xB58E61C3098d85632Df34EecfB899A1Ed80921cB")
	Equity       = strings.ToLower("0x1bA26788dfDe592fec8bcB0Eaff472a42BE341B2")
)

// EthereumRPCPool — public RPC endpoints for mainnet.
var EthereumRPCPool = []string{
	"https://eth.drpc.org",
	"https://eth.blockrazor.xyz",
	"https://1rpc.io/eth",
	"https://eth.meowrpc.com",
	"https://ethereum-public.nodies.app",
	"https://mainnet.gateway.tenderly.co",
	"https://eth.api.onfinality.io/public",
	"https://0xrpc.io/eth",
	"https://api.zan.top/eth-mainnet",
	"https://ethereum-mainnet.gateway.tatum.io",
	"https://eth.llamarpc.com",
	"https://ethereum-rpc.publicnode.com",
	"https://public-eth.nownodes.io",
	"https://rpc.sentio.xyz/mainnet",
	"https://eth.api.pocket.network",
	"https://rpc.eth.gateway.fm",
	"https://gateway.tenderly.co/public/mainnet",
	"https://rpc.flashbots.net/fast",
	"https://rpc.mevblocker.io/fast",
	"https://eth-mainnet.public.blastapi.io",
	"https://ethereum.public.blockpi.network/v1/rpc/public",
	"https://eth1.lava.build",
	"https://ethereum-json-rpc.stakely.io",
	"https://rpc.mevblocker.io/fullprivacy",
}

// Server settings
const (
	HTTPPort = 8080
	DataDir  = "./data"
	DBPath   = "./data/grenadier.db"
)
