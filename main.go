package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DiumPay/zchf-grenadier/api"
	"github.com/DiumPay/zchf-grenadier/chain"
	"github.com/DiumPay/zchf-grenadier/config"
	"github.com/DiumPay/zchf-grenadier/indexer"
	"github.com/DiumPay/zchf-grenadier/store"
	"github.com/DiumPay/zchf-grenadier/transport"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// ensure data dir exists
	if err := os.MkdirAll("./data", 0755); err != nil {
		return fmt.Errorf("data dir: %w", err)
	}

	// 1. store (sqlite)
	st, err := store.Open(config.DBPath)
	if err != nil {
		return fmt.Errorf("store open: %w", err)
	}
	defer st.Close()

	// 2. transport (balanced RPC pool)
	rpc := transport.New(transport.Config{
		Pool:            config.EthereumRPCPool,
		BlockAwareCache: true,
		OnFail: func(url, method string, err error) {
			// uncomment for verbose rpc failure logs
			// fmt.Printf("[rpc fail] %s %s: %v\n", method, url, err)
		},
		OnBreaker: func(url string, open bool) {
			state := "closed"
			if open {
				state = "OPEN"
			}
			fmt.Printf("[rpc breaker] %s → %s\n", url, state)
		},
	})

	// 3. chain client
	ch := chain.New(rpc)

	// graceful shutdown context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n[main] shutting down...")
		cancel()
	}()

	// 4. bootstrap (one-time seed if store empty)
	bootCtx, bootCancel := context.WithTimeout(ctx, 60*time.Second)
	if err := indexer.Bootstrap(bootCtx, st, ch); err != nil {
		bootCancel()
		return fmt.Errorf("bootstrap: %w", err)
	}
	bootCancel()

	count, _ := st.Count()
	lastBlock, _ := st.GetLastBlock()
	fmt.Printf("[main] ready: %d positions, last block %d\n", count, lastBlock)

	// 5. indexer (the tick loop)
	ix := indexer.New(ch, st)
	go ix.Run(ctx)

	// 6. http server
	srv := api.New(st, ix.LastBlock)
	go func() {
		fmt.Printf("[main] http server on :%d\n", config.HTTPPort)
		if err := srv.Listen(config.HTTPPort); err != nil {
			fmt.Printf("[main] http server: %v\n", err)
			cancel()
		}
	}()

	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = shutdownCtx
	_ = srv.Shutdown()
	fmt.Println("[main] bye")
	return nil
}
