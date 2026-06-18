// Command netmesh is the single NetMesh binary. It runs as either the
// Controller (Master) or an Agent (Node) depending on the -master flag:
//
//	netmesh -master=self [-admin=user:pass] [-port=5999]   # Controller
//	netmesh -master=10.10.10.5 [-port=5999]                # Agent, joins now
//	netmesh [-port=5999]                                    # Agent, holding state
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"netmesh/internal/agent"
	"netmesh/internal/config"
	"netmesh/internal/controller"
	"netmesh/internal/logging"
)

func main() {
	cfg, err := config.Parse("netmesh", os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "netmesh:", err)
		os.Exit(2)
	}

	// Cancel on SIGINT/SIGTERM for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cfg.Mode {
	case config.ModeController:
		log := logging.New("controller")
		if err := controller.New(cfg, log).Run(ctx); err != nil {
			log.Errorf("controller exited with error", "err", err)
			os.Exit(1)
		}
	case config.ModeAgentJoin, config.ModeAgentHolding:
		log := logging.New("agent")
		if err := agent.New(cfg, log).Run(ctx); err != nil {
			log.Errorf("agent exited with error", "err", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "netmesh: unknown mode")
		os.Exit(2)
	}
}
