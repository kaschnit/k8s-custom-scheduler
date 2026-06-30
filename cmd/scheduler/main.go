package main

import (
	"os"

	"github.com/kaschnit/kaschnit-scheduler/internal/plugin/quotaawarepreempt"
	"k8s.io/component-base/cli"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
)

func main() {
	command := app.NewSchedulerCommand(quotaawarepreempt.WithPlugin())
	exitStatus := cli.Run(command)
	os.Exit(exitStatus)
}
