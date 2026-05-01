package main

import (
	"os"

	"github.com/kaschnit/custom-scheduler/quotaawarepreempt"
	"k8s.io/component-base/cli"
	scheduler "k8s.io/kubernetes/cmd/kube-scheduler/app"
)

func main() {
	command := scheduler.NewSchedulerCommand(
		scheduler.WithPlugin(quotaawarepreempt.PluginName, quotaawarepreempt.NewPlugin),
	)

	exitStatus := cli.Run(command)
	os.Exit(exitStatus)
}
