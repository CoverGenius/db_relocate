//  Copyright (C) 2023 The db_relocate authors.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation;
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.

package main

import (
	"db_relocate/cmd"
	"db_relocate/errors"
	"db_relocate/log"

	"github.com/alecthomas/kong"
	"github.com/spf13/viper"
)

var (
	v *viper.Viper
)

var cli struct {
	ConfigPath string `name:"config" short:"c" default:"." help:"Directory to search for config.yaml file" type:"path"`

	Run cmd.RunCmd `cmd:"" run:"initiate relocate routine"`
}

func initConfig(path string) *viper.Viper {
	v = viper.New()
	v.SetConfigName("config")
	v.AddConfigPath(path)
	v.AutomaticEnv()
	v.SetConfigType("yaml")
	if err := v.ReadInConfig(); err != nil {
		log.Fatalln("Failed to read the config file!")
	}
	return v
}

func main() {
	log.Infoln("Starting main routine")

	errorChannel := errors.InitializeBackgroundErrorChecking()

	ctx := kong.Parse(&cli, kong.UsageOnError())

	v = initConfig(cli.ConfigPath)
	ctx.Bind(v, errorChannel)

	err := ctx.Run()
	if err != nil {
		log.Errorln(err)
		log.Fatalf("Failed to run a context!")
	}
}
