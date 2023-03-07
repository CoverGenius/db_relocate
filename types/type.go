//  Copyright (C) 2023 The db_relocate authors.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation;
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.

package types

import (
	"db_relocate/log"

	"context"

	"github.com/spf13/viper"
)

type DBInstanceDetails struct {
	User       string
	Password   string
	Schema     string
	Name       string
	Port       string
	Host       string
	InstanceID string
}

type UpgradeDetails struct {
	CAIdentifier     string
	SubnetGroupName  string
	EngineVersion    string
	KMSID            string
	SecurityGroupIDs []string
	ParameterGroup   string
	InstanceClass    string
	StorageType      string
	Password         string
	User             string
	VPCID            string
}

type Items struct {
	Src     *DBInstanceDetails
	Dst     *DBInstanceDetails
	Upgrade *UpgradeDetails
}

type Configuration struct {
	Context      *context.Context
	Items        *Items
	Force        bool
	LoggingLevel string
	AWSProfile   string
	AWSRegion    string
}

func (c *Configuration) initLogger() {
	log.Infoln("Initializing log object")
	log.SetLogLevel(&c.LoggingLevel)
}

func (c *Configuration) initContext() {
	cont := context.TODO()
	c.Context = &cont
}

func setDefault(v *viper.Viper) {
	v.SetDefault("logging.level", "info")
	v.SetDefault("force", false)
	v.SetDefault("aws.profile", "default")
	v.SetDefault("aws.region", "us-east-1")
	v.SetDefault("src.user", "ops")
	v.SetDefault("src.password", "secret")
	v.SetDefault("src.schema", "public")
	v.SetDefault("src.name", "postgres")
	v.SetDefault("src.port", "5432")
	v.SetDefault("src.host", "127.0.0.1")
	v.SetDefault("src.instance_id", "")
	v.SetDefault("dst.user", "")
	v.SetDefault("dst.password", "")
	v.SetDefault("dst.schema", "public")
	v.SetDefault("dst.name", "")
	v.SetDefault("dst.port", "")
	v.SetDefault("dst.host", "")
	v.SetDefault("dst.instance_id", "")
	v.SetDefault("upgrade.subnet_group", "")
	v.SetDefault("upgrade.engine_version", "")
	v.SetDefault("upgrade.kms_id", "")
	v.SetDefault("upgrade.security_groups", []string{})
	v.SetDefault("upgrade.parameter_group", "")
	v.SetDefault("upgrade.instance_class", "")
	v.SetDefault("upgrade.storage_type", "")
	v.SetDefault("upgrade.user", "upgrade")
	v.SetDefault("upgrade.password", "s4p3rs3cr3t!")
	v.SetDefault("upgrade.vpc_id", "")
	v.SetDefault("upgrade.ca_identifier", "")
}

func getSrcDBDetails(v *viper.Viper) *DBInstanceDetails {
	srcDBDetails := &DBInstanceDetails{
		User:       v.GetString("src.user"),
		Password:   v.GetString("src.password"),
		Schema:     v.GetString("src.schema"),
		Name:       v.GetString("src.name"),
		Port:       v.GetString("src.port"),
		Host:       v.GetString("src.host"),
		InstanceID: v.GetString("src.instance_id"),
	}
	return srcDBDetails
}

func getDstDBDetails(v *viper.Viper) *DBInstanceDetails {
	dstDBDetails := &DBInstanceDetails{
		User:       v.GetString("dst.user"),
		Password:   v.GetString("dst.password"),
		Schema:     v.GetString("dst.schema"),
		Name:       v.GetString("dst.name"),
		Port:       v.GetString("dst.port"),
		Host:       v.GetString("dst.host"),
		InstanceID: v.GetString("dst.instance_id"),
	}
	return dstDBDetails
}

func getUpgradeDetails(v *viper.Viper) *UpgradeDetails {
	upgradeDetails := &UpgradeDetails{
		SubnetGroupName:  v.GetString("upgrade.subnet_group"),
		EngineVersion:    v.GetString("upgrade.engine_version"),
		KMSID:            v.GetString("upgrade.kms_id"),
		SecurityGroupIDs: v.GetStringSlice("upgrade.security_groups"),
		ParameterGroup:   v.GetString("upgrade.parameter_group"),
		InstanceClass:    v.GetString("upgrade.instance_class"),
		StorageType:      v.GetString("upgrade.storage_type"),
		User:             v.GetString("upgrade.user"),
		Password:         v.GetString("upgrade.password"),
		VPCID:            v.GetString("upgrade.vpc_id"),
		CAIdentifier:     v.GetString("upgrade.ca_identifier"),
	}
	return upgradeDetails
}

func readConfig(v *viper.Viper, configuration *Configuration) {
	srcDBDetails := getSrcDBDetails(v)
	dstDBDetails := getDstDBDetails(v)
	upgradeDetails := getUpgradeDetails(v)
	items := &Items{
		Src:     srcDBDetails,
		Dst:     dstDBDetails,
		Upgrade: upgradeDetails,
	}
	configuration.LoggingLevel = v.GetString("logging.level")
	configuration.Force = v.GetBool("force")
	configuration.AWSRegion = v.GetString("aws.region")
	configuration.AWSProfile = v.GetString("aws.profile")
	configuration.Items = items
}

func ReadConfiguration(v *viper.Viper) *Configuration {
	c := &Configuration{}
	c.initContext()
	setDefault(v)
	readConfig(v, c)
	c.initLogger()
	return c
}
