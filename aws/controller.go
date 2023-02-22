// Copyright (C) 2023 The db_relocate authors.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation;
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.

package aws

import (
	a "github.com/aws/aws-sdk-go-v2/aws"
	c "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/rds"

	"db_relocate/log"
	"db_relocate/types"

	"context"

	"os"
)

type Controller struct {
	session       *a.Config
	rdsClient     *rds.Client
	cwClient      *cloudwatch.Client
	ec2Client     *ec2.Client
	kmsClient     *kms.Client
	errorChannel  chan error
	configuration *types.Configuration
}

func initSession(profile *string, region *string, context *context.Context) (*a.Config, error) {
	log.Infof("Using AWS profile with name: '%s'", *profile)

	os.Setenv("AWS_PROFILE", *profile)
	config, err := c.LoadDefaultConfig(*context, c.WithRegion(*region))
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func NewController(configuration *types.Configuration, errorChannel chan error) (*Controller, error) {
	session, err := initSession(
		&configuration.AWSProfile,
		&configuration.AWSRegion,
		configuration.Context,
	)
	if err != nil {
		return nil, err
	}

	controller := Controller{
		session:       session,
		errorChannel:  errorChannel,
		configuration: configuration,
	}
	controller.initRDSClient()
	controller.initCWClient()
	controller.initEC2Client()
	controller.initKMSClient()

	return &controller, nil
}

func (c *Controller) initRDSClient() {
	client := rds.NewFromConfig(*c.session)
	c.rdsClient = client
}

func (c *Controller) initCWClient() {
	client := cloudwatch.NewFromConfig(*c.session)
	c.cwClient = client
}

func (c *Controller) initEC2Client() {
	client := ec2.NewFromConfig(*c.session)
	c.ec2Client = client
}

func (c *Controller) initKMSClient() {
	client := kms.NewFromConfig(*c.session)
	c.kmsClient = client
}
