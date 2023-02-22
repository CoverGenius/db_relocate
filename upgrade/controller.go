// Copyright (C) 2023 The db_relocate authors.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation;
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.

package upgrade

import (
	"db_relocate/aws"
	"db_relocate/database"
	"db_relocate/log"

	"db_relocate/types"
)

type Controller struct {
	awsController      *aws.Controller
	databaseController *database.Controller
	configuration      *types.Configuration
	errorChannel       chan error
}

func NewController(configuration *types.Configuration, databaseController *database.Controller, awsController *aws.Controller, errorChannel chan error) *Controller {
	log.Infoln("Initializing upgrade controller")

	return &Controller{
		awsController:      awsController,
		databaseController: databaseController,
		errorChannel:       errorChannel,
		configuration:      configuration,
	}
}
