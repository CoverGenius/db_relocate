//  Copyright (C) 2023 The db_relocate authors.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation;
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.

package cmd

import (
	"db_relocate/aws"
	"db_relocate/database"
	"db_relocate/types"
	"db_relocate/upgrade"

	"github.com/spf13/viper"
)

type RunCmd struct{}

func (pc *RunCmd) Run(v *viper.Viper, errorChannel chan error) error {
	configuration := types.ReadConfiguration(v)

	dbController, err := database.NewController(configuration, errorChannel)
	if err != nil {
		return err
	}

	awsController, err := aws.NewController(configuration, errorChannel)
	if err != nil {
		return err
	}

	upgradeController := upgrade.NewController(
		configuration,
		dbController,
		awsController,
		errorChannel,
	)

	if err := upgradeController.Run(); err != nil {
		return err
	}

	return nil
}
