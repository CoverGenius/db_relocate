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
	"db_relocate/input"
	"db_relocate/log"

	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

func (c *Controller) performCleanup(instance *rdsTypes.DBInstance) error {
	log.Infoln("Running cleanup operations.")

	cleanupOperations := []*cleanupOperation{
		{
			message:          "Ready to delete a subscription that was used in an upgrade/migrations process: y/n?",
			positiveResponse: "y",
			negativeResponse: "n",
			cleanupFunction:  c.databaseController.DeleteUpgradeSubscription,
		},
		{
			message:          "Ready to drop a table that was used in an upgrade/migrations process for healthcheck purposes: y/n?",
			positiveResponse: "y",
			negativeResponse: "n",
			cleanupFunction:  c.databaseController.DropHealthCheckTable,
		},
		{
			message:          "Ready to delete a user that was used in an upgrade/migrations process: y/n?",
			positiveResponse: "y",
			negativeResponse: "n",
			cleanupFunction:  c.databaseController.DeleteUpgradeUser,
		},
		{
			message:          "Ready to drop a replication slot that was used in an upgrade/migrations process: y/n?",
			positiveResponse: "y",
			negativeResponse: "n",
			cleanupFunction:  c.databaseController.DropUpgradeLogicalReplicationSlot,
		},
		{
			message:          "Ready to drop a publication that was used in an upgrade/migrations process: y/n?",
			positiveResponse: "y",
			negativeResponse: "n",
			cleanupFunction:  c.databaseController.DropUpgradePublication,
		},
	}

	for idx := range cleanupOperations {
		positiveResponse, err := input.ProcessBinaryInput(
			&cleanupOperations[idx].message,
			&cleanupOperations[idx].positiveResponse,
			&cleanupOperations[idx].negativeResponse,
		)
		if err != nil {
			return err
		}

		if positiveResponse {
			err = cleanupOperations[idx].cleanupFunction()
			if err != nil {
				return err
			}
		}
	}

	awsSrcDBInstance := &cleanupOperation{
		message:          "Ready to stop an RDS instance that was used as a donor in an upgrade/migrations process: y/n?",
		positiveResponse: "y",
		negativeResponse: "n",
	}
	positiveResponse, err := input.ProcessBinaryInput(
		&awsSrcDBInstance.message,
		&awsSrcDBInstance.positiveResponse,
		&awsSrcDBInstance.negativeResponse,
	)
	if err != nil {
		return err
	}

	if positiveResponse {
		err = c.awsController.StopSrcDBInstance(instance)
		if err != nil {
			return err
		}
	}

	log.Infoln("All cleanup operations have been completed!")

	return nil
}
