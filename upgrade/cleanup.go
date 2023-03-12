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

	cleanupOperationsInput := []*input.BinaryInputMetadata{
		{
			Message:          "Ready to delete a subscription that was used in an upgrade/migrations process: y/n?",
			PositiveResponse: "y",
			NegativeResponse: "n",
			Handler:          c.databaseController.DeleteUpgradeSubscription,
		},
		{
			Message:          "Ready to drop a table that was used in an upgrade/migrations process for healthcheck purposes: y/n?",
			PositiveResponse: "y",
			NegativeResponse: "n",
			Handler:          c.databaseController.DropHealthCheckTable,
		},
		{
			Message:          "Ready to delete a user that was used in an upgrade/migrations process: y/n?",
			PositiveResponse: "y",
			NegativeResponse: "n",
			Handler:          c.databaseController.DeleteUpgradeUser,
		},
		{
			Message:          "Ready to drop a replication slot that was used in an upgrade/migrations process: y/n?",
			PositiveResponse: "y",
			NegativeResponse: "n",
			Handler:          c.databaseController.DropUpgradeLogicalReplicationSlot,
		},
		{
			Message:          "Ready to drop a publication that was used in an upgrade/migrations process: y/n?",
			PositiveResponse: "y",
			NegativeResponse: "n",
			Handler:          c.databaseController.DropUpgradePublication,
		},
	}

	for idx := range cleanupOperationsInput {
		positiveResponse, err := cleanupOperationsInput[idx].ProcessBinaryInput()
		if err != nil {
			return err
		}

		if positiveResponse {
			err = cleanupOperationsInput[idx].Handler()
			if err != nil {
				return err
			}
		}

	}

	stopAWSSrcDBInstanceInput := &input.BinaryInputMetadata{
		Message:          "Ready to stop an RDS instance that was used as a donor in an upgrade/migrations process: y/n?",
		PositiveResponse: "y",
		NegativeResponse: "n",
	}
	positiveResponse, err := stopAWSSrcDBInstanceInput.ProcessBinaryInput()
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
