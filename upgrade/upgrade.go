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
	"db_relocate/log"
	"time"
)

const (
	COOLDOWN_PERIOD time.Duration = 5 // minutes
)

func (c *Controller) Run() error {
	now := time.Now().UTC()
	instance, err := c.runPreFlightChecks(&now)
	if err != nil {
		return err
	}

	err = c.ensureParametersOnSrcDB(instance)
	if err != nil {
		return err
	}

	heartBeatRecords := []int64{}
	heartbeatTicker, heartBeatDoneChannel := c.databaseController.BeginHealthCheckProcess(&heartBeatRecords)

	err = c.databaseController.PrepareSrcDatabaseForUpgrade()
	if err != nil {
		return err
	}

	timeBeforeSnapshot := time.Now().UTC()

	newInstance, err := c.awsController.RunDBSnapshotMaintenance(instance)
	if err != nil {
		return err
	}

	timeAfterRestore := time.Now().UTC()

	latestUnhealthyLSN, err := c.awsController.SearchLogFilesForEarliestUnhealthyLSN(newInstance, &timeBeforeSnapshot, &timeAfterRestore)
	if err != nil {
		return err
	}

	err = c.ensureParametersOnDstDB(newInstance)
	if err != nil {
		return err
	}

	err = c.databaseController.InitUpgradeDatabaseConnection(newInstance.Endpoint.Address)
	if err != nil {
		return err
	}

	err = c.databaseController.PrepareDstDatabaseForUpgrade(latestUnhealthyLSN)
	if err != nil {
		return err
	}

	heartbeatTicker.Stop()
	heartBeatDoneChannel <- true

	time.Sleep(COOLDOWN_PERIOD * time.Minute)

	err = c.databaseController.CompareSendAndReceivedHeartbeatRecords(heartBeatRecords)
	if err != nil {
		return err
	}

	log.Infoln("Database snapshot has been upgraded and restored.")
	log.Infoln("Replication is up and running. All health check records have been synced.")

	err = c.databaseController.IncrementSequenceValues()
	if err != nil {
		return err
	}

	err = c.performCleanup(instance)
	if err != nil {
		return err
	}

	log.Infoln("Success!")

	return nil
}
