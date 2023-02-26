// Copyright (C) 2023 The db_relocate authors.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation;
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.

package database

import (
	"errors"
	"fmt"
	"time"

	"db_relocate/log"
)

const (
	HEALTHCHECK_TABLE_NAME string        = "healthcheck_heartbeats"
	HEALTHCHECK_INTERVAL   time.Duration = 1 // seconds
)

func (c *Controller) fetchHeartBeatRecords() ([]int64, error) {
	heartBeatRecords := []int64{}

	statement := `SELECT timestamp FROM %s;`

	_, err := c.readTransaction(&heartBeatRecords, c.dstDatabaseConnection, &statement, HEALTHCHECK_TABLE_NAME)

	return heartBeatRecords, err
}

func (c *Controller) CompareSendAndReceivedHeartbeatRecords(sendHeartBeatRecords []int64) error {
	receivedHeartBeatRecords, err := c.fetchHeartBeatRecords()
	if err != nil {
		return err
	}

	if len(receivedHeartBeatRecords) != len(sendHeartBeatRecords) {
		return errors.New(fmt.Sprintf(
			"Found inconsistencies within heartbeat records!"+
				"Number of send records: %d."+
				"Number of received records: %d.",
			len(sendHeartBeatRecords),
			len(receivedHeartBeatRecords),
		))
	}

	difference := []int64{}

	for idx := range sendHeartBeatRecords {
		if sendHeartBeatRecords[idx] != receivedHeartBeatRecords[idx] {
			difference = append(difference, receivedHeartBeatRecords[idx])
		}
	}

	if len(difference) > 0 {
		return errors.New(fmt.Sprintf(
			"Found inconsistencies within heartbeat records!"+
				"Following records are missing on the receiver: %d",
			difference,
		))
	}

	return nil
}
func (c *Controller) insertHeartBeatRecord(timestamp *int64) error {
	log.Debugf("Inserting a new heart beat record with value: %d", *timestamp)

	statement := `INSERT INTO %s (timestamp) VALUES(%d);`
	err := c.writeTransaction(c.srcDatabaseConnection, &statement, HEALTHCHECK_TABLE_NAME, *timestamp)

	return err
}

func (c *Controller) BeginHealthCheckProcess(heartBeatRecords *[]int64) (*time.Ticker, chan bool) {
	log.Infoln("Starting a health check process")

	healthCheckProcessTicker := time.NewTicker(HEALTHCHECK_INTERVAL * time.Second)
	healthCheckCompletionChannel := make(chan bool)

	healthCheckTableName := HEALTHCHECK_TABLE_NAME

	exists, err := c.tableExists(c.srcDatabaseConnection, &healthCheckTableName)
	if err != nil {
		c.errorChannel <- errors.New(fmt.Sprintf(
			"Failed to perform a health check table lookup: '%s'. Received an error: '%s'",
			HEALTHCHECK_TABLE_NAME,
			err,
		))
	}

	if !exists {
		err := c.createHealthCheckTable(&healthCheckTableName)
		if err != nil {
			c.errorChannel <- errors.New(fmt.Sprintf(
				"Failed to create a health check table: '%s'. Received an error: '%s'",
				HEALTHCHECK_TABLE_NAME,
				err,
			))
		}
	}

	err = c.truncateTable(&healthCheckTableName)
	if err != nil {
		c.errorChannel <- errors.New(fmt.Sprintf(
			"Failed to truncate a health check table: '%s'. Received an error: '%s'",
			HEALTHCHECK_TABLE_NAME,
			err,
		))
	}

	go func() {
		for {
			select {
			case <-healthCheckCompletionChannel:
				return
			case t := <-healthCheckProcessTicker.C:
				timestamp := t.UnixMilli()
				*heartBeatRecords = append(*heartBeatRecords, timestamp)
				if err := c.insertHeartBeatRecord(&timestamp); err != nil {
					c.errorChannel <- err
					return
				}
			}
		}
	}()

	return healthCheckProcessTicker, healthCheckCompletionChannel
}

func (c *Controller) DropHealthCheckTable() error {
	log.Infoln("Deleting the healthcheck table that was used during the upgrade/migration process.")
	healthCheckTableName := HEALTHCHECK_TABLE_NAME
	existsOnSrc, err := c.tableExists(c.srcDatabaseConnection, &healthCheckTableName)
	if err != nil {
		return err
	}

	if existsOnSrc {
		err = c.dropTable(c.srcDatabaseConnection, &healthCheckTableName)
		if err != nil {
			return err
		}
	}

	existsOnDst, err := c.tableExists(c.dstDatabaseConnection, &healthCheckTableName)
	if err != nil {
		return err
	}

	if existsOnDst {
		err = c.dropTable(c.dstDatabaseConnection, &healthCheckTableName)
		if err != nil {
			return err
		}
	}

	return nil
}
