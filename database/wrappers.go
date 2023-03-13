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
	"database/sql"
	"db_relocate/input"
	"db_relocate/log"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

func (c *Controller) buildQuery(statement *string, args ...interface{}) *string {
	query := fmt.Sprintf(*statement, args...)

	// TODO: filter out queries which migh contain sensitive information such as 'ALTER ROLE'.
	log.Debugf("Running a query: '%s'", query)

	return &query
}

// TODO: use interface instead in order to distinguish testing and real database connections
// Or migrate to pgx.
func (c *Controller) ensureDatabaseConnection(databaseConnection *databaseConnection) error {
	if *databaseConnection.id == "test" {
		return nil
	}

	err := databaseConnection.connection.PingContext(*c.configuration.Context)
	if err == nil {
		return nil
	}

	log.Warnf(
		"Database connection with an identifier: '%s' is closed. Trying to re-establish.",
		*databaseConnection.id,
	)
	databaseConnection.connection.Close()

	connection, err := sqlx.Connect("postgres", *databaseConnection.dsn)
	if err != nil {
		log.Errorf(
			"Failed to re-establish database connection with an identifier: '%s'!",
			*databaseConnection.id,
		)
		return err
	}

	log.Infof(
		"Database connection with an identifier: '%s' was successfully re-established!",
		*databaseConnection.id,
	)
	databaseConnection.connection = connection

	return nil
}

func (c *Controller) writeTransaction(databaseConnection *databaseConnection, statement *string, args ...interface{}) error {
	query := c.buildQuery(statement, args...)

	opts := &sql.TxOptions{
		ReadOnly: false,
	}

	err := c.ensureDatabaseConnection(databaseConnection)
	if err != nil {
		return err
	}

	tx, err := databaseConnection.connection.BeginTxx(*c.configuration.Context, opts)
	if err != nil {
		return err
	}

	_, err = tx.Exec(*query)

	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return errors.New(fmt.Sprintf(
				"Received an err: '%s', while trying to rollback a transaction caused by an error: '%s'",
				rollbackErr,
				err,
			))
		}
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) simpleWriteTransaction(databaseConnection *databaseConnection, statement *string, args ...interface{}) error {
	query := c.buildQuery(statement, args...)

	err := c.ensureDatabaseConnection(databaseConnection)
	if err != nil {
		return err
	}

	_, err = databaseConnection.connection.Exec(*query)

	return err
}

func (c *Controller) getContainerLength(container interface{}) int {
	switch t := container.(type) {
	case *[]int64:
		return len(*t)
	case *[]string:
		return len(*t)
	case *[]user:
		return len(*t)
	case *[]publication:
		return len(*t)
	case *[]replicationSlot:
		return len(*t)
	case *[]tablePrivilege:
		return len(*t)
	case *[]table:
		return len(*t)
	case *[]subscription:
		return len(*t)
	default:
		return 0
	}
}

func (c *Controller) readTransaction(container interface{}, databaseConnection *databaseConnection, statement *string, args ...interface{}) (bool, error) {
	query := c.buildQuery(statement, args...)

	err := c.ensureDatabaseConnection(databaseConnection)
	if err != nil {
		return false, err
	}

	err = databaseConnection.connection.Select(container, *query)
	if err != nil {
		return false, err
	}

	if c.getContainerLength(container) > 0 {
		return true, nil
	}

	return false, nil
}

func (c *Controller) PrepareSrcDatabaseForUpgrade() error {
	err := c.ensureUpgradeUser()
	if err != nil {
		return err
	}

	publicationName := PUBLICATION_NAME
	err = c.ensurePublication(&publicationName)
	if err != nil {
		return err
	}

	err = c.ensureLogicalReplicationSlot()
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) PrepareDstDatabaseForUpgrade(latestUnhealthyLSN *string) error {
	subscriptionID, err := c.ensureSubscription()
	if err != nil {
		return err
	}

	err = c.advanceReplication(subscriptionID, latestUnhealthyLSN)
	if err != nil {
		return err
	}

	err = c.enableSubscription()
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) WaitUntilSync() error {
	startTime := time.Now()
	waitTimeout := time.Minute * WAIT_UNTIL_SYNC_TIMEOUT
	checkInterval := time.Second * 10
	replicationSlotName := REPLICATION_SLOT_NAME

	currentLSNDistance, err := c.getLSNDistanceForLogicalReplicationSlot(&replicationSlotName)
	if err != nil {
		return err
	}

	for *currentLSNDistance != 0 {
		currentTime := time.Now()
		if currentTime.Sub(startTime) > waitTimeout {
			return errors.New(fmt.Sprintf(
				"Reached a timeout '%s', while waiting for sync between old and new masters. LSN distance: '%d'",
				waitTimeout.String(),
				*currentLSNDistance,
			))
		}

		time.Sleep(checkInterval)

		currentLSNDistance, err = c.getLSNDistanceForLogicalReplicationSlot(&replicationSlotName)
		if err != nil {
			return err
		}
		log.Infof("Current LSN distance between old and new master: %d", *currentLSNDistance)

	}

	log.Infoln("Old and new masters are in sync!")

	return nil
}

func (c *Controller) PerformPostUpgradeOperations() error {
	log.Infoln("Running post-upgrade operations.")

	postUpgradeOperationsInput := []*input.BinaryInputMetadata{
		{
			Message:          "Ready to perform VACUUM and then ANALYZE on a new instance: y/n?",
			PositiveResponse: "y",
			NegativeResponse: "n",
			Handler:          c.performVacuumAndThenAnalyze,
		},
		{
			Message:          "Ready to increment sequence values on a new instance: y/n?",
			PositiveResponse: "y",
			NegativeResponse: "n",
			Handler:          c.incrementSequenceValues,
		},
	}

	for idx := range postUpgradeOperationsInput {
		positiveResponse, err := postUpgradeOperationsInput[idx].ProcessBinaryInput()
		if err != nil {
			return err
		}

		if positiveResponse {
			err = postUpgradeOperationsInput[idx].Handler()
			if err != nil {
				return err
			}
		}

	}

	return nil
}
