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

func (c *Controller) writeTransaction(connection *sqlx.DB, statement *string, args ...interface{}) error {
	query := c.buildQuery(statement, args...)

	opts := &sql.TxOptions{
		ReadOnly: false,
	}
	tx, err := connection.BeginTxx(*c.configuration.Context, opts)
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

func (c *Controller) simpleWriteTransaction(connection *sqlx.DB, statement *string, args ...interface{}) error {
	query := c.buildQuery(statement, args...)

	_, err := connection.Exec(*query)

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

func (c *Controller) readTransaction(container interface{}, connection *sqlx.DB, statement *string, args ...interface{}) (bool, error) {
	query := c.buildQuery(statement, args...)

	err := connection.Select(container, *query)
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
