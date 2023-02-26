//  Copyright (C) 2023 The db_relocate authors.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation;
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.

package database

import "db_relocate/log"

const (
	REPLICATION_SLOT_NAME string = "upgrade"
)

func (c *Controller) createLogicalReplicationSlot() error {
	statement := `SELECT pg_create_logical_replication_slot('%s', 'pgoutput');`

	err := c.writeTransaction(c.srcDatabaseConnection, &statement, REPLICATION_SLOT_NAME)

	return err
}

func (c *Controller) dropLogicalReplicationSlot(replicationSlotName *string) error {
	statement := `SELECT pg_drop_replication_slot('%s');`

	err := c.writeTransaction(c.srcDatabaseConnection, &statement, *replicationSlotName)

	return err
}

func (c *Controller) logicalReplicationSlotExists(replicationSlotName *string) (bool, error) {
	replicationSlots := []replicationSlot{}
	statement := `
        SELECT
                slot_name AS name,
                plugin AS plugin,
                slot_type AS type,
                database AS database,
                active AS active
        FROM pg_catalog.pg_replication_slots
	WHERE slot_name = '%s';`

	exists, err := c.readTransaction(&replicationSlots, c.srcDatabaseConnection, &statement, *replicationSlotName)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (c *Controller) LogicalReplicationSlotsExists() (bool, error) {
	replicationSlots := []replicationSlot{}
	statement := `
	SELECT
		slot_name AS name,
		plugin AS plugin,
		slot_type AS type,
		database AS database,
		active AS active
	FROM pg_catalog.pg_replication_slots;`

	exists, err := c.readTransaction(&replicationSlots, c.srcDatabaseConnection, &statement)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (c *Controller) ensureLogicalReplicationSlot() error {
	replicationSlotName := REPLICATION_SLOT_NAME
	exists, err := c.logicalReplicationSlotExists(&replicationSlotName)
	if err != nil {
		return err
	}

	// TODO: add force logic
	if exists {
		err = c.dropLogicalReplicationSlot(&replicationSlotName)
		if err != nil {
			return err
		}
	}

	err = c.createLogicalReplicationSlot()

	return err
}

func (c *Controller) DropUpgradeLogicalReplicationSlot() error {
	log.Infoln("Deleting the upgrade replication slot that was used during the upgrade/migration process.")

	replicationSlotName := REPLICATION_SLOT_NAME
	exists, err := c.logicalReplicationSlotExists(&replicationSlotName)
	if err != nil {
		return err
	}

	if exists {
		replicationSlotName := REPLICATION_SLOT_NAME
		err = c.dropLogicalReplicationSlot(&replicationSlotName)
		if err != nil {
			return err
		}
	}

	return nil
}
