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

const (
	REPLICATION_SLOT_NAME string = "upgrade"
)

func (c *Controller) createLogicalReplicationSlot() error {
	statement := `SELECT pg_create_logical_replication_slot('%s', 'pgoutput');`

	err := c.writeTransaction(c.srcDatabaseConnection, &statement, REPLICATION_SLOT_NAME)

	return err
}

func (c *Controller) dropLogicalReplicationSlot() error {
	statement := `SELECT pg_drop_replication_slot('%s');`

	err := c.writeTransaction(c.srcDatabaseConnection, &statement, REPLICATION_SLOT_NAME)

	return err
}

func (c *Controller) logicalReplicationSlotExists() (bool, error) {
	replicationSlots := []replicationSlot{}
	statement := `
	SELECT
		p.slot_name AS name,
		p.plugin AS plugin,
		p.slot_type AS type,
		p.database AS database,
		p.active AS active
	FROM pg_catalog.pg_replication_slots AS p
	WHERE p.slot_name = '%s';`

	exists, err := c.readTransaction(&replicationSlots, c.srcDatabaseConnection, &statement, REPLICATION_SLOT_NAME)

	return exists, err
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

	return exists, err
}

func (c *Controller) ensureLogicalReplicationSlot() error {
	exists, err := c.logicalReplicationSlotExists()
	if err != nil {
		return err
	}

	// TODO: add force logic
	if exists {
		err = c.dropLogicalReplicationSlot()
		if err != nil {
			return err
		}
	}

	err = c.createLogicalReplicationSlot()

	return err
}
