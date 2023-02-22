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

import (
	"errors"
	"fmt"
)

const (
	SUBSCRIPTION_NAME string = "upgrade"
)

func (c *Controller) createDisabledSubscription() error {
	statement := `
	CREATE SUBSCRIPTION %s
	CONNECTION 'host=%s port=%s dbname=%s user=%s password=%s'
	PUBLICATION %s
	WITH (
		copy_data = false,
		synchronous_commit = false,
		connect = true,
		enabled = false,
		create_slot = false,
		slot_name = '%s'
	);`

	err := c.writeTransaction(
		c.dstDatabaseConnection,
		&statement,
		SUBSCRIPTION_NAME,
		c.configuration.Items.Src.Host,
		c.configuration.Items.Src.Port,
		c.configuration.Items.Src.Name,
		c.configuration.Items.Upgrade.User,
		c.configuration.Items.Upgrade.Password,
		PUBLICATION_NAME,
		REPLICATION_SLOT_NAME,
	)

	return err
}

func (c *Controller) getSubscriptionID() (*string, error) {
	subscriptionIDs := []string{}

	statement := `
	SELECT 'pg_'||oid::text AS "external_id"
	FROM pg_subscription
	WHERE subname = '%s';`

	exists, err := c.readTransaction(&subscriptionIDs, c.dstDatabaseConnection, &statement, SUBSCRIPTION_NAME)
	if err != nil {
		return nil, err
	}

	if !exists {
		errMsg := fmt.Sprintf("Subscription: '%s'", SUBSCRIPTION_NAME)
		return nil, errors.New(errMsg)
	}

	return &subscriptionIDs[0], nil
}

func (c *Controller) ensureSubscription() (*string, error) {
	// No need to check existing subscriptions because the instance was just restored from a snapshot.
	err := c.createDisabledSubscription()
	if err != nil {
		return nil, err
	}

	subscriptionID, err := c.getSubscriptionID()
	if err != nil {
		return nil, err
	}

	return subscriptionID, nil
}

func (c *Controller) advanceReplication(subscriptionID *string, positionID *string) error {
	statement := `SELECT pg_replication_origin_advance('%s', '%s');`

	err := c.writeTransaction(c.dstDatabaseConnection, &statement, *subscriptionID, *positionID)

	return err
}

func (c *Controller) enableSubscription() error {
	statement := `ALTER SUBSCRIPTION %s ENABLE;`

	err := c.writeTransaction(c.dstDatabaseConnection, &statement, SUBSCRIPTION_NAME)

	return err
}
