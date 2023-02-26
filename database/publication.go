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
	PUBLICATION_NAME string = "upgrade"
)

func (c *Controller) createPublication(publicationName *string) error {
	statement := `
	CREATE publication %s
	FOR ALL TABLES;`

	err := c.writeTransaction(c.srcDatabaseConnection, &statement, *publicationName)

	return err
}

func (c *Controller) dropPublication(publicationName *string) error {
	statement := `DROP publication %s;`
	err := c.writeTransaction(c.srcDatabaseConnection, &statement, *publicationName)

	return err
}

func (c *Controller) publicationExists(publicationName *string) (bool, error) {
	publications := []publication{}

	statement := `
	SELECT
		p.pubname AS name,
		p.pubowner AS owner,
		p.puballtables AS all_tables,
		p.pubinsert AS insert,
		p.pubupdate AS update,
		p.pubdelete AS delete
	FROM pg_catalog.pg_publication AS p
	WHERE p.pubname = '%s';`

	exists, err := c.readTransaction(&publications, c.srcDatabaseConnection, &statement, *publicationName)

	return exists, err
}

func (c *Controller) ensurePublication(publicationName *string) error {
	exists, err := c.publicationExists(publicationName)
	if err != nil {
		return err
	}

	// TODO: add force flag logic
	if exists {
		err = c.dropPublication(publicationName)
		if err != nil {
			return err
		}
	}

	err = c.createPublication(publicationName)

	return err
}

func (c *Controller) DropUpgradePublication() error {
	log.Infoln("Deleting the upgrade publication that was used during the upgrade/migration process.")

	publicationName := PUBLICATION_NAME
	exists, err := c.publicationExists(&publicationName)
	if err != nil {
		return err
	}

	if exists {
		err = c.dropPublication(&publicationName)
		if err != nil {
			return err
		}
	}

	return nil
}
