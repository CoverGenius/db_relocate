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
	PUBLICATION_NAME string = "upgrade"
)

func (c *Controller) createPublication() error {
	statement := `
	CREATE publication %s
	FOR ALL TABLES;`

	err := c.writeTransaction(c.srcDatabaseConnection, &statement, PUBLICATION_NAME)

	return err
}

func (c *Controller) dropPublication() error {
	statement := `DROP publication %s;`
	err := c.writeTransaction(c.srcDatabaseConnection, &statement, PUBLICATION_NAME)

	return err
}

func (c *Controller) publicationExists() (bool, error) {
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

	exists, err := c.readTransaction(&publications, c.srcDatabaseConnection, &statement, PUBLICATION_NAME)

	return exists, err
}

func (c *Controller) ensurePublication() error {
	exists, err := c.publicationExists()
	if err != nil {
		return err
	}

	// TODO: add force flag logic
	if exists {
		err = c.dropPublication()
		if err != nil {
			return err
		}
	}

	err = c.createPublication()

	return err
}
