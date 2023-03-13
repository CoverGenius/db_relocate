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

func (c *Controller) tableExists(databaseConnection *databaseConnection, tableName *string) (bool, error) {
	tables := []table{}
	statement := `
	SELECT
		table_catalog AS catalog,
		table_schema AS schema,
		table_name AS name,
		table_type AS type
	FROM information_schema.tables
	WHERE
		table_catalog='%s'
	AND
		table_schema='%s'
	AND
		table_name='%s'
	AND
		table_type='BASE TABLE'`

	exists, err := c.readTransaction(
		&tables,
		databaseConnection,
		&statement,
		c.configuration.Items.Src.Name,
		c.configuration.Items.Src.Schema,
		*tableName,
	)

	return exists, err
}

func (c *Controller) createHealthCheckTable(table *string) error {
	statement := `
	CREATE TABLE %s (
		timestamp NUMERIC
	);`

	err := c.writeTransaction(c.srcDatabaseConnection, &statement, *table)

	return err
}

func (c *Controller) truncateTable(table *string) error {
	statement := `TRUNCATE TABLE %s;`

	err := c.writeTransaction(c.srcDatabaseConnection, &statement, *table)

	return err
}

func (c *Controller) dropTable(databaseConnection *databaseConnection, table *string) error {
	statement := `DROP TABLE %s;`

	err := c.writeTransaction(databaseConnection, &statement, *table)

	return err
}
