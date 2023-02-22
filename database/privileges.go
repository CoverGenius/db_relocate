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

const (
	RDS_SUPERUSER_ROLE_NAME   string = "rds_superuser"
	RDS_REPLICATION_ROLE_NAME string = "rds_replication"
)

func (c *Controller) getDatabaseOwner(database *string) (*string, error) {
	owners := []string{}

	statement := `
	SELECT
		datdba::regrole AS owner
	FROM
		pg_catalog.pg_database
	WHERE
		datname = '%s';`

	_, err := c.readTransaction(&owners, c.srcDatabaseConnection, &statement, *database)
	if err != nil {
		return nil, err
	}

	return &owners[0], nil
}

func (c *Controller) listMissingReadOnlyPrivilegesForTablesInSchemaAndDatabaseForUser(u *user, database *string, schema *string) (bool, error) {
	tablePrivileges := []tablePrivilege{}

	statement := `
	SELECT
		t.table_catalog AS catalog,
		t.table_schema AS schema,
		t.table_name AS name,
		t.table_type AS table_type,
		tp.privilege_type AS privilege_type,
		tp.grantee AS grantee
	FROM
		information_schema.tables AS t
	LEFT JOIN
		information_schema.table_privileges AS tp
	ON
		(
			tp.table_name = t.table_name
		AND
			tp.grantee = '%s'
		AND
			tp.privilege_type = 'SELECT'
		)
	WHERE
		(
			t.table_catalog = '%s'
		AND
			t.table_schema = '%s'
		AND
			table_type = 'BASE TABLE'
		AND
			tp.privilege_type IS NULL
		);`

	result, err := c.readTransaction(&tablePrivileges, c.srcDatabaseConnection, &statement, u.Name, *database, *schema)

	return result, err
}

func (c *Controller) ensureReadOnlyPrivilegesForUserInSchemaAndDatabase(u *user, database *string, schema *string) error {
	foundMissingPrivileges, err := c.listMissingReadOnlyPrivilegesForTablesInSchemaAndDatabaseForUser(u, database, schema)
	if err != nil {
		return err
	}

	if !foundMissingPrivileges {
		return nil
	}

	statement := `GRANT SELECT ON ALL TABLES IN SCHEMA %s TO %s;`

	err = c.writeTransaction(
		c.srcDatabaseConnection,
		&statement,
		c.configuration.Items.Src.Schema,
		c.configuration.Items.Upgrade.User,
	)

	return err
}

func (c *Controller) ensureReplicationPrivilege(u *user) error {
	if u.memberOf(RDS_REPLICATION_ROLE_NAME) {
		return nil
	}

	statement := `GRANT %s TO %s;`

	err := c.writeTransaction(c.srcDatabaseConnection, &statement, RDS_REPLICATION_ROLE_NAME, u.Name)

	return err
}
