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
	"db_relocate/log"
	"errors"
	"fmt"
	"strings"
)

func (c *Controller) addLoginOption(user *user) error {
	statement := `ALTER ROLE %s WITH LOGIN;`

	err := c.writeTransaction(c.srcDatabaseConnection, &statement, user.Name)

	return err
}

func (c *Controller) ensureCorrectPassword(u *user, password *string) error {
	statement := `ALTER ROLE %s WITH PASSWORD '%s';`

	err := c.writeTransaction(c.srcDatabaseConnection, &statement, u.Name, *password)

	return err
}

func (c *Controller) ensureCanLogin(user *user) error {
	if user.Login != "t" && user.Login != "true" {
		log.Infof("User '%s' is missing LOGIN option. Adding it now.", user.Name)
		err := c.addLoginOption(user)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Controller) getUserByName(username *string, u *user) (bool, error) {
	// TODO: handle row level security policy if any.
	users := []user{}
	statement := `
	SELECT
		roles.rolname AS name,
		roles.rolcanlogin AS login,
		roles.rolvaliduntil AS password_valid_until,
		roles.rolbypassrls AS bypass_row_level_security_policy,
		array_to_string(
			ARRAY(
				SELECT
					mroles.rolname
				FROM pg_catalog.pg_auth_members AS members
				JOIN pg_catalog.pg_roles AS mroles ON (members.roleid = mroles.oid)
				WHERE members.member = roles.oid
			), ','
		) AS member_of
		FROM pg_catalog.pg_roles AS roles
		WHERE roles.rolname='%s';`

	exists, err := c.readTransaction(&users, c.srcDatabaseConnection, &statement, *username)
	if err != nil {
		return false, err
	}

	if !exists {
		return false, nil
	}

	*u = users[0]

	return true, nil
}

func (c *Controller) CurrentUserCanProceed() (bool, error) {
	user := user{}

	found, err := c.getUserByName(&c.configuration.Items.Src.User, &user)
	if err != nil {
		return false, err
	}

	if !found {
		return false, errors.New(fmt.Sprintf(
			"User with name: '%s' does not exist!",
			c.configuration.Items.Src.User,
		))
	}

	user.memberOfStringToMemberOfList()

	if !user.memberOf(RDS_SUPERUSER_ROLE_NAME) {
		log.Infof("Current user '%s' is missing a '%s' role!", user.Name, RDS_SUPERUSER_ROLE_NAME)
		query := fmt.Sprintf(`GRANT %s TO %s;`, RDS_SUPERUSER_ROLE_NAME, user.Name)
		log.Infof("You can fix it by running the following query: '%s'", query)
		return false, nil
	}

	databaseOwner, err := c.getDatabaseOwner(&c.configuration.Items.Src.Name)
	if err != nil {
		return false, err
	}

	if strings.ReplaceAll(*databaseOwner, `"`, "") != user.Name {
		log.Infof("Current user '%s' is not an owner of the database '%s'", user.Name, c.configuration.Items.Src.Name)
		query := fmt.Sprintf(`ALTER DATABASE %s OWNER TO %s;`, c.configuration.Items.Src.Name, user.Name)
		log.Infof("You can fix it by running following the query: '%s'.", query)
		return false, nil
	}

	return true, nil
}

func (c *Controller) ensureUpgradeUser() error {
	user, err := c.ensureUser(&c.configuration.Items.Upgrade.User, &c.configuration.Items.Upgrade.Password)
	if err != nil {
		return err
	}

	err = c.ensureReplicationPrivilege(user)
	if err != nil {
		return err
	}

	err = c.ensureReadOnlyPrivilegesForUserInSchemaAndDatabase(
		user,
		&c.configuration.Items.Src.Name,
		&c.configuration.Items.Src.Schema,
	)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) createRoleWithLogin(username *string, password *string, u *user) error {
	statement := `CREATE ROLE %s WITH LOGIN PASSWORD %s;`

	err := c.writeTransaction(c.srcDatabaseConnection, &statement, *username, *password)

	if err != nil {
		return err
	}

	found, err := c.getUserByName(username, u)
	if err != nil {
		return err
	}

	if !found {
		return errors.New(fmt.Sprintf(
			"User with a name: '%s' does not exist.",
			*username,
		))
	}

	return nil
}

func (c *Controller) ensureUser(username *string, password *string) (*user, error) {
	user := user{}
	found, err := c.getUserByName(username, &user)
	if err != nil {
		return nil, err
	}

	if !found {
		err = c.createRoleWithLogin(username, password, &user)
		if err != nil {
			return nil, err
		}
	}

	err = c.ensureCanLogin(&user)
	if err != nil {
		return nil, err
	}

	err = c.ensureCorrectPassword(&user, password)
	if err != nil {
		return nil, err
	}

	user.memberOfStringToMemberOfList()

	return &user, nil
}
