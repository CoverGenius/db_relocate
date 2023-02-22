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
	thelper "db_relocate/testing"
	"db_relocate/types"
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type Controller struct {
	srcDatabaseConnection *sqlx.DB
	dstDatabaseConnection *sqlx.DB
	configuration         *types.Configuration
	errorChannel          chan error
}

func initDatabaseConnection(user *string, password *string, host *string, port *string, name *string) (*sqlx.DB, error) {
	dsn := fmt.Sprintf(
		"user=%s password=%s host=%s port=%s dbname=%s sslmode=require",
		*user,
		*password,
		*host,
		*port,
		*name,
	)
	connection, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, err
	}
	return connection, nil
}

func (c *Controller) InitUpgradeDatabaseConnection(host *string) error {
	log.Infof("Initializing destination database connection to host: %s", *host)

	connection, err := initDatabaseConnection(
		&c.configuration.Items.Src.User,
		&c.configuration.Items.Src.Password,
		host,
		&c.configuration.Items.Src.Port,
		&c.configuration.Items.Src.Name,
	)

	if err != nil {
		return err
	}

	c.dstDatabaseConnection = connection

	return nil
}

func NewController(configuration *types.Configuration, errorChannel chan error) (*Controller, error) {
	log.Infoln("Initializing database controller.")

	log.Infoln("Initializing source database connection.")
	connection, err := initDatabaseConnection(
		&configuration.Items.Src.User,
		&configuration.Items.Src.Password,
		&configuration.Items.Src.Host,
		&configuration.Items.Src.Port,
		&configuration.Items.Src.Name,
	)
	if err != nil {
		return nil, err
	}

	return &Controller{
		srcDatabaseConnection: connection,
		configuration:         configuration,
		errorChannel:          errorChannel,
	}, nil
}

func setupDatabaseMockData() (*Controller, *sqlmock.Sqlmock) {
	databaseMockData := thelper.SetupDatabaseMockData()

	return &Controller{
		srcDatabaseConnection: databaseMockData.Connection,
		dstDatabaseConnection: databaseMockData.Connection,
		configuration: &types.Configuration{
			Context: databaseMockData.Context,
		},
	}, databaseMockData.Mock
}
