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

type databaseConnection struct {
	connection *sqlx.DB
	dsn        *string
	id         *string
}

type Controller struct {
	srcDatabaseConnection *databaseConnection
	dstDatabaseConnection *databaseConnection
	configuration         *types.Configuration
	errorChannel          chan error
}

func initDatabaseConnection(user *string, password *string, host *string, port *string, name *string, id *string) (*databaseConnection, error) {
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

	databaseConnection := &databaseConnection{
		connection: connection,
		dsn:        &dsn,
		id:         id,
	}

	return databaseConnection, nil
}

func (c *Controller) InitDestinationDatabaseConnection(host *string) error {
	log.Infof("Initializing destination database connection to host: %s", *host)

	connectionId := "destination"

	connection, err := initDatabaseConnection(
		&c.configuration.Items.Src.User,
		&c.configuration.Items.Src.Password,
		host,
		&c.configuration.Items.Src.Port,
		&c.configuration.Items.Src.Name,
		&connectionId,
	)

	if err != nil {
		return err
	}

	c.dstDatabaseConnection = connection

	return nil
}

func (c *Controller) InitSourceDatabaseConnection() error {
	log.Infoln("Initializing source database connection.")

	connectionId := "source"

	connection, err := initDatabaseConnection(
		&c.configuration.Items.Src.User,
		&c.configuration.Items.Src.Password,
		&c.configuration.Items.Src.Host,
		&c.configuration.Items.Src.Port,
		&c.configuration.Items.Src.Name,
		&connectionId,
	)

	if err != nil {
		return err
	}

	c.srcDatabaseConnection = connection

	return nil
}

func NewController(configuration *types.Configuration, errorChannel chan error) (*Controller, error) {
	log.Infoln("Initializing database controller.")
	controller := &Controller{
		configuration: configuration,
		errorChannel:  errorChannel,
	}

	err := controller.InitSourceDatabaseConnection()
	if err != nil {
		return nil, err
	}

	return controller, nil
}

func setupDatabaseMockData() (*Controller, *sqlmock.Sqlmock) {
	databaseMockData := thelper.SetupDatabaseMockData()
	connectionId := "test"

	databaseConnection := &databaseConnection{
		connection: databaseMockData.Connection,
		dsn:        databaseMockData.DSN,
		id:         &connectionId,
	}

	return &Controller{
		srcDatabaseConnection: databaseConnection,
		dstDatabaseConnection: databaseConnection,
		configuration: &types.Configuration{
			Context: databaseMockData.Context,
		},
	}, databaseMockData.Mock
}
