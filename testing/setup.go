// Copyright (C) 2023 The db_relocate authors.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation;
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.

package testing

import (
	"context"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"

	"log"
)

type DatabaseMockData struct {
	Context    *context.Context
	Connection *sqlx.DB
	Mock       *sqlmock.Sqlmock
}

func SetupDatabaseMockData() *DatabaseMockData {
	cont := context.TODO()

	connection, mock, err := sqlmock.New()
	if err != nil {
		log.Fatalf("Failed to initialize database mock data! Received an error: %v", err)
	}

	return &DatabaseMockData{
		Context:    &cont,
		Connection: sqlx.NewDb(connection, "sqlmock"),
		Mock:       &mock,
	}
}
