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

import (
	"database/sql/driver"
	thelper "db_relocate/testing"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestBuildQuery(t *testing.T) {
	c := &Controller{}

	tests := []struct {
		name      string
		statement string
		args      []interface{}
		expected  string
	}{
		{
			name:      "Non-empty statement, no args",
			statement: "SELECT id FROM users;",
			args:      nil,
			expected:  "SELECT id FROM users;",
		},
		{
			name:      "Non-empty statement, 1 string arg",
			statement: "SELECT id FROM owners WHERE email='%s';",
			args:      []interface{}{"jhon@example.com"},
			expected:  "SELECT id FROM owners WHERE email='jhon@example.com';",
		},
		{
			name:      "Non-empty statement, 1 string arg, 1 int arg",
			statement: "INSERT INTO %s (timestamp) VALUES(%d);",
			args:      []interface{}{"events", 111133334444},
			expected:  "INSERT INTO events (timestamp) VALUES(111133334444);",
		},
		{
			name:      "Empty statement, no args",
			statement: "",
			args:      nil,
			expected:  "",
		},
		{
			name:      "Empty statement, 1 arg",
			statement: "",
			args:      []interface{}{10},
			expected:  "%!(EXTRA int=10)",
		},
	}
	for _, test := range tests {
		testFunction := func(t *testing.T) {
			received := c.buildQuery(&test.statement, test.args...)
			assert.Equal(t, test.expected, *received, "received statement must match expected statement")
		}
		t.Run(test.name, testFunction)
	}
}

func TestWriteTransaction(t *testing.T) {
	c, mock := setupDatabaseMockData()

	tests := []struct {
		name          string
		statement     string
		args          []interface{}
		expectedError error
		rollback      bool
	}{
		{
			name:          "valid write transaction",
			statement:     `INSERT INTO %s (timestamp) VALUES(%d);`,
			args:          []interface{}{"test", 12345},
			expectedError: nil,
			rollback:      false,
		},
	}
	for _, test := range tests {
		testFunction := func(t *testing.T) {
			(*mock).ExpectBegin()
			query := c.buildQuery(&test.statement, test.args...)
			thelper.EscapeParanthesis(query)
			(*mock).ExpectExec(*query).WillReturnResult(sqlmock.NewResult(0, 0)).WillReturnError(test.expectedError)
			if test.rollback {
				(*mock).ExpectRollback()
			} else {
				(*mock).ExpectCommit()
			}

			err := c.writeTransaction(c.srcDatabaseConnection, &test.statement, test.args...)
			assert.NoError(t, err, "no error must be raised")

			if err := (*mock).ExpectationsWereMet(); err != nil {
				assert.NoError(t, err, "expectation must be fulfilled")
			}
		}
		t.Run(test.name, testFunction)
	}
}

func TestGetContainerLength(t *testing.T) {
	c := &Controller{}

	tests := []struct {
		name      string
		container interface{}
		expected  int
	}{
		{
			name:      "Non empty int64 slice",
			container: &[]int64{100, 0, 10, 1, 33},
			expected:  5,
		},
		{
			name:      "Empty int64 slice",
			container: &[]int64{},
			expected:  0,
		},
		{
			name:      "Non empty string slice",
			container: &[]string{"test", "123"},
			expected:  2,
		},
		{
			name:      "Empty string slice",
			container: &[]string{},
			expected:  0,
		},
		{
			name:      "Non empty user slice",
			container: &[]user{{Name: "Alice"}, {Name: "Bob"}},
			expected:  2,
		},
		{
			name:      "Empty user slice",
			container: &[]user{},
			expected:  0,
		},
		{
			name:      "Non empty publication slice",
			container: &[]publication{{Name: "upgrade"}},
			expected:  1,
		},
		{
			name:      "Empty publication slice",
			container: &[]publication{},
			expected:  0,
		},
		{
			name:      "Non empty replicationSlot slice",
			container: &[]replicationSlot{{Name: "upgrade"}, {Name: "backup"}},
			expected:  2,
		},
		{
			name:      "Empty replicationSlot slice",
			container: &[]replicationSlot{},
			expected:  0,
		},
		{
			name:      "Non empty tablePrivilege slice",
			container: &[]tablePrivilege{{Name: "test_table"}},
			expected:  1,
		},
		{
			name:      "Empty tablePrivilege slice",
			container: &[]tablePrivilege{},
			expected:  0,
		},
		{
			name:      "Non empty table slice",
			container: &[]table{{Name: "abc"}, {Name: "xyz"}},
			expected:  2,
		},
		{
			name:      "Empty table slice",
			container: &[]table{},
			expected:  0,
		},
		{
			name:      "Unmatched non-empty container",
			container: &[]float64{1.10, 2.0, 3},
			expected:  0,
		},
		{
			name:      "Unmatched empty container",
			container: &[]float64{},
			expected:  0,
		},
	}

	for _, test := range tests {
		testFunction := func(t *testing.T) {
			received := c.getContainerLength(test.container)
			assert.Equal(t, test.expected, received, "received container length must match expected container length")
		}
		t.Run(test.name, testFunction)
	}
}

func TestReadTransaction(t *testing.T) {
	c, mock := setupDatabaseMockData()

	tests := []struct {
		name            string
		statement       string
		args            []interface{}
		inputContainer  interface{}
		outputContainer interface{}
		expectedError   error
		exists          bool
		columns         []string
		rows            [][]driver.Value
	}{
		{
			name:            "non-empty int64 container",
			statement:       "SELECT %s FROM events;",
			args:            []interface{}{"id"},
			inputContainer:  &[]int64{},
			outputContainer: &[]int64{1, 10, 20, 100},
			expectedError:   nil,
			exists:          true,
			columns:         []string{"id"},
			rows:            [][]driver.Value{{1}, {10}, {20}, {100}},
		},
		{
			name:            "non-empty string container",
			statement:       "SELECT %s FROM %s;",
			args:            []interface{}{"word", "vocabulary"},
			inputContainer:  &[]string{},
			outputContainer: &[]string{"golang", "for", "the", "win"},
			expectedError:   nil,
			exists:          true,
			columns:         []string{"word"},
			rows:            [][]driver.Value{{"golang"}, {"for"}, {"the"}, {"win"}},
		},
		{
			name:            "empty string container",
			statement:       "SELECT %s FROM %s;",
			args:            []interface{}{"item", "storage"},
			inputContainer:  &[]string{},
			outputContainer: &[]string{},
			expectedError:   nil,
			exists:          false,
			columns:         []string{"item"},
			rows:            [][]driver.Value{},
		},
	}

	for _, test := range tests {
		testFunction := func(t *testing.T) {
			query := c.buildQuery(&test.statement, test.args...)
			rows := sqlmock.NewRows(test.columns).AddRows(test.rows...)
			(*mock).ExpectQuery(*query).WillReturnRows(rows).WillReturnError(test.expectedError)

			exists, err := c.readTransaction(test.inputContainer, c.srcDatabaseConnection, &test.statement, test.args...)
			assert.NoError(t, err, "no error must be raised")

			assert.Equal(t, test.exists, exists, "expectations around object existence must match")

			assert.Equal(t, true, thelper.CompareInterfaces(test.inputContainer, test.outputContainer), "query results must match")

			if err := (*mock).ExpectationsWereMet(); err != nil {
				assert.NoError(t, err, "expectation must be fulfilled")
			}
		}
		t.Run(test.name, testFunction)
	}
}
