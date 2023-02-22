package database

import (
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestCompareSendAndReceivedHeartbeatRecords(t *testing.T) {
	c, mock := setupDatabaseMockData()

	statement := "SELECT timestamp FROM healthcheck_heartbeats;"

	tests := []struct {
		name                 string
		sendHeartBeatRecords []int64
		columns              []string
		rows                 [][]driver.Value
		expectedError        bool
	}{
		{
			name:                 "send and receive heartbeat records match",
			sendHeartBeatRecords: []int64{123, 124, 125},
			columns:              []string{"timestamp"},
			rows:                 [][]driver.Value{{123}, {124}, {125}},
			expectedError:        false,
		},
		{
			name:                 "some heartbeat records are missing",
			sendHeartBeatRecords: []int64{123, 124, 125},
			columns:              []string{"timestamp"},
			rows:                 [][]driver.Value{{123}, {125}},
			expectedError:        true,
		},
		{
			name:                 "some heartbeat records are in incorrect order",
			sendHeartBeatRecords: []int64{123, 124, 125},
			columns:              []string{"timestamp"},
			rows:                 [][]driver.Value{{123}, {125}, {124}},
			expectedError:        true,
		},
	}

	for _, test := range tests {
		testFunction := func(t *testing.T) {
			query := c.buildQuery(&statement)
			rows := sqlmock.NewRows(test.columns).AddRows(test.rows...)
			(*mock).ExpectQuery(*query).WillReturnRows(rows).WillReturnError(nil)

			err := c.CompareSendAndReceivedHeartbeatRecords(test.sendHeartBeatRecords)
			if test.expectedError {
				assert.Errorf(t, err, "error must be raised")
			} else {
				assert.NoError(t, err, "no error must be raised")
			}

			if err := (*mock).ExpectationsWereMet(); err != nil {
				assert.NoError(t, err, "expectation must be fulfilled")
			}
		}
		t.Run(test.name, testFunction)
	}
}
