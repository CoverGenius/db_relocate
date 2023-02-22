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
	thelper "db_relocate/testing"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemberOfStringToMemberOfList(t *testing.T) {
	tests := []struct {
		name     string
		memberOf string
		expected []string
	}{
		{
			name:     "user has multiple roles",
			memberOf: "rds_superuser,rds_replication",
			expected: []string{"rds_superuser", "rds_replication"},
		},
		{
			name:     "user has single role",
			memberOf: "rds_superuser",
			expected: []string{"rds_superuser"},
		},
	}

	for _, test := range tests {
		testFunction := func(t *testing.T) {
			u := user{MemberOf: test.memberOf}
			u.memberOfStringToMemberOfList()
			assert.Equal(t, true, thelper.CompareInterfaces(u.memberOfList, test.expected), "role member list must match")

		}
		t.Run(test.name, testFunction)
	}
}

func TestMemberOf(t *testing.T) {
	tests := []struct {
		name         string
		memberOfList []string
		role         string
		expected     bool
	}{
		{
			name:         "user role found",
			memberOfList: []string{"rds_replication", "rds_superuser"},
			role:         "rds_superuser",
			expected:     true,
		},
		{
			name:         "user role not found",
			memberOfList: []string{"rds_superuser"},
			role:         "rds_replication",
			expected:     false,
		},
	}

	for _, test := range tests {
		testFunction := func(t *testing.T) {
			u := user{memberOfList: test.memberOfList}
			assert.Equal(t, test.expected, u.memberOf(test.role), "user role presence must match expectations")
		}
		t.Run(test.name, testFunction)
	}
}
