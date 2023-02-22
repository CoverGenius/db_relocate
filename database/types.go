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
	"database/sql"
	"strings"
)

type user struct {
	Name                         string         `db:"name"`
	Login                        string         `db:"login"`
	BypassRowLevelSecurityPolicy string         `db:"bypass_row_level_security_policy"`
	PasswordValidUntil           sql.NullString `db:"password_valid_until"`
	MemberOf                     string         `db:"member_of"`
	memberOfList                 []string
}

func (u *user) memberOfStringToMemberOfList() {
	u.memberOfList = strings.Split(u.MemberOf, ",")
}

func (u *user) memberOf(role string) bool {
	for idx := range u.memberOfList {
		if u.memberOfList[idx] == role {
			return true
		}
	}

	return false
}

type publication struct {
	Name      string `db:"name"`
	Owner     string `db:"owner"`
	AllTables string `db:"all_tables"`
	Insert    string `db:"insert"`
	Update    string `db:"update"`
	Delete    string `db:"delete"`
}

type replicationSlot struct {
	Name     string `db:"name"`
	Plugin   string `db:"plugin"`
	Type     string `db:"type"`
	Database string `db:"database"`
	Active   string `db:"active"`
}

type tablePrivilege struct {
	Catalog   string         `db:"catalog"`
	Schema    string         `db:"schema"`
	Name      string         `db:"name"`
	Type      string         `db:"table_type"`
	Privilege sql.NullString `db:"privilege_type"`
	Grantee   sql.NullString `db:"grantee"`
}

type table struct {
	Catalog string `db:"catalog"`
	Schema  string `db:"schema"`
	Name    string `db:"name"`
	Type    string `db:"type"`
}
