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
	"errors"
	"fmt"

	"db_relocate/log"

	"golang.org/x/sync/errgroup"
)

const (
	SEQUENCE_INCREMENT string = "128"
)

func (c *Controller) prepareSelectSequenceStatement() (*string, error) {
	statements := []string{}

	statement := `
	SELECT
		string_agg('select ''select setval(''''' || relname || ''''', '' || last_value + %s || '');'' from ' || relname, ' union ' order by relname)
	FROM pg_catalog.pg_class
	WHERE relkind ='S';`

	exists, err := c.readTransaction(&statements, c.srcDatabaseConnection, &statement, SEQUENCE_INCREMENT)

	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, errors.New("Failed to run prepare select sequence statement!")
	}

	return &statements[0], nil
}

func (c *Controller) buildUpdatableSequenceList(statement *string) ([]string, error) {
	*statement = fmt.Sprintf("%s;", *statement)

	statements := []string{}
	_, err := c.readTransaction(&statements, c.srcDatabaseConnection, statement)

	return statements, err
}

func (c *Controller) updateSequence(statement *string) error {
	err := c.writeTransaction(c.dstDatabaseConnection, statement)

	return err
}

func (c *Controller) incrementSequenceValues() error {
	log.Infoln("Incrementing sequence values by leaving a small gap in order to avoid conflicts.")

	statement, err := c.prepareSelectSequenceStatement()
	if err != nil {
		return err
	}

	statements, err := c.buildUpdatableSequenceList(statement)
	if err != nil {
		return err
	}

	g := new(errgroup.Group)

	for idx := range statements {
		item := statements[idx]
		g.Go(func() error {
			err := c.updateSequence(&item)
			if err != nil {
				return err
			}
			return nil
		})
	}
	if err = g.Wait(); err != nil {
		return err
	}

	return nil
}
