// Copyright (C) 2023 The db_relocate authors.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation;
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.

package upgrade

import (
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

func (c *Controller) performPostUpgradeOperations(instance *rdsTypes.DBInstance) error {
	err := c.performCleanup(instance)
	if err != nil {
		return err
	}

	err = c.databaseController.PerformPostUpgradeOperations()
	if err != nil {
		return err
	}

	return nil
}
