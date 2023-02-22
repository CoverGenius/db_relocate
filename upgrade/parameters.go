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
	a "github.com/aws/aws-sdk-go-v2/aws"

	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

func (c *Controller) EnsureParametersOnSrcDB(instance *rdsTypes.DBInstance) error {
	requiredParametersOnSrcDBInstance := map[string]*rdsTypes.Parameter{
		"rds.logical_replication": {
			ParameterName:  a.String("rds.logical_replication"),
			ParameterValue: a.String("1"),
			ApplyMethod:    rdsTypes.ApplyMethodPendingReboot,
		},
		"track_commit_timestamp": {
			ParameterName:  a.String("track_commit_timestamp"),
			ParameterValue: a.String("1"),
			ApplyMethod:    rdsTypes.ApplyMethodPendingReboot,
		},
	}

	err := c.awsController.EnsureParameters(instance, requiredParametersOnSrcDBInstance)
	return err
}

func (c *Controller) EnsureParametersOnDstDB(instance *rdsTypes.DBInstance) error {
	requiredParametersOnDstDBInstance := map[string]*rdsTypes.Parameter{
		"track_commit_timestamp": {
			ParameterName:  a.String("track_commit_timestamp"),
			ParameterValue: a.String("1"),
			ApplyMethod:    rdsTypes.ApplyMethodPendingReboot,
		},
	}

	err := c.awsController.EnsureParameters(instance, requiredParametersOnDstDBInstance)
	return err
}
