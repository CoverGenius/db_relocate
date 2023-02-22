// Copyright (C) 2023 The db_relocate authors.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation;
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.

package aws

import (
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"

	"db_relocate/log"
)

func (c *Controller) getDBSubnetGroups(subnetGroupName *string) ([]rdsTypes.DBSubnetGroup, error) {
	input := &rds.DescribeDBSubnetGroupsInput{
		DBSubnetGroupName: subnetGroupName,
	}

	paginator := rds.NewDescribeDBSubnetGroupsPaginator(c.rdsClient, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(*c.configuration.Context)
		if err != nil {
			return nil, err
		}
		return output.DBSubnetGroups, nil

	}

	return nil, nil
}

func (c *Controller) IsValidDBSubnetGroup(instance *rdsTypes.DBInstance, vpcID *string) (bool, error) {
	// If empty, the subnet group will be copied from the source instance.
	if c.configuration.Items.Upgrade.SubnetGroupName == "" {
		// We need to make sure the selected subnet group belongs to the correct VPC.
		if *instance.DBSubnetGroup.VpcId == *vpcID {
			return true, nil
		}
		return false, nil
	}

	subnetGroups, err := c.getDBSubnetGroups(&c.configuration.Items.Upgrade.SubnetGroupName)
	if err != nil {
		return false, err
	}

	if len(subnetGroups) == 0 {
		log.Errorf("Failed to find a subnet group with a name: %s", c.configuration.Items.Upgrade.SubnetGroupName)
		return false, nil
	}

	if *subnetGroups[0].VpcId == *vpcID {
		return true, nil
	}

	return false, nil
}
