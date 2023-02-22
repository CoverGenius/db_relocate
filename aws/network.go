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
	"db_relocate/log"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2Types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

// At this point VPC is set to the correct value.
// Either specified by the user or copied from an instance.
func (c *Controller) getVPCSecurityGroupsByID(securityGroupIDs []string) ([]ec2Types.SecurityGroup, error) {
	input := &ec2.DescribeSecurityGroupsInput{
		GroupIds: securityGroupIDs,
	}

	paginator := ec2.NewDescribeSecurityGroupsPaginator(c.ec2Client, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(*c.configuration.Context)
		if err != nil {
			return nil, err
		}
		return output.SecurityGroups, nil

	}

	return nil, nil
}

func (c *Controller) IsValidSecurityGroups(instance *rdsTypes.DBInstance) (bool, error) {
	if len(c.configuration.Items.Upgrade.SecurityGroupIDs) == 0 {
		if *instance.DBSubnetGroup.VpcId != c.configuration.Items.Upgrade.VPCID {
			// If not specified, security groups will be copied from the existing instance.
			// We need to make sure they belong to the correct VPC.
			return false, nil
		}
		return true, nil
	}

	securityGroups, err := c.getVPCSecurityGroupsByID(c.configuration.Items.Upgrade.SecurityGroupIDs)
	if err != nil {
		return false, err
	}

	if len(c.configuration.Items.Upgrade.SecurityGroupIDs) != len(securityGroups) {
		log.Errorf(
			"Some of the security groups do not exist. Found: %v, wanted: %s",
			securityGroups,
			c.configuration.Items.Upgrade.SecurityGroupIDs,
		)
		return false, nil
	}

	for idx := range securityGroups {
		if *securityGroups[idx].VpcId != c.configuration.Items.Upgrade.VPCID {
			log.Errorf(
				"Security group with an ID '%s', does not belong to VPC with an ID: '%s'!",
				*securityGroups[idx].GroupId,
				c.configuration.Items.Upgrade.VPCID,
			)
			return false, nil
		}
	}

	return true, nil
}

func (c *Controller) getVPCByID(vpcID *string) ([]ec2Types.Vpc, error) {
	input := &ec2.DescribeVpcsInput{
		VpcIds: []string{*vpcID},
	}

	paginator := ec2.NewDescribeVpcsPaginator(c.ec2Client, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(*c.configuration.Context)
		if err != nil {
			return nil, err
		}
		return output.Vpcs, nil

	}

	return nil, nil
}

func (c *Controller) IsValidVPC(instance *rdsTypes.DBInstance, vpcID *string) (bool, error) {
	if *vpcID == "" {
		c.configuration.Items.Upgrade.VPCID = *instance.DBSubnetGroup.VpcId
		return true, nil
	}

	vpcIDs, err := c.getVPCByID(vpcID)
	if err != nil {
		return false, err
	}

	if len(vpcIDs) == 0 {
		log.Errorf("Failed to find a VPC with an ID: '%s'", c.configuration.Items.Upgrade.VPCID)
		return false, nil
	}

	return true, nil
}
