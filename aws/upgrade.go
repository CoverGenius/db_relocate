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
	"strconv"
)

// Find all available upgrade targets.
// WARNING: The value returned is not accurate.
// Because AWS API does not return all possible upgrade paths which include intermediary engine versions.
//
// TODO: think about having a hard-coded hashmap of all available upgrade paths.
// There will be a problem keeping it up to date.
func (c *Controller) getValidUpgradeTargets(instance *rdsTypes.DBInstance) (map[string]bool, error) {
	validUpgradeTargets := make(map[string]bool)

	input := &rds.DescribeDBEngineVersionsInput{
		Engine:        instance.Engine,
		EngineVersion: instance.EngineVersion,
		DefaultOnly:   false,
	}

	paginator := rds.NewDescribeDBEngineVersionsPaginator(c.rdsClient, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(*c.configuration.Context)
		if err != nil {
			return nil, err
		}
		for idx1 := range output.DBEngineVersions {
			for idx2 := range output.DBEngineVersions[idx1].ValidUpgradeTarget {
				validUpgradeTargets[*output.DBEngineVersions[idx1].ValidUpgradeTarget[idx2].EngineVersion] = true
			}
		}
	}

	return validUpgradeTargets, nil
}

func (c *Controller) IsValidUpgradeTarget(instance *rdsTypes.DBInstance) (bool, error) {
	desiredEngineVersionFloat, err := strconv.ParseFloat(c.configuration.Items.Upgrade.EngineVersion, 2)
	if err != nil {
		return false, err
	}

	currentEngineVersionFloat, err := strconv.ParseFloat(*instance.EngineVersion, 2)
	if err != nil {
		return false, err
	}

	if desiredEngineVersionFloat <= currentEngineVersionFloat {
		log.Errorf(
			"Desired version: %f cannot be smaller or equal to the current version: %f\n",
			desiredEngineVersionFloat,
			currentEngineVersionFloat,
		)
		return false, nil
	}

	validUpgradeTargets, err := c.getValidUpgradeTargets(instance)
	if err != nil {
		return false, err
	}

	if _, engineVersion := validUpgradeTargets[c.configuration.Items.Upgrade.EngineVersion]; engineVersion {
		return true, nil
	}

	log.Errorf(
		"Failed to validate selected upgrade target: %s. Available upgrade versions: %v",
		c.configuration.Items.Upgrade.EngineVersion,
		validUpgradeTargets,
	)

	return false, nil
}
