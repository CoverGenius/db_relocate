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
	a "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"

	"db_relocate/log"

	"fmt"
	"strings"
)

const (
	DEFAULT_DB_PARAMETER_GROUP_NAME string = "default.postgres"
)

func (c *Controller) getCurrentDBParameters(parameterGroupName *string) (map[string]*rdsTypes.Parameter, error) {
	parameters := make(map[string]*rdsTypes.Parameter)

	input := &rds.DescribeDBParametersInput{
		DBParameterGroupName: parameterGroupName,
		MaxRecords:           a.Int32(100),
	}

	paginator := rds.NewDescribeDBParametersPaginator(c.rdsClient, input)
	for paginator.HasMorePages() {
		parameterGroup, err := paginator.NextPage(*c.configuration.Context)
		if err != nil {
			return nil, err
		}
		for idx := range parameterGroup.Parameters {
			if parameterGroup.Parameters[idx].IsModifiable {
				if _, ok := parameters[*parameterGroup.Parameters[idx].ParameterName]; !ok {
					if parameterGroup.Parameters[idx].ParameterValue != nil {
						parameters[*parameterGroup.Parameters[idx].ParameterName] = &parameterGroup.Parameters[idx]
					} else {
						parameters[*parameterGroup.Parameters[idx].ParameterName] = &rdsTypes.Parameter{ParameterValue: a.String("nil")}
					}
				}
			}
		}
	}

	return parameters, nil
}

func (c *Controller) diffDBParameters(existingParameters map[string]*rdsTypes.Parameter, desiredParameters map[string]*rdsTypes.Parameter) map[string]*rdsTypes.Parameter {
	diffParameters := make(map[string]*rdsTypes.Parameter)

	for key, value := range desiredParameters {
		if v, ok := existingParameters[key]; ok {
			if *v.ParameterValue != *value.ParameterValue {
				diffParameters[key] = value
				log.Infof("Found diff parameter with key: %s. Default value: %s. Desired value: %s", key, *v.ParameterValue, *value.ParameterValue)
			}
		}
	}

	return diffParameters
}

func (c *Controller) addDBParameters(parameterGroupName *string, dbParameters map[string]*rdsTypes.Parameter) error {
	parameters := []rdsTypes.Parameter{}
	for _, value := range dbParameters {
		parameters = append(
			parameters, rdsTypes.Parameter{
				ParameterName:  value.ParameterName,
				ParameterValue: value.ParameterValue,
				ApplyMethod:    value.ApplyMethod,
			},
		)
	}
	input := &rds.ModifyDBParameterGroupInput{
		DBParameterGroupName: parameterGroupName,
		Parameters:           parameters,
	}

	_, err := c.rdsClient.ModifyDBParameterGroup(*c.configuration.Context, input)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) isDefaultDBParameterGroup(parameterGroup *rdsTypes.DBParameterGroup) bool {
	if strings.Contains(*parameterGroup.DBParameterGroupName, DEFAULT_DB_PARAMETER_GROUP_NAME) {
		return false
	}

	return true
}

func (c *Controller) getDBParameterGroup(parameterGroupName *string) ([]rdsTypes.DBParameterGroup, error) {
	input := &rds.DescribeDBParameterGroupsInput{
		DBParameterGroupName: parameterGroupName,
	}

	output, err := c.rdsClient.DescribeDBParameterGroups(*c.configuration.Context, input)
	if err != nil {
		return nil, err
	}

	return output.DBParameterGroups, nil
}

func (c *Controller) engineVersionToGroupFamily(engineVersion *string) *string {
	majorVersion := strings.Split(*engineVersion, ".")[0]
	groupFamily := fmt.Sprintf("postgres%s", majorVersion)

	return &groupFamily
}

func (c *Controller) IsValidDBParameterGroup(parameterGroupName *string, engineVersion *string) (bool, error) {
	parameterGroups, err := c.getDBParameterGroup(parameterGroupName)
	if err != nil {
		return false, err
	}

	if len(parameterGroups) == 0 {
		log.Errorf("Failed to find a parameter group with a name: %s", *parameterGroupName)
		return false, nil
	}

	// Check if the default parameter group is used.
	if !c.isDefaultDBParameterGroup(&parameterGroups[0]) {
		return false, nil
	}

	groupFamily := c.engineVersionToGroupFamily(engineVersion)
	if *groupFamily != *parameterGroups[0].DBParameterGroupFamily {
		log.Errorf(
			"Found specified parameter group: '%s',"+
				"but it belongs to an incorrect group family '%s',"+
				"while it should belong to: '%s'!",
			*parameterGroupName,
			*parameterGroups[0].DBParameterGroupFamily,
			*groupFamily,
		)
		return false, nil
	}

	return true, nil
}

func (c *Controller) EnsureParameters(instance *rdsTypes.DBInstance, desiredParameters map[string]*rdsTypes.Parameter) error {
	rebootRequired := c.isRebootRequired(instance)

	existingParameters, err := c.getCurrentDBParameters(instance.DBParameterGroups[0].DBParameterGroupName)
	if err != nil {
		return err
	}

	parametersNeeded := c.diffDBParameters(existingParameters, desiredParameters)
	if len(parametersNeeded) > 0 {
		log.Debugln("Applying DB parameters!")
		err = c.addDBParameters(instance.DBParameterGroups[0].DBParameterGroupName, parametersNeeded)
		if err != nil {
			return err
		}
		rebootRequired = true
	}

	if rebootRequired {
		err = c.rebootDBInstance(instance)
		if err != nil {
			return err
		}
	}

	return nil
}
