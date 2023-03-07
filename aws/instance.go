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

	"time"
)

const (
	DB_INSTANCE_REBOOT_TIMEOUT string = "15m"
	DB_INSTANCE_MODIFY_TIMEOUT string = "30m"
)

func (c *Controller) DescribeDBInstance(instanceID *string) ([]rdsTypes.DBInstance, error) {
	log.Debugf("Looking for an instance with ID: %s", *instanceID)

	input := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: instanceID,
	}

	result, err := c.rdsClient.DescribeDBInstances(*c.configuration.Context, input)
	if err != nil {
		return nil, err
	}

	return result.DBInstances, nil
}

func (c *Controller) rebootDBInstance(instance *rdsTypes.DBInstance) error {
	log.Debugf("Rebooting an instance with an ID: %s", *instance.DBInstanceIdentifier)

	input := &rds.RebootDBInstanceInput{
		DBInstanceIdentifier: instance.DBInstanceIdentifier,
	}
	if instance.MultiAZ {
		input.ForceFailover = a.Bool(true)
	}
	_, err := c.rdsClient.RebootDBInstance(*c.configuration.Context, input)
	if err != nil {
		return err
	}

	waitParams := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: instance.DBInstanceIdentifier,
	}
	waiter := rds.NewDBInstanceAvailableWaiter(c.rdsClient)

	duration, err := time.ParseDuration(DB_INSTANCE_REBOOT_TIMEOUT)
	if err != nil {
		return err
	}
	err = waiter.Wait(*c.configuration.Context, waitParams, duration)

	return err
}

func (c *Controller) pendingDBChanges(pendingDBChanges *rdsTypes.PendingModifiedValues) bool {
	if pendingDBChanges.AllocatedStorage != nil ||
		pendingDBChanges.BackupRetentionPeriod != nil ||
		pendingDBChanges.CACertificateIdentifier != nil ||
		pendingDBChanges.DBInstanceClass != nil ||
		pendingDBChanges.DBInstanceIdentifier != nil ||
		pendingDBChanges.DBSubnetGroupName != nil ||
		pendingDBChanges.EngineVersion != nil ||
		pendingDBChanges.IAMDatabaseAuthenticationEnabled != nil ||
		pendingDBChanges.Iops != nil ||
		pendingDBChanges.LicenseModel != nil ||
		pendingDBChanges.MasterUserPassword != nil ||
		pendingDBChanges.MultiAZ != nil ||
		pendingDBChanges.PendingCloudwatchLogsExports != nil ||
		pendingDBChanges.Port != nil ||
		len(pendingDBChanges.ProcessorFeatures) > 0 ||
		pendingDBChanges.ResumeFullAutomationModeTime != nil ||
		pendingDBChanges.StorageThroughput != nil ||
		pendingDBChanges.StorageType != nil {

		return true
	}
	return false
}

func (c *Controller) isRebootRequired(instance *rdsTypes.DBInstance) bool {
	pendingDBChanges := c.pendingDBChanges(instance.PendingModifiedValues)

	if *instance.DBParameterGroups[0].ParameterApplyStatus == "pending-reboot" || pendingDBChanges {
		return true
	}

	return false
}

func (c *Controller) getValidInstanceClasses(engineVersion *string) (map[string]bool, error) {
	validInstanceClasses := make(map[string]bool)

	input := &rds.DescribeOrderableDBInstanceOptionsInput{
		EngineVersion: engineVersion,
	}

	paginator := rds.NewDescribeOrderableDBInstanceOptionsPaginator(c.rdsClient, input)
	for paginator.HasMorePages() {
		orderableDBInstanceOptions, err := paginator.NextPage(*c.configuration.Context)
		if err != nil {
			return nil, err
		}

		for idx := range orderableDBInstanceOptions.OrderableDBInstanceOptions {
			validInstanceClasses[*orderableDBInstanceOptions.OrderableDBInstanceOptions[idx].DBInstanceClass] = true
		}
	}

	return validInstanceClasses, nil
}

func (c *Controller) IsValidInstanceClass(engineVersion *string) (bool, error) {
	// If empty, the instance class will be copied from the source instance.
	if c.configuration.Items.Upgrade.InstanceClass == "" {
		return true, nil
	}

	validInstanceClasses, err := c.getValidInstanceClasses(engineVersion)
	if err != nil {
		return false, err
	}

	if _, instanceClass := validInstanceClasses[c.configuration.Items.Upgrade.InstanceClass]; instanceClass {
		return true, nil
	}

	log.Errorf(
		"Failed to validate the provided instance class: '%s'. Available instance classes: %v",
		c.configuration.Items.Upgrade.InstanceClass,
		validInstanceClasses,
	)

	return false, nil
}

func (c *Controller) setCAIdentifier(instance *rdsTypes.DBInstance, caIdentifier *string) error {
	log.Infof("Modifying an instance to use a new CA identifier: '%s'", *caIdentifier)
	instanceInput := &rds.ModifyDBInstanceInput{
		DBInstanceIdentifier:    instance.DBInstanceIdentifier,
		CACertificateIdentifier: caIdentifier,
		ApplyImmediately:        true,
	}
	_, err := c.rdsClient.ModifyDBInstance(*c.configuration.Context, instanceInput)
	if err != nil {
		return err
	}

	log.Infoln("Waiting for an instance to become available.")
	waitParams := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: instance.DBInstanceIdentifier,
	}
	waiter := rds.NewDBInstanceAvailableWaiter(c.rdsClient)

	duration, err := time.ParseDuration(DB_INSTANCE_MODIFY_TIMEOUT)
	if err != nil {
		return err
	}
	err = waiter.Wait(*c.configuration.Context, waitParams, duration)
	if err != nil {
		return err
	}

	err = c.rebootDBInstance(instance)

	return err
}

func (c *Controller) getValidCAIdentifiers() ([]rdsTypes.Certificate, error) {
	validCAIdentifiers := []rdsTypes.Certificate{}

	input := &rds.DescribeCertificatesInput{}

	paginator := rds.NewDescribeCertificatesPaginator(c.rdsClient, input)
	for paginator.HasMorePages() {
		caIdentifiers, err := paginator.NextPage(*c.configuration.Context)
		if err != nil {
			return nil, err
		}
		validCAIdentifiers = append(validCAIdentifiers, caIdentifiers.Certificates...)
	}

	return validCAIdentifiers, nil
}

func (c *Controller) IsValidCAIdentifier() (bool, error) {
	// If empty, the CA identifier will be copied from the source instance.
	if c.configuration.Items.Upgrade.CAIdentifier == "" {
		return true, nil
	}

	validCAIdentifiers, err := c.getValidCAIdentifiers()
	if err != nil {
		return false, err
	}

	for idx := range validCAIdentifiers {
		if c.configuration.Items.Upgrade.CAIdentifier == *validCAIdentifiers[idx].CertificateIdentifier {
			return true, nil
		}
	}

	log.Errorf(
		"Failed to validate the provided CA Identifier: '%s'!",
		c.configuration.Items.Upgrade.CAIdentifier,
	)

	return false, nil
}

func (c *Controller) getValidStorageTypes(instance *rdsTypes.DBInstance) (map[string]bool, error) {
	validStorageTypes := make(map[string]bool)

	input := &rds.DescribeValidDBInstanceModificationsInput{
		DBInstanceIdentifier: instance.DBInstanceIdentifier,
	}

	output, err := c.rdsClient.DescribeValidDBInstanceModifications(*c.configuration.Context, input)
	if err != nil {
		return nil, err
	}

	for idx := range output.ValidDBInstanceModificationsMessage.Storage {
		validStorageTypes[*output.ValidDBInstanceModificationsMessage.Storage[idx].StorageType] = true
	}

	return validStorageTypes, nil
}

func (c *Controller) IsValidStorageType(instance *rdsTypes.DBInstance) (bool, error) {
	// If empty, the storage class will be copied from the source instance.
	if c.configuration.Items.Upgrade.StorageType == "" {
		return true, nil
	}

	validStorageTypes, err := c.getValidStorageTypes(instance)
	if err != nil {
		return false, err
	}

	if _, storageType := validStorageTypes[c.configuration.Items.Upgrade.StorageType]; storageType {
		return true, nil
	}

	log.Errorf(
		"Failed to validate the provided storage type: '%s'. Available storage types: %v",
		c.configuration.Items.Upgrade.StorageType,
		validStorageTypes,
	)

	return false, nil
}

func (c *Controller) StopSrcDBInstance(instance *rdsTypes.DBInstance) error {
	input := &rds.StopDBInstanceInput{
		DBInstanceIdentifier: instance.DBInstanceIdentifier,
	}
	_, err := c.rdsClient.StopDBInstance(*c.configuration.Context, input)

	return err
}
