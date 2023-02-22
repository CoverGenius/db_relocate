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
	"db_relocate/log"
	"errors"
	"time"

	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

const (
	OK_CHECK_STATUS     string = "OK"
	FAILED_CHECK_STATUS string = "FAIL"
)

type preFlightChecks struct {
	preFlightChecks map[string]bool
	passed          bool
}

func (pfc *preFlightChecks) pass() bool {
	log.Infoln("Displaying pre-flight checks status...")
	for key, value := range pfc.preFlightChecks {
		status := FAILED_CHECK_STATUS
		if value {
			status = OK_CHECK_STATUS
		}
		log.Infof("%s: [%s]", key, status)
	}

	return pfc.passed
}

func (c *Controller) initPreFlightChecks() *preFlightChecks {
	preFlightChecks := &preFlightChecks{
		preFlightChecks: make(map[string]bool),
		passed:          true,
	}

	return preFlightChecks
}

func (c *Controller) srcDatabaseInstanceExistsCheck(pfc *preFlightChecks) (*rdsTypes.DBInstance, error) {
	instance, err := c.awsController.DescribeDBInstance(&c.configuration.Items.Src.InstanceID)

	if err != nil {
		return nil, err
	}

	if len(instance) == 0 {
		pfc.preFlightChecks["SrcDatabaseExists"] = false
		pfc.passed = false

		return nil, nil
	}

	pfc.preFlightChecks["SrcDatabaseExists"] = true

	return &instance[0], nil
}

func (c *Controller) dstDatabaseInstanceExistsCheck(pfc *preFlightChecks) (*rdsTypes.DBInstance, error) {
	instance, err := c.awsController.DescribeDBInstance(&c.configuration.Items.Dst.InstanceID)
	if err != nil {
		return nil, err
	}

	if len(instance) > 0 {
		pfc.preFlightChecks["DstDatabaseExists"] = false
		pfc.passed = false

		return nil, nil
	}

	pfc.preFlightChecks["DstDatabaseExists"] = true

	return &instance[0], nil
}

func (c *Controller) validVPCCheck(pfc *preFlightChecks, instance *rdsTypes.DBInstance) error {
	ok, err := c.awsController.IsValidVPC(instance, &c.configuration.Items.Upgrade.VPCID)
	if err != nil {
		return err
	}

	if !ok {
		pfc.preFlightChecks["ValidVPC"] = false
		pfc.passed = false

		return nil
	}

	pfc.preFlightChecks["ValidVPC"] = true

	return nil
}

func (c *Controller) validSecurityGroupsCheck(pfc *preFlightChecks, instance *rdsTypes.DBInstance) error {
	ok, err := c.awsController.IsValidSecurityGroups(instance)

	if err != nil {
		return err
	}

	if !ok {
		pfc.preFlightChecks["SecurityGroups"] = false
		pfc.passed = false

		return nil
	}

	pfc.preFlightChecks["SecurityGroups"] = true

	return nil
}

func (c *Controller) validSubnetGroupCheck(pfc *preFlightChecks, instance *rdsTypes.DBInstance) error {
	ok, err := c.awsController.IsValidDBSubnetGroup(instance, &c.configuration.Items.Upgrade.VPCID)
	if err != nil {
		return err
	}

	if !ok {
		pfc.preFlightChecks["SubnetGroup"] = false
		pfc.passed = false

		return nil

	}

	pfc.preFlightChecks["SubnetGroup"] = true

	return nil
}

func (c *Controller) validParameterGroupCheck(pfc *preFlightChecks) error {
	ok, err := c.awsController.IsValidDBParameterGroup(&c.configuration.Items.Upgrade.ParameterGroup, &c.configuration.Items.Upgrade.EngineVersion)
	if err != nil {
		return err
	}

	if !ok {
		pfc.preFlightChecks["ParameterGroup"] = false
		pfc.passed = false

		return nil

	}

	pfc.preFlightChecks["ParameterGroup"] = true

	return nil
}

func (c *Controller) validUpgradeTargetCheck(pfc *preFlightChecks, instance *rdsTypes.DBInstance) error {
	ok, err := c.awsController.IsValidUpgradeTarget(instance)
	if err != nil {
		return err
	}

	if !ok {
		pfc.preFlightChecks["UpgradeTarget"] = false
		pfc.passed = false

		return nil

	}

	pfc.preFlightChecks["UpgradeTarget"] = true

	return nil
}

func (c *Controller) validStorageTypeCheck(pfc *preFlightChecks, instance *rdsTypes.DBInstance) error {
	ok, err := c.awsController.IsValidStorageType(instance)
	if err != nil {
		return err
	}

	if !ok {
		pfc.preFlightChecks["StorageType"] = false
		pfc.passed = false

		return nil

	}

	pfc.preFlightChecks["StorageType"] = true

	return nil
}

func (c *Controller) validInstanceClassCheck(pfc *preFlightChecks) error {
	ok, err := c.awsController.IsValidInstanceClass(&c.configuration.Items.Upgrade.InstanceClass)
	if err != nil {
		return err
	}

	if !ok {
		pfc.preFlightChecks["InstanceClass"] = false
		pfc.passed = false

		return nil

	}

	pfc.preFlightChecks["InstanceClass"] = true

	return nil
}

func (c *Controller) backupWindowCheck(pfc *preFlightChecks, instance *rdsTypes.DBInstance, now *time.Time) error {
	isBackupWindow, err := c.awsController.IsDBInstanceInBackupWindow(instance, now)
	if err != nil {
		return err
	}

	if isBackupWindow {
		pfc.preFlightChecks["BackupWindow"] = false
		pfc.passed = false

		return nil

	}

	pfc.preFlightChecks["BackupWindow"] = true

	return nil
}

func (c *Controller) maintenanceWindowCheck(pfc *preFlightChecks, instance *rdsTypes.DBInstance, now *time.Time) error {
	isMaintenanceWindow, err := c.awsController.IsDBInstanceInMaintenanceWindow(instance, now)
	if err != nil {
		return err
	}

	if isMaintenanceWindow {
		pfc.preFlightChecks["MaintenanceWindow"] = false
		pfc.passed = false

		return nil

	}

	pfc.preFlightChecks["MaintenanceWindow"] = true

	return nil
}

func (c *Controller) availableDiskSpaceCheck(pfc *preFlightChecks, instance *rdsTypes.DBInstance, now *time.Time) error {
	ok, err := c.awsController.IsEnoughOfAvailableDiskSpaceForDBInstance(instance, now)
	if err != nil {
		return err
	}

	if !ok {
		pfc.preFlightChecks["AvailableDiskSpace"] = false
		pfc.passed = false

		return nil

	}

	pfc.preFlightChecks["AvailableDiskSpace"] = true

	return nil
}

func (c *Controller) validKMSKeyIDCheck(pfc *preFlightChecks) error {
	ok, err := c.awsController.IsValidKMSKey(&c.configuration.Items.Upgrade.KMSID)
	if err != nil {
		return err
	}

	if !ok {
		pfc.preFlightChecks["KMSKey"] = false
		pfc.passed = false

		return nil

	}

	pfc.preFlightChecks["KMSKey"] = true

	return nil
}

func (c *Controller) databaseUserCheck(pfc *preFlightChecks) error {
	// Current user MUST have superuser privilege and be an owner of the selected database.
	ok, err := c.databaseController.CurrentUserCanProceed()
	if err != nil {
		return err
	}

	if !ok {
		pfc.preFlightChecks["DatabaseUser"] = false
		pfc.passed = false

		return nil

	}

	pfc.preFlightChecks["DatabaseUser"] = true

	return nil
}

func (c *Controller) logicalReplicationSlotsCheck(pfc *preFlightChecks) error {
	ok, err := c.databaseController.LogicalReplicationSlotsExists()
	if err != nil {
		return err
	}

	if !ok {
		pfc.preFlightChecks["LogicalReplicationSlots"] = false
		pfc.passed = false

		return nil

	}

	pfc.preFlightChecks["LogicalReplicationSlots"] = true

	return nil
}

func (c *Controller) runPreFlightChecks(now *time.Time) (*rdsTypes.DBInstance, error) {
	preFlightChecks := c.initPreFlightChecks()

	srcDatabaseInstance, err := c.srcDatabaseInstanceExistsCheck(preFlightChecks)
	if err != nil {
		return nil, err
	}

	if srcDatabaseInstance == nil {
		log.Errorf(
			"Failed to perform subsequent checks because src db instance: %s does not exist.",
			c.configuration.Items.Src.InstanceID,
		)

		preFlightChecks.pass()
		return srcDatabaseInstance, errors.New("Failed to find source DB instance!")
	}

	if c.configuration.Items.Dst.InstanceID != "" {
		dstDatabaseInstance, err := c.dstDatabaseInstanceExistsCheck(preFlightChecks)
		if err != nil {
			return nil, err
		}

		if dstDatabaseInstance != nil {
			log.Errorf(
				"Failed to perform subsequent checks because dst db instance: %s already exists.",
				c.configuration.Items.Dst.InstanceID,
			)
			preFlightChecks.pass()
			return srcDatabaseInstance, errors.New("Failed to find destination DB instance!")
		}
	}

	err = c.validVPCCheck(preFlightChecks, srcDatabaseInstance)
	if err != nil {
		return nil, err
	}

	err = c.validSecurityGroupsCheck(preFlightChecks, srcDatabaseInstance)
	if err != nil {
		return nil, err
	}

	err = c.validSubnetGroupCheck(preFlightChecks, srcDatabaseInstance)
	if err != nil {
		return nil, err
	}

	err = c.validUpgradeTargetCheck(preFlightChecks, srcDatabaseInstance)
	if err != nil {
		return nil, err
	}

	err = c.validParameterGroupCheck(preFlightChecks)
	if err != nil {
		return nil, err
	}

	err = c.validStorageTypeCheck(preFlightChecks, srcDatabaseInstance)
	if err != nil {
		return nil, err
	}

	err = c.validInstanceClassCheck(preFlightChecks)
	if err != nil {
		return nil, err
	}

	err = c.backupWindowCheck(preFlightChecks, srcDatabaseInstance, now)
	if err != nil {
		return nil, err
	}

	err = c.maintenanceWindowCheck(preFlightChecks, srcDatabaseInstance, now)
	if err != nil {
		return nil, err
	}

	err = c.availableDiskSpaceCheck(preFlightChecks, srcDatabaseInstance, now)
	if err != nil {
		return nil, err
	}

	err = c.validKMSKeyIDCheck(preFlightChecks)
	if err != nil {
		return nil, err
	}

	err = c.databaseUserCheck(preFlightChecks)
	if err != nil {
		return nil, err
	}

	err = c.logicalReplicationSlotsCheck(preFlightChecks)
	if err != nil {
		return nil, err
	}

	status := preFlightChecks.pass()
	if !status {
		return nil, errors.New("Some of the pre-flight checks have failed!")
	}

	return srcDatabaseInstance, nil
}
