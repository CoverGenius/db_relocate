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
	"time"
)

const (
	SNAPSHOT_IDENTIFIER_SUFFIX string = "upgrade"
	SNAPSHOT_CREATE_TIMEOUT    string = "60m"
	SNAPSHOT_UPGRADE_TIMEOUT   string = "60m"
	SNAPSHOT_COPY_TIMEOUT      string = "60m"
	SNAPSHOT_RESTORE_TIMEOUT   string = "60m"
	SNAPSHOT_ENCRYPTED_SUFFIX  string = "-encrypted"
)

func (c *Controller) takeDBInstanceSnapshot(instance *rdsTypes.DBInstance) (*rdsTypes.DBSnapshot, error) {
	log.Infoln("Taking a snapshot!")
	encryptionStatusSuffix := ""

	if instance.StorageEncrypted {
		encryptionStatusSuffix = SNAPSHOT_ENCRYPTED_SUFFIX
	}

	snapshotName := fmt.Sprintf("%s-%s%s", *instance.DBInstanceIdentifier, SNAPSHOT_IDENTIFIER_SUFFIX, encryptionStatusSuffix)

	snapshotInput := &rds.CreateDBSnapshotInput{
		DBInstanceIdentifier: instance.DBInstanceIdentifier,
		DBSnapshotIdentifier: a.String(snapshotName),
	}
	snapshot, err := c.rdsClient.CreateDBSnapshot(*c.configuration.Context, snapshotInput)
	if err != nil {
		return nil, err
	}

	log.Infoln("Waiting for a snapshot to become available.")
	waiter := rds.NewDBSnapshotAvailableWaiter(c.rdsClient)
	waiterParams := &rds.DescribeDBSnapshotsInput{
		DBSnapshotIdentifier: snapshot.DBSnapshot.DBSnapshotIdentifier,
	}

	duration, err := time.ParseDuration(SNAPSHOT_CREATE_TIMEOUT)
	if err != nil {
		return nil, err
	}

	err = waiter.Wait(*c.configuration.Context, waiterParams, duration)
	if err != nil {
		return nil, err
	}

	return snapshot.DBSnapshot, nil
}

func (c *Controller) upgradeDBSnapshot(snapshot *rdsTypes.DBSnapshot, engineVersion *string) (*rdsTypes.DBSnapshot, error) {
	log.Infof("Upgrading snapshot to a new database engine: '%s'", *engineVersion)
	snapshotInput := &rds.ModifyDBSnapshotInput{
		DBSnapshotIdentifier: snapshot.DBSnapshotIdentifier,
		EngineVersion:        engineVersion,
	}
	output, err := c.rdsClient.ModifyDBSnapshot(*c.configuration.Context, snapshotInput)
	if err != nil {
		return nil, err
	}

	log.Infoln("Waiting for a snapshot to become available.")
	waiter := rds.NewDBSnapshotAvailableWaiter(c.rdsClient)
	waiterParams := &rds.DescribeDBSnapshotsInput{
		DBSnapshotIdentifier: output.DBSnapshot.DBSnapshotIdentifier,
	}

	duration, err := time.ParseDuration(SNAPSHOT_UPGRADE_TIMEOUT)
	if err != nil {
		return nil, err
	}

	err = waiter.Wait(*c.configuration.Context, waiterParams, duration)
	if err != nil {
		return nil, err
	}

	return output.DBSnapshot, nil

}

func (c *Controller) copyDBSnapshot(snapshot *rdsTypes.DBSnapshot, engineVersion *string, kmsKeyID *string) (*rdsTypes.DBSnapshot, error) {
	log.Infof("Encrypting a snapshot: '%s' with a KMS key: '%s'", *snapshot.DBSnapshotIdentifier, *kmsKeyID)

	snapshotName := fmt.Sprintf("%s%s", *snapshot.DBSnapshotIdentifier, SNAPSHOT_ENCRYPTED_SUFFIX)

	snapshotInput := &rds.CopyDBSnapshotInput{
		SourceDBSnapshotIdentifier: snapshot.DBSnapshotIdentifier,
		TargetDBSnapshotIdentifier: &snapshotName,
		CopyTags:                   a.Bool(true),
		KmsKeyId:                   kmsKeyID,
	}
	output, err := c.rdsClient.CopyDBSnapshot(*c.configuration.Context, snapshotInput)
	if err != nil {
		return nil, err
	}

	log.Infoln("Waiting for a snapshot to become available.")
	waiter := rds.NewDBSnapshotAvailableWaiter(c.rdsClient)
	waiterParams := &rds.DescribeDBSnapshotsInput{
		DBSnapshotIdentifier: output.DBSnapshot.DBSnapshotIdentifier,
	}

	duration, err := time.ParseDuration(SNAPSHOT_COPY_TIMEOUT)
	if err != nil {
		return nil, err
	}

	err = waiter.Wait(*c.configuration.Context, waiterParams, duration)
	if err != nil {
		return nil, err
	}

	return output.DBSnapshot, nil

}

func (c *Controller) sanitizeTargetDBInstanceConfiguration(instance *rdsTypes.DBInstance) *targetDBConfiguration {
	configuration := targetDBConfiguration{}

	configuration.setDBInstanceIdentifier(c.configuration.Items, instance)

	configuration.setDBSubnetGroup(c.configuration.Items.Upgrade, instance)

	configuration.setVPCSecurityGroupIDs(c.configuration.Items.Upgrade, instance)

	configuration.setDBInstanceClass(c.configuration.Items.Upgrade, instance)

	configuration.setDBParameterGroupName(c.configuration.Items.Upgrade, instance)

	configuration.setStorageType(c.configuration.Items.Upgrade, instance)

	return &configuration
}

func (c *Controller) restoreDBSnapshot(snapshot *rdsTypes.DBSnapshot, instance *rdsTypes.DBInstance) (*rdsTypes.DBInstance, error) {
	log.Infoln("Restoring a snapshot!")
	configuration := c.sanitizeTargetDBInstanceConfiguration(instance)

	input := &rds.RestoreDBInstanceFromDBSnapshotInput{
		DBInstanceIdentifier:            configuration.instanceIdentifier,
		AllocatedStorage:                &snapshot.AllocatedStorage,
		AutoMinorVersionUpgrade:         &instance.AutoMinorVersionUpgrade,
		CopyTagsToSnapshot:              &instance.CopyTagsToSnapshot,
		DBInstanceClass:                 configuration.instanceClass,
		DBParameterGroupName:            configuration.parameterGroupName,
		DBSnapshotIdentifier:            snapshot.DBSnapshotIdentifier,
		DBSubnetGroupName:               configuration.subnetGroupName,
		DeletionProtection:              &instance.DeletionProtection,
		EnableIAMDatabaseAuthentication: &instance.IAMDatabaseAuthenticationEnabled,
		Iops:                            configuration.iops,
		MultiAZ:                         &instance.MultiAZ,
		StorageThroughput:               configuration.storageThroughput,
		StorageType:                     configuration.storageType,
		Tags:                            snapshot.TagList,
		VpcSecurityGroupIds:             configuration.vpcSecurityGroupIDs,
	}
	_, err := c.rdsClient.RestoreDBInstanceFromDBSnapshot(*c.configuration.Context, input)
	if err != nil {
		return nil, err
	}

	log.Infoln("Waiting for an instance to become available.")
	waitParams := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: configuration.instanceIdentifier,
	}
	waiter := rds.NewDBInstanceAvailableWaiter(c.rdsClient)

	duration, err := time.ParseDuration(SNAPSHOT_RESTORE_TIMEOUT)
	if err != nil {
		return nil, err
	}
	output, err := waiter.WaitForOutput(*c.configuration.Context, waitParams, duration)
	if err != nil {
		return nil, err
	}

	return &output.DBInstances[0], nil
}

func (c *Controller) RunDBSnapshotMaintenance(instance *rdsTypes.DBInstance) (*rdsTypes.DBInstance, error) {
	snapshot, err := c.takeDBInstanceSnapshot(instance)
	if err != nil {
		return nil, err
	}

	if !strings.HasSuffix(*snapshot.DBSnapshotIdentifier, SNAPSHOT_ENCRYPTED_SUFFIX) {
		snapshot, err = c.copyDBSnapshot(
			snapshot,
			&c.configuration.Items.Upgrade.EngineVersion,
			&c.configuration.Items.Upgrade.KMSID,
		)
		if err != nil {
			return nil, err
		}
	}

	snapshot, err = c.upgradeDBSnapshot(snapshot, &c.configuration.Items.Upgrade.EngineVersion)
	if err != nil {
		return nil, err
	}

	newInstance, err := c.restoreDBSnapshot(snapshot, instance)
	if err != nil {
		return nil, err
	}

	return newInstance, nil
}
