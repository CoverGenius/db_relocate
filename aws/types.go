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
	"db_relocate/types"
	"fmt"
	"strings"

	a "github.com/aws/aws-sdk-go-v2/aws"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

type targetDBConfiguration struct {
	instanceIdentifier  *string
	subnetGroupName     *string
	vpcSecurityGroupIDs []string
	instanceClass       *string
	parameterGroupName  *string
	storageType         *string
	storageSize         *int32
	iops                *int32
	storageThroughput   *int32
}

const (
	GP3_STORAGE_TYPE                     string = "gp3"
	GP3_STORAGE_SIZE_THRESHOLD           int32  = 400 // GB
	GP3_STORAGE_THROUGHPUT_LOW_WATERMARK int32  = 500
	GP3_STORAGE_IOPS_LOW_WATERMARK       int32  = 12000
)

func (tdbc *targetDBConfiguration) setDBInstanceIdentifier(itemsConfiguration *types.Items, instance *rdsTypes.DBInstance) {
	if itemsConfiguration.Dst.InstanceID == "" {
		targetEnginePrefix := strings.Split(itemsConfiguration.Upgrade.EngineVersion, ".")[0]
		newIdentifier := fmt.Sprintf("%s-v%s", *instance.DBInstanceIdentifier, targetEnginePrefix)
		log.Infof("Desired name was not specified for the destination DB instance. Generating a new one: %s", newIdentifier)

		tdbc.instanceIdentifier = &newIdentifier
	} else {
		tdbc.instanceIdentifier = &itemsConfiguration.Dst.InstanceID
	}
}

func (tdbc *targetDBConfiguration) setDBSubnetGroup(upgradeConfiguration *types.UpgradeDetails, instance *rdsTypes.DBInstance) {
	if upgradeConfiguration.SubnetGroupName == "" {
		log.Infoln("DB subnet group was not set by the user. Using the same applied for source database.")
		tdbc.subnetGroupName = instance.DBSubnetGroup.DBSubnetGroupName
	} else {
		tdbc.subnetGroupName = &upgradeConfiguration.SubnetGroupName
	}
}

func (tdbc *targetDBConfiguration) setVPCSecurityGroupIDs(upgradeConfiguration *types.UpgradeDetails, instance *rdsTypes.DBInstance) {
	if len(upgradeConfiguration.SecurityGroupIDs) == 0 {
		log.Infoln("Security group IDs were not set by the user. Copying existing ones from the source database!")
		vpcSecurityGroupIDs := []string{}
		for idx := range instance.VpcSecurityGroups {
			vpcSecurityGroupIDs = append(vpcSecurityGroupIDs, *instance.VpcSecurityGroups[idx].VpcSecurityGroupId)
		}
		tdbc.vpcSecurityGroupIDs = vpcSecurityGroupIDs
	} else {
		tdbc.vpcSecurityGroupIDs = upgradeConfiguration.SecurityGroupIDs
	}
}

func (tdbc *targetDBConfiguration) setDBInstanceClass(upgradeConfiguration *types.UpgradeDetails, instance *rdsTypes.DBInstance) {
	if upgradeConfiguration.InstanceClass == "" {
		log.Infoln("Instance class was not specified by the user. Copying existing one from the source database!")
		tdbc.instanceClass = instance.DBInstanceClass
	} else {
		tdbc.instanceClass = &upgradeConfiguration.InstanceClass
	}
}

func (tdbc *targetDBConfiguration) setDBParameterGroupName(upgradeConfiguration *types.UpgradeDetails, instance *rdsTypes.DBInstance) {
	if upgradeConfiguration.ParameterGroup == "" {
		log.Infoln("The DB Parameter group name  was not specified by the user. Copying existing one from source database!")
		tdbc.parameterGroupName = instance.DBParameterGroups[0].DBParameterGroupName
	} else {
		tdbc.parameterGroupName = &upgradeConfiguration.ParameterGroup
	}
}

func (tdbc *targetDBConfiguration) setStorageSize(upgradeConfiguration *types.UpgradeDetails, snapshot *rdsTypes.DBSnapshot) {
	if upgradeConfiguration.StorageSize == 0 {
		log.Infoln("Storage size was not specified by the user. Copying existing one from source database snapshot!")

		tdbc.storageSize = &snapshot.AllocatedStorage
	} else {
		tdbc.storageSize = a.Int32(upgradeConfiguration.StorageSize)
	}
}

func (tdbc *targetDBConfiguration) setStorageIOPS(upgradeConfiguration *types.UpgradeDetails) {
	if *tdbc.storageType == GP3_STORAGE_TYPE {
		if *tdbc.storageSize < GP3_STORAGE_SIZE_THRESHOLD {
			tdbc.iops = nil
		} else {
			if upgradeConfiguration.StorageIOPS == 0 || upgradeConfiguration.StorageIOPS < GP3_STORAGE_IOPS_LOW_WATERMARK {
				tdbc.iops = a.Int32(GP3_STORAGE_IOPS_LOW_WATERMARK)
				return
			}
			tdbc.iops = a.Int32(upgradeConfiguration.StorageIOPS)
		}
	}
}

func (tdbc *targetDBConfiguration) setStorageThroughput(upgradeConfiguration *types.UpgradeDetails) {
	if *tdbc.storageType == GP3_STORAGE_TYPE {
		if *tdbc.storageSize < GP3_STORAGE_SIZE_THRESHOLD {
			tdbc.storageThroughput = nil
		} else {
			if upgradeConfiguration.StorageThroughput == 0 || upgradeConfiguration.StorageIOPS < GP3_STORAGE_THROUGHPUT_LOW_WATERMARK {
				tdbc.storageThroughput = a.Int32(GP3_STORAGE_THROUGHPUT_LOW_WATERMARK)
				return
			}
			tdbc.storageThroughput = a.Int32(upgradeConfiguration.StorageThroughput)
		}
	}
}

func (tdbc *targetDBConfiguration) setStorageType(upgradeConfiguration *types.UpgradeDetails, snapshot *rdsTypes.DBSnapshot) {
	if upgradeConfiguration.StorageType == "" {
		log.Infoln("Storage type was not specified by the user. Copying existing one from source database snapshot!")
		tdbc.storageType = snapshot.StorageType
	} else {
		tdbc.storageType = &upgradeConfiguration.StorageType
	}
}
