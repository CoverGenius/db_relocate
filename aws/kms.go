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
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/service/kms"

	"db_relocate/log"
	"fmt"
)

func (c *Controller) getKMSKeyByAlias(alias string) (*string, error) {
	log.Debugf("Searching for a KMS key with an alias: %s", alias)

	input := &kms.ListAliasesInput{}

	paginator := kms.NewListAliasesPaginator(c.kmsClient, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(*c.configuration.Context)
		if err != nil {
			return nil, err
		}
		for idx := range output.Aliases {
			if output.Aliases[idx].TargetKeyId != nil {
				if *output.Aliases[idx].AliasName == alias {
					aliasArn, err := arn.Parse(*output.Aliases[idx].AliasArn)
					if err != nil {
						return nil, err
					}
					aliasArn.Resource = fmt.Sprintf("key/%s", *output.Aliases[idx].TargetKeyId)
					aliasArnString := aliasArn.String()
					return &aliasArnString, nil
				}
			}
		}
	}

	return nil, nil
}

func (c *Controller) isKMSKeyExists(kmsKeyID *string) (bool, error) {
	input := &kms.ListKeysInput{}

	paginator := kms.NewListKeysPaginator(c.kmsClient, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(*c.configuration.Context)
		if err != nil {
			return false, err
		}
		for idx := range output.Keys {
			if *output.Keys[idx].KeyArn == *kmsKeyID {
				return true, nil
			}
		}
	}

	return false, nil
}

func (c *Controller) IsValidKMSKey(kmsKeyID *string) (bool, error) {
	if *kmsKeyID == "" {
		log.Warnf("No KMS key specified. Using the default one.")
		defaultKMSKeyID, err := c.getKMSKeyByAlias("alias/aws/rds")
		if err != nil {
			return false, err
		}
		if defaultKMSKeyID == nil {
			return false, nil
		}

		*kmsKeyID = *defaultKMSKeyID
		return true, nil
	}

	exists, err := c.isKMSKeyExists(kmsKeyID)
	if err != nil {
		return false, err
	}

	return exists, nil
}
