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
	"errors"
	"fmt"
	"regexp"
	"time"
)

const (
	UNHEALTHY_WAL_LOG_RECORD_REGEXP = `(?P<date>[\d\-]+)\s+(?P<time>[0-9:]+)\s+\S+\s+invalid\s+record\s+length\s+at\s+(?P<position>[\d\w\/]+)`
)

func (c *Controller) searchLogFileForLatestUnhealthyLSN(
	instance *rdsTypes.DBInstance,
	logFileName *string,
	timeBeforeSnapshot *time.Time,
	timeAfterRestore *time.Time,
) (*string, error) {
	regexpObject, err := regexp.Compile(UNHEALTHY_WAL_LOG_RECORD_REGEXP)
	if err != nil {
		return nil, err
	}

	input := &rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: instance.DBInstanceIdentifier,
		LogFileName:          logFileName,
	}
	paginator := rds.NewDownloadDBLogFilePortionPaginator(c.rdsClient, input, func(opts *rds.DownloadDBLogFilePortionPaginatorOptions) {
		opts.StopOnDuplicateToken = true
	})

	// TODO: use time.DateTime constant from go 1.20+ instead.
	timeLayout := "2006-01-02 15:04:05"

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(*c.configuration.Context)
		if err != nil {
			return nil, err
		}

		searchResult := regexpObject.FindStringSubmatch(*output.LogFileData)
		if len(searchResult) > 0 {
			timeString := fmt.Sprintf("%s %s", searchResult[1], searchResult[2])
			logFileEntryRelativeTime, err := time.Parse(timeLayout, timeString)
			if err != nil {
				return nil, err
			}
			if logFileEntryRelativeTime.UTC().After(*timeBeforeSnapshot) && logFileEntryRelativeTime.UTC().Before(*timeAfterRestore) {
				return &searchResult[3], nil
			} else {
				continue
			}

		}
	}

	return nil, nil
}

func (c *Controller) SearchLogFilesForEarliestUnhealthyLSN(instance *rdsTypes.DBInstance, timeBeforeSnapshot *time.Time, timeAfterRestore *time.Time) (*string, error) {
	log.Debugf(
		"Looking for the earliest unhealthy LSN within the time range: [%s - %s]",
		timeBeforeSnapshot.String(),
		timeAfterRestore.String(),
	)

	input := &rds.DescribeDBLogFilesInput{
		DBInstanceIdentifier: instance.DBInstanceIdentifier,
		FileLastWritten:      timeBeforeSnapshot.UnixMilli(),
	}

	paginator := rds.NewDescribeDBLogFilesPaginator(c.rdsClient, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(*c.configuration.Context)
		if err != nil {
			return nil, err
		}
		for idx := range output.DescribeDBLogFiles {
			if output.DescribeDBLogFiles[idx].Size > 0 {
				positionID, err := c.searchLogFileForLatestUnhealthyLSN(
					instance,
					output.DescribeDBLogFiles[idx].LogFileName,
					timeBeforeSnapshot,
					timeAfterRestore,
				)
				if err != nil {
					return nil, err
				}
				if positionID == nil {
					continue
				}
				return positionID, nil
			}
		}
	}

	return nil, errors.New(fmt.Sprintf(
		"Failed to find a useable LSN record within the time range: [%s - %s]",
		timeBeforeSnapshot.String(),
		timeAfterRestore.String(),
	))
}
