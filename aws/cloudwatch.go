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
	"time"

	a "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwTypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

const (
	CW_METRIC_PERIOD         int32   = 600
	DISK_SPACE_LOW_WATERMARK float64 = 10 // GB
)

func calculateAverage(items []float64) (float64, error) {
	var sum float64
	for idx := range items {
		sum += items[idx]
	}
	average := sum / float64(len(items))
	return average, nil
}

func buildDimensionsForDBInstance(instance *rdsTypes.DBInstance) []cwTypes.Dimension {
	return []cwTypes.Dimension{
		{
			Name:  a.String("DBInstanceIdentifier"),
			Value: instance.DBInstanceIdentifier,
		},
	}
}

func buildMetricDataQueryForDBInstance(instance *rdsTypes.DBInstance) []cwTypes.MetricDataQuery {
	dimensions := buildDimensionsForDBInstance(instance)

	return []cwTypes.MetricDataQuery{
		{
			Id: a.String("upgrade"),
			MetricStat: &cwTypes.MetricStat{
				Metric: &cwTypes.Metric{
					MetricName: a.String("FreeStorageSpace"),
					Namespace:  a.String("AWS/RDS"),
					Dimensions: dimensions,
				},
				Stat:   a.String("Average"),
				Period: a.Int32(CW_METRIC_PERIOD),
			},
			ReturnData: a.Bool(true),
		},
	}
}

func (c *Controller) getAvailableDiskSpaceForDBInstance(instance *rdsTypes.DBInstance, now *time.Time) (float64, error) {
	end := now.Round(5 * time.Minute)
	start := end.Add(-time.Duration(CW_METRIC_PERIOD) * time.Second)

	metricDataQuery := buildMetricDataQueryForDBInstance(instance)

	input := &cloudwatch.GetMetricDataInput{
		EndTime:           &end,
		StartTime:         &start,
		MetricDataQueries: metricDataQuery,
	}

	value := 0.0
	paginator := cloudwatch.NewGetMetricDataPaginator(c.cwClient, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(*c.configuration.Context)
		if err != nil {
			return 0, err
		}

		for idx := range output.MetricDataResults {
			if len(output.MetricDataResults[idx].Values) == 0 {
				continue
			}
			// TODO: optimize this one.
			value, err = calculateAverage(output.MetricDataResults[idx].Values)
			if err != nil {
				return 0, err
			}
		}
	}
	// Since we only query 1 data source, we can return straight away after the value has been received
	return value, nil
}

func (c *Controller) IsEnoughOfAvailableDiskSpaceForDBInstance(instance *rdsTypes.DBInstance, now *time.Time) (bool, error) {
	availableDiskSpace, err := c.getAvailableDiskSpaceForDBInstance(instance, now)
	if err != nil {
		return false, err
	}

	availableDiskSpaceInGB := availableDiskSpace / 1024 / 1024 / 1024
	if availableDiskSpaceInGB < DISK_SPACE_LOW_WATERMARK {
		log.Errorf(
			"Not enough disk space! Available: '%.2f. Low watermark %f",
			availableDiskSpaceInGB,
			DISK_SPACE_LOW_WATERMARK,
		)
		return false, nil
	}

	return true, nil
}
