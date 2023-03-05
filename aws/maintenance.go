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
	"fmt"
	"strings"
	"time"

	"db_relocate/log"

	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

const (
	SERVICE_WINDOW_LOW_THRESHOLD  int = 2  // hours
	SERVICE_WINDOW_HIGH_THRESHOLD int = 10 // minutes
)

func (c *Controller) isInServiceWindow(now *time.Time, serviceWindowStart *time.Time, serviceWindowEnd *time.Time) bool {
	if (now.Equal(*serviceWindowStart) || now.After(*serviceWindowStart)) && (now.Equal(*serviceWindowEnd) || now.Before(*serviceWindowEnd)) {
		log.Errorf("Current time %s is within the service window time range!", now.String())
		return true
	}

	// Check if the current time is before the 2 hours of service window start.
	if now.Before(*serviceWindowStart) && int(serviceWindowStart.Sub(*now).Hours()) < SERVICE_WINDOW_LOW_THRESHOLD {
		log.Errorf("Current time %s is too close to a service window start: %s!", now.String(), serviceWindowStart.String())
		return true
	}

	// Check if the current time is less than 10 minutes since the service window ends.
	if now.After(*serviceWindowEnd) && int(now.Sub(*serviceWindowEnd).Minutes()) < SERVICE_WINDOW_HIGH_THRESHOLD {
		log.Errorf("Current time %s is too close to a service window end: %s!", now.String(), serviceWindowEnd.String())
		return true
	}

	return false
}

func (c *Controller) IsDBInstanceInBackupWindow(instance *rdsTypes.DBInstance, now *time.Time) (bool, error) {
	log.Debugf("Check if the process will be affected by a backup window: '%s'", *instance.PreferredBackupWindow)
	backupWindowRange := strings.Split(*instance.PreferredBackupWindow, "-")

	backupWindowStartTimeString := fmt.Sprintf(
		"%v-%02d-%02dT%s:00Z",
		now.Year(),
		int(now.Month()),
		now.Day(),
		backupWindowRange[0],
	)
	backupWindowStart, err := time.Parse(time.RFC3339, backupWindowStartTimeString)
	if err != nil {
		return false, err
	}

	backupWindowEndTimeString := fmt.Sprintf(
		"%v-%02d-%02dT%s:00Z",
		now.Year(),
		int(now.Month()),
		now.Day(),
		backupWindowRange[1],
	)
	backupWindowEnd, err := time.Parse(time.RFC3339, backupWindowEndTimeString)
	if err != nil {
		return false, err
	}
	inBackupWindow := c.isInServiceWindow(now, &backupWindowStart, &backupWindowEnd)
	if inBackupWindow {
		return true, nil
	}

	return false, nil
}

func (c *Controller) getMaintenanceWindowWeekday(weekday *string) string {
	// TODO: optimize this
	switch *weekday {
	case "mon":
		return "Monday"
	case "tue":
		return "Tuesday"
	case "wed":
		return "Wednesday"
	case "thu":
		return "Thursday"
	case "fri":
		return "Friday"
	case "sat":
		return "Saturday"
	case "sun":
		return "Sunday"
	default:
		return "Invalid input"
	}
}

func (c *Controller) IsDBInstanceInMaintenanceWindow(instance *rdsTypes.DBInstance, now *time.Time) (bool, error) {
	log.Debugf("Check if the process will be affected by a maintenance window: '%s'", *instance.PreferredMaintenanceWindow)

	// One day a week, e.g: thu:4:00-thu:4:45
	maintenanceWindowRange := strings.Split(*instance.PreferredMaintenanceWindow, "-")
	maintenanceWindowStart := strings.Split(maintenanceWindowRange[0], ":")
	maintenanceWindowEnd := strings.Split(maintenanceWindowRange[1], ":")

	// The idea here is to calculate maintenance window start and end time.
	//
	// Because AWS returns it in a weird format we also need to compare current weekday to maintenance window weekdays.
	// If current time is close to the maintenance window or is inside maintenance window we should fail the pre-flight check!
	maintenanceWindowStartTimeString := fmt.Sprintf(
		"%v-%02d-%02dT%s:%s:00Z",
		now.Year(),
		int(now.Month()),
		now.Day(),
		maintenanceWindowStart[1],
		maintenanceWindowStart[2],
	)
	maintenanceWindowStartTime, err := time.Parse(time.RFC3339, maintenanceWindowStartTimeString)
	if err != nil {
		return false, err
	}
	maintenanceWindowStartWeekday := c.getMaintenanceWindowWeekday(&maintenanceWindowStart[0])

	maintenanceWindowEndTimeString := fmt.Sprintf(
		"%v-%02d-%02dT%s:%s:00Z",
		now.Year(),
		int(now.Month()),
		now.Day(),
		maintenanceWindowEnd[1],
		maintenanceWindowEnd[2],
	)
	maintenanceWindowEndTime, err := time.Parse(time.RFC3339, maintenanceWindowEndTimeString)
	if err != nil {
		return false, err
	}
	maintenanceWindowEndWeekday := c.getMaintenanceWindowWeekday(&maintenanceWindowEnd[0])

	// This covers an edge case when maintenance weekdays are the same as our current weekday.
	if now.Weekday().String() == maintenanceWindowStartWeekday || now.Weekday().String() == maintenanceWindowEndWeekday {
		inMaintenanceWindow := c.isInServiceWindow(now, &maintenanceWindowStartTime, &maintenanceWindowEndTime)
		if inMaintenanceWindow {
			return true, nil
		}
	}

	// Adding 24 hours so we can cover an edge case when current time is too close to the end of the day, e.g: Mon 23:50
	// And the next maintenance window is too close to the beginning of the next day, e.g: Tue 00:10.
	maintenanceWindowStartTime = maintenanceWindowStartTime.Add(time.Hour * 24)
	maintenanceWindowEndTime = maintenanceWindowEndTime.Add(time.Hour * 24)

	if now.Weekday().String() == maintenanceWindowStartWeekday || now.Weekday().String() == maintenanceWindowEndWeekday {
		inMaintenanceWindow := c.isInServiceWindow(now, &maintenanceWindowStartTime, &maintenanceWindowEndTime)
		if inMaintenanceWindow {
			return true, nil
		}
	}

	return false, nil
}

// TODO: add check for urgent maintenance actions which might be applied outside of preferred maintenance windows.
// See: PendingMaintenanceAction type.
