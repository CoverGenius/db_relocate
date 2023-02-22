//  Copyright (C) 2023 The db_relocate authors.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation;
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.

package errors

import (
	"db_relocate/log"
)

const (
	ERROR_CHANNEL_CAPACITY int = 10
)

func createErrorChannel() chan error {
	errorChannel := make(chan error, ERROR_CHANNEL_CAPACITY)

	return errorChannel
}

func InitializeBackgroundErrorChecking() chan error {
	errorChannel := createErrorChannel()

	go func() {
		for {
			select {
			case err := <-errorChannel:
				log.Fatalln(err)
				return
			}
		}
	}()

	return errorChannel
}
