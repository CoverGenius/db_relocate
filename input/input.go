// Copyright (C) 2023 The db_relocate authors.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation;
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.

package input

import (
	"bufio"
	"db_relocate/log"
	"fmt"
	"os"
	"strings"
)

type BinaryInputMetadata struct {
	Message          string
	PositiveResponse string
	NegativeResponse string
	Handler          func() error
}

func (b *BinaryInputMetadata) readResponse() (*string, error) {
	reader := bufio.NewReaderSize(os.Stdin, 4)

	response, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	response = strings.Replace(response, "\n", "", -1)

	return &response, nil
}

func (b *BinaryInputMetadata) ProcessBinaryInput() (bool, error) {
	for {
		fmt.Println(b.Message)

		response, err := b.readResponse()
		if err != nil {
			return false, err
		}

		if *response == b.PositiveResponse {
			log.Infoln("Positive response received. Executing handler.")
			return true, nil
		}

		if *response == b.NegativeResponse {
			log.Infoln("Negative response received. Skipping handler.")
			return false, nil
		}

		log.Infof(
			"Response not recognized. Please select only between: '%s' or '%s'!",
			b.PositiveResponse,
			b.NegativeResponse,
		)
	}
}
