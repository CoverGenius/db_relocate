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
	"fmt"
	"os"
	"strings"
)

func ProcessBinaryInput(message *string, positiveResponse *string, negativeResponse *string) (bool, error) {
	for {
		fmt.Println(*message)

		reader := bufio.NewReaderSize(os.Stdin, 4)

		response, err := reader.ReadString('\n')
		if err != nil {
			return false, err
		}

		response = strings.Replace(response, "\n", "", -1)

		if response == *positiveResponse {
			return true, nil
		}

		if response == *negativeResponse {
			return false, nil
		}
	}
}
