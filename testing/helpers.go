// Copyright (C) 2023 The db_relocate authors.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation;
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.

package testing

import (
	"reflect"
	"strings"
)

func EscapeParanthesis(s *string) {
	*s = strings.ReplaceAll(*s, `(`, `\(`)
	*s = strings.ReplaceAll(*s, `)`, `\)`)
}

func CompareInterfaces(a interface{}, b interface{}) bool {
	return reflect.DeepEqual(a, b)
}
