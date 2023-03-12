//  Copyright (C) 2023 The db_relocate authors.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License version 2 as
// published by the Free Software Foundation;
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.

package database

import "db_relocate/log"

func (c *Controller) performVacuumAndThenAnalyze() error {
	log.Infoln("Performing VACUUM and then ANALYZE in order to avoid lazy-loading related performance issues.")

	statement := `VACUUM(ANALYZE, DISABLE_PAGE_SKIPPING);`

	err := c.writeTransaction(c.srcDatabaseConnection, &statement)

	return err
}
