/*
 * Copyright (C) 1999-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cns_rewinddir.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:20 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	Cns_rewinddir - reset the position to the beginning of the directory */

#include <sys/types.h>
#include "Cns_api.h"

void DLL_DECL
Cns_rewinddir(Cns_DIR *dirp)
{
	if (! dirp)
		return;
	dirp->bod = 1;
	dirp->eod = 0;
	dirp->dd_loc = 0;
	dirp->dd_size = 0;
	return;
}
