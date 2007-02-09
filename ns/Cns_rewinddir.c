/*
 * Copyright (C) 1999-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

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
