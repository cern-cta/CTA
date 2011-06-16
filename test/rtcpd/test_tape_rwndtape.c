/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
int rwndtape(int tapefd,
             char *path)
{
	char func[16];

	ENTRY (rwndtape);
	RETURN (0);
}
