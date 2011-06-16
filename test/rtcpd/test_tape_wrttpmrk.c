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


int wrttpmrk(int tapefd,
             char *path,
             int n,
             int immed)
{
	char func[16];

	ENTRY (wrttpmrk);
	RETURN (0);
}
