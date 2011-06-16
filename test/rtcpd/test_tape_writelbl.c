/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */


/*	writelbl - write one label record */
/*	return	-1 and serrno set in case of error
 *		0	if ok
 */
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
int writelbl(int tapefd,
             char *path,
             char *lblbuf)
{
	char func[16];
	ENTRY (writelbl);
	RETURN (80);
}
