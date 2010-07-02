/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	checkjobdied - returns the list of jobs that have died */

#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#if defined(linux)
#include <errno.h>
#include <sys/stat.h>
#endif
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
static char func[16];

int checkjobdied(int jobs[])
{
	int i, j, k;
#if defined(linux)
	char name[12];
	struct stat st;
#endif
	ENTRY (checkjobdied);
#if defined(linux)
	for (i = 0, k = 0; jobs[i]; i++) {
		sprintf (name, "/proc/%d", jobs[i]);
		j = stat (name, &st);
		if (j < 0 && errno == ENOENT) jobs[k++] = jobs[i];	/* job has died */
	}
#endif
	RETURN (k);
}
