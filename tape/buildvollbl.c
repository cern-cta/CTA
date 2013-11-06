/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */


/*	buildvollbl - build VOL1 */
#include <string.h>
#include <sys/types.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "stage_constants.h"

int buildvollbl(char vol1[],
		char *vsn,
		int lblcode,
		char *name)
{
	int i;

	/* build VOL1 */

	for (i = 0; i < 80; i++)
		vol1[i] = ' ';
	vol1[80] = '\0';
	strcpy (vol1, "VOL1");
	memcpy (vol1 + 4, vsn, strlen(vsn));
		if (strcmp (name, STAGERSUPERUSER))
			memcpy (vol1 + 37, name, strlen(name));
		else
			memcpy (vol1 + 37, "CASTOR", 6);
	if (lblcode == AUL)
		vol1[79] = '3';
        return (0);
}
