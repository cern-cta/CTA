/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "%W% %G% CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	buildvollbl - build VOL1 */
#include <sys/types.h>
#include "Ctape.h"
buildvollbl(vol1, vsn, lblcode, name)
char vol1[];
char *vsn;
int lblcode;
char *name;
{
	int i;

	/* build VOL1 */

	for (i = 0; i < 80; i++)
		vol1[i] = ' ';
	vol1[80] = '\0';
	strcpy (vol1, "VOL1");
	memcpy (vol1 + 4, vsn, strlen(vsn));
	if (lblcode == SL)
		memcpy (vol1 + 41, name, strlen(name));
	if (lblcode == AL)
		vol1[79] = '3';
}
