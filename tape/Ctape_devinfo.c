/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Ctape_devinfo.c,v $ $Revision: 1.2 $ $Date: 2000/03/09 08:42:56 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	Ctape_devinfo - table of device characteristics */

#include <string.h>
#include <sys/types.h>
#include "Ctape.h"
#include "Ctape_api.h"

static struct devinfo devinfo[] = {
	"3480", 1, 2, 0, 1,   262144,  32760, 0x10, D38000, 0, D38KD, 0, D38KC, 0, D38KDC, 0, 0, 0, 0, 0,
	"3590", 1, 1, 1, 1,  2097152,  32760, 0x10, D10G, 0, D10GC, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	"8200", 1, 2, 0, 1,   245760,  32760, 0x00, D8200, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	"8500", 1, 2, 0, 1,   245760,  32760, 0x00, D8200, 0x14, D8500, 0x15, 0, 0, 0, 0, 0, 0, 0, 0,
	"8505", 1, 2, 0, 1,   245760,  32760, 0x00, D8200, 0x14, D8500, 0x15, D8200C, 0x90, D8500C, 0x8C, 0, 0, 0, 0,
	"9840", 1, 1, 1, 1,   262144,  32760, 0x10, D20G, 0, D20GC, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	"DAT",  1, 2, 0, 1, 16777215,  32760, 0x0F, DDS, 0x24, DDSC, 0x24, 0, 0, 0, 0, 0, 0, 0, 0,
	"DLT",  1, 1, 1, 1, 16777215,  32760, 0x10, D10G, 0x19, D20G, 0x1A, D35G, 0x1B, D10GC, 0x19, D20GC, 0x1A, D35GC, 0x1B,
	"QIC",  0, 2, 0, 512,    512,    512, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	"SD3",  1, 1, 1, 1,   262144, 262144, 0x10, D10G, 0, D25G, 0, D50G, 0, D10GC, 0, D25GC, 0, D50GC, 0,

	/* new devices should be added here */

	/* last line corresponds to unknown device */

	"", 1, 2, 0, 1, 262144, 32760, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

struct devinfo *
Ctape_devinfo(devtype)
char *devtype;
{
	char devtype_local[CA_MAXMODELLEN+1];
	int i;
	char *p;

	strcpy (devtype_local, devtype);
	if (p = strstr (devtype_local, "/VB"))
		*p = '\0';
	for (i = 0; *devinfo[i].devtype; i++)
		if (strcmp (devtype_local, devinfo[i].devtype) == 0) break;
	return (&devinfo[i]);
}
