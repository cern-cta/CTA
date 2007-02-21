/*
 * Copyright (C) 1993-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)cvtden.c,v 1.7 2002/11/06 10:11:26 CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*	cvtden - check and convert alphanumeric densities to integer */
#include <errno.h>
#include <sys/types.h>
#include <string.h>
#include "Ctape.h"
#include "serrno.h"
static char adens[CA_MAXDENNUM][6] = {"0", "800", "1600", "6250", "38000",
	"8200", "8500", "38KD", "2G", "6G", "10G", "FMT", "RAW", "DDS",
				      "20G", "25G", "35G", "50G", "40G", "60G", "100G", "200G", "110G","160G","300G", "400G", "500G", "700G"};
static char adensc[CA_MAXDENNUM][6] = {"", "", "", "", "38KC",
	"8200C", "8500C", "38KDC", "", "", "10GC", "", "", "DDSC",
				       "20GC", "25GC", "35GC", "50GC", "40GC", "60GC", "100GC", "200GC", "110GC","160GC","300GC", "400GC", "500GC", "700GC"};
int cvtden(aden)
char	*aden;
{
	int i;

	for (i = 0; i < CA_MAXDENNUM; i++)
		if (strcmp (aden, adens[i]) == 0) return (i);
	for (i = 0; i < CA_MAXDENNUM; i++)
		if (strcmp (aden, adensc[i]) == 0) return (i | IDRC);
	if (!strcmp (aden, "38K")) return (D38000);
	if (!strcmp (aden, "43200")) return (D8200);
	if (!strcmp (aden, "86400")) return (D8500);
	serrno = EINVAL;
	return (-1);
}

/*	den2aden - convert integer densities to alphanumeric */
char *
den2aden(den)
int	den;
{
	if (den & IDRC)
		return (adensc[den & ~IDRC]);
	else
		return (adens[den]);
}
