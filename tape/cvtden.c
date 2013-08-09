/*
 * Copyright (C) 1993-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */


/*	cvtden - check and convert alphanumeric densities to integer */
#include <errno.h>
#include <sys/types.h>
#include <string.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
/* List of the non-compressed tape densities (not so much used in CASTOR as of may 2011 */
/* As of may 2011, we have:
 *  - STK T10KA at 500G
 *  - STK T10KB at 1000G
 *  - IBM TS1130 at 1000G
 *  - STK T10KC at 5000G for usual cartridges and 1000G for sports cartridges
 *  - IBM TS1140 at 4000G (using JC/JY media), 1600G (using JB/JX media), or 500 GB (using JK media).
 */
static char *adens[CA_MAXDENNUM] = {
       "0",    "800",   "1600",   "6250", "38000",  "8200",  "8500",  "38KD",    "2G",     "6G",
     "10G",    "FMT",    "RAW",    "DDS",   "20G",   "25G",   "35G",   "50G",   "40G",    "60G",
    "100G",   "200G",   "110G",   "160G",  "300G",  "400G",  "500G",  "700G",  "800G",  "1000G",
   "1500G",  "5000G",  "1600G",  "4000G"};
/* List of the compressed tape densities (the preferred type in CASTOR as of may 2011) */
static char *adensc[CA_MAXDENNUM] = {
        "",       "",       "",       "",  "38KC", "8200C", "8500C", "38KDC",      "",       "",
    "10GC",       "",       "",   "DDSC",  "20GC",  "25GC",  "35GC",  "50GC",  "40GC",   "60GC",
   "100GC",  "200GC",  "110GC",  "160GC", "300GC", "400GC", "500GC", "700GC", "800GC", "1000GC",
  "1500GC", "5000GC", "1600GC", "4000GC"};
int cvtden(const char *const aden)
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
char* den2aden(int	den)
{
	if (den & IDRC)
		return (adensc[den & ~IDRC]);
	else
		return (adens[den]);
}
