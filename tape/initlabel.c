/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */


#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"

/* initlabelroutines - provides access to the static structures used to store */
/* the information needed by the label processing routines                    */

/* Boolean value that is true if the static variable s_devlblinfo has a value */
static int s_devlblinfoHasValue = 0;

static struct devlblinfo s_devlblinfo;

int getlabelinfo (const char *const path,
                  struct devlblinfo  **const dlipp)
{
	/* Can't get the label info if the static variable s_devlblinfo has */
	/* no value                                                         */
	if (!s_devlblinfoHasValue) {
		serrno = ETNOLBLINFO;
		return (-1);
	}

	if (strncmp(s_devlblinfo.path, path, sizeof(s_devlblinfo.path))) {
		serrno = ETNOLBLINFO;
		return (-1);
	} else {
		*dlipp = &s_devlblinfo;
		return (0);
	}
}

/*	rmlabelinfo - remove label information from the internal table */

int rmlabelinfo (const char *const path,
                 const int flags)
{
	/* Cannot remove the label info if the static variable s_devlblinfo */
        /* has no value                                                     */
	if (!s_devlblinfoHasValue) {
		return (0);
	}

	/* If requested to release all then no need to search for path */
	if(flags & TPRLS_ALL) {
		memset (&s_devlblinfo, '\0', sizeof(s_devlblinfo));
		s_devlblinfoHasValue = 0;
		return 0;
	}

	/* If the label info cannot be found then nothing to do */
	if (strncmp(s_devlblinfo.path, path, sizeof(s_devlblinfo.path))) {
		return (0);
	}

	/* Clear the label info */
	memset (&s_devlblinfo, '\0', sizeof(s_devlblinfo));

	/* Keep the label info memory reservered if requested by the */
        /* TPRLS_KEEP_RSV bit of flags                               */
	if (flags & TPRLS_KEEP_RSV) {
		s_devlblinfoHasValue = 1;
	} else {
		s_devlblinfoHasValue = 0;
	}

	return (0);
}

/*	setdevinfo - set flags for optimization of label processing routines
 *	according to device type and density
 */
int setdevinfo (const char *const path,
                const char *const devtype,
                const int den,
                const int lblcode)
{
	const struct devinfo *const devinfo = Ctape_devinfo (devtype);

	/* Cannot set the info if the static variable s_devlblinfo already */
	/* has a value for another path */
	if (s_devlblinfoHasValue &&
		strncmp(s_devlblinfo.path, path, sizeof(s_devlblinfo.path))) {
		serrno = ETNOLBLINFOMEM;
		return (-1);
	}

	strncpy (s_devlblinfo.path, path, sizeof(s_devlblinfo.path));
	s_devlblinfo.path[sizeof(s_devlblinfo.path) - 1] = '\0';

	strncpy (s_devlblinfo.devtype, devtype, sizeof(s_devlblinfo.devtype));
	s_devlblinfo.devtype[sizeof(s_devlblinfo.devtype) - 1] = '\0';

	s_devlblinfo.dev1tm = (devinfo->eoitpmrks == 1) ? 1 : 0;

	if (strcmp (devtype, "8200") == 0 || den == D8200 || den == D8200C) {
		/* An Exabyte 8200 must be positionned */
		/* on the BOT side of a long filemark  */
		/* before starting to write            */
		s_devlblinfo.rewritetm = 1;
	} else {
		s_devlblinfo.rewritetm = 0;
	}

	s_devlblinfo.lblcode = lblcode;

	s_devlblinfoHasValue = 1;

	return (0);
}

/*	setlabelinfo - set label information for label processing routines */

int setlabelinfo (const char *const path,
                  const int flags,
                  const int fseq,
                  const char *const vol1,
                  const char *const hdr1,
                  const char *const hdr2,
                  const char *const uhl1)
{
	/* Cannot set the label info if the static variable s_devlblinfo has */
	/* no value                                                          */
	if (!s_devlblinfoHasValue) {
		serrno = ETNOLBLINFO;
		return (-1);
	}

	if (strncmp(s_devlblinfo.path, path, sizeof(s_devlblinfo.path))) {
		serrno = ETNOLBLINFO;
		return (-1);
	} else {
		s_devlblinfo.flags = flags;
		s_devlblinfo.fseq = fseq;
		if (s_devlblinfo.lblcode == AL || s_devlblinfo.lblcode == AUL ||
			s_devlblinfo.lblcode == SL) {
			strncpy (s_devlblinfo.vol1, vol1,
				sizeof(s_devlblinfo.vol1));
			s_devlblinfo.vol1[sizeof(s_devlblinfo.vol1) - 1] = '\0';

			strncpy (s_devlblinfo.hdr1, hdr1,
				sizeof(s_devlblinfo.hdr1));
			s_devlblinfo.hdr1[sizeof(s_devlblinfo.hdr1) - 1] = '\0';

			strncpy (s_devlblinfo.hdr2, hdr2,
				sizeof(s_devlblinfo.hdr2));
			s_devlblinfo.hdr2[sizeof(s_devlblinfo.hdr2) - 1] = '\0';

			strncpy (s_devlblinfo.uhl1, uhl1,
				sizeof(s_devlblinfo.uhl1));
			s_devlblinfo.uhl1[sizeof(s_devlblinfo.uhl1) - 1] = '\0';
		}
        	return (0);
	}
}
