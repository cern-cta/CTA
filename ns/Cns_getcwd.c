/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cns_getcwd.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:18 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	Cns_getcwd - get current working directory */

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

char DLL_DECL *
Cns_getcwd(char *buf, int size)
{
	int alloc = 0;
	char func[16];
	char path[CA_MAXPATHLEN+1];
	struct Cns_api_thread_info *thip;
 
	strcpy (func, "Cns_getcwd");
	if (Cns_apiinit (&thip))
		return (NULL);

	if (size <= 0) {
		serrno = EINVAL;
		return (NULL);
	}
	if (! *thip->server) {
		serrno = ENOENT;
		return (NULL);
	}
	if (! buf) {
		if ((buf = malloc (size)) == NULL) {
			serrno = ENOMEM;
			return (NULL);
		}
		alloc = 1;
	}
		

	if (Cns_getpath (thip->server, thip->cwd, path) < 0) {
		if (alloc)
			free (buf);
		return (NULL);
	}
	if (strlen (path) > (size - 1)) {
		serrno = ERANGE;
		if (alloc)
			free (buf);
		return (NULL);
	}
	strcpy (buf, path);
	return (buf);
}
