/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

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
