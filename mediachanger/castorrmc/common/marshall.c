/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: marshall.c,v $ $Revision: 1.2 $ $Date: 2000/12/12 09:41:49 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*
 * marshall.c - wrappers on top of marshall macros
 */

#include <string.h> 

int DLL_DECL
_unmarshall_STRINGN(ptr, str, n)
char **ptr;
char *str;
int n;
{
	char *p;

	(void) strncpy (*ptr, str, n);
	if (p = memchr (str, 0, n)) {
		*ptr += (p - str + 1);
		return (0);
	}
	*(str + n - 1) = '\0';
	*ptr += strlen(*ptr) + 1;
	return (-1);
}
