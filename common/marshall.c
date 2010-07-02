/*
 * Copyright (C) 2000-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * marshall.c - wrappers on top of marshall macros
 */

#include <string.h> 
#include "marshall.h"
#include "osdep.h"

int
_unmarshall_STRINGN(ptr, str, n)
char **ptr;
char *str;
int n;
{
	char *p;

	(void) strncpy (str, *ptr, n);
	if ((p = memchr (str, 0, n)) != NULL) {
		*ptr += (p - str + 1);
		return (0);
	}
	*(str + n - 1) = '\0';
	*ptr += strlen(*ptr) + 1;
	return (-1);
}
