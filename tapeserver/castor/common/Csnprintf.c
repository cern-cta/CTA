/*
 * $Id: Csnprintf.c,v 1.2 2007/12/07 11:40:52 sponcec3 Exp $
 */

#include "Csnprintf.h"

/* Hide the snprintf and al. call v.s. different OS. */
/* Sometimes a different name, sometimes do not exist */

int Csnprintf(char *str, size_t size, const char *format, ...) {
	int rc;
	va_list args;

	va_start (args, format);
	/* Note: we cannot call sprintf again, because a va_list is a real */
	/* parameter on its own - it cannot be changed to a real list of */
	/* parameters on the stack without being not portable */
	rc = Cvsnprintf(str,size,format,args);
	va_end (args);
	return(rc);
}

int Cvsnprintf(char *str, size_t size, const char *format, va_list args)
{
	return(vsnprintf(str, size, format, args));
}

