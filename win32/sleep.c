/*
 * Copyright (C) 1997 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)sleep.c	1.1 08/07/97 CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <windows.h>
unsigned int
sleep(seconds)
unsigned int seconds;
{
	DWORD ms = 1000 * seconds;
	Sleep (ms);
	return (0);
}
