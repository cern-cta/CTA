/*
 * $Id: sleep.c,v 1.2 1999/12/09 13:47:54 jdurand Exp $
 */

/*
 * Copyright (C) 1997 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: sleep.c,v $ $Revision: 1.2 $ $Date: 1999/12/09 13:47:54 $ CERN/IT/PDP/DM Jean-Philippe Baud";
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
