/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: findpgrp.c,v $ $Revision: 1.6 $ $Date: 2007/02/21 16:31:31 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*	findpgrp - finds the process group */

#include <unistd.h>

int findpgrp()
{
#if defined(_WIN32)
	return (getpid());
#else
	return (getpgrp());
#endif
}
