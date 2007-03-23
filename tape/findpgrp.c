/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: findpgrp.c,v $ $Revision: 1.7 $ $Date: 2007/03/23 13:08:33 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*	findpgrp - finds the process group */

#include <unistd.h>
#include <sys/types.h>
#include "Ctape_api.h"

int findpgrp()
{
#if defined(_WIN32)
	return (getpid());
#else
	return (getpgrp());
#endif
}
