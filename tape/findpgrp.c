/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: findpgrp.c,v $ $Revision: 1.5 $ $Date: 2000/10/03 07:37:27 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	findpgrp - finds the process group */

findpgrp()
{
#if defined(_WIN32)
	return (getpid());
#else
	return (getpgrp());
#endif
}
