/*
 * Copyright (C) 1994-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */


/*      gettperror - get drive status after I/O error and
			build error msg from sense key or sense bytes */
/*	return	ETBLANK		blank tape
 *		ETCOMPA		compatibility problem
 *		ETHWERR		device malfunction
 *		ETPARIT		parity error
 *		ETUNREC		unrecoverable media error
 *		ETNOSNS		no sense
 */
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"

int gettperror(int tapefd,
               char *path,
               char **msgaddr)
{
  const int rc = -1;
  *msgaddr = "gettperror not supported";
  errno = ENOTSUP;
  RETURN (rc);
}
