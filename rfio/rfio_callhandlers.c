/*
 * $Id: rfio_callhandlers.c,v 1.1 2005/01/05 17:37:16 bcouturi Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rfio_callhandlers.c,v $ $Revision: 1.1 $ $Date: 2005/01/05 17:37:16 $ CERN IT-ADC/CA Benjamin Couturier";
#endif /* not lint */

#include <sys/types.h>

int rfio_handle_open(const char *lfn, 
		     int flags,
		     int mode,
		     uid_t uid,
		     gid_t gid,
		     char **pfn, 
		     void **ctx) {

  *ctx = (void *)strdup(lfn);
  *pfn = (char *)strdup(lfn);
  return 0;
}

int rfio_handle_firstwrite(void *ctx) {
}

int rfio_handle_close(void *ctx) {
}
