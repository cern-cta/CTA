/*
 * $Id: rfio_callhandlers.c,v 1.3 2005/01/24 16:23:25 bcouturi Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rfio_callhandlers.c,v $ $Revision: 1.3 $ $Date: 2005/01/24 16:23:25 $ CERN IT-ADC/CA Benjamin Couturier";
#endif /* not lint */

#include <sys/types.h>
#include <sys/stat.h>

int rfio_handle_open(const char *lfn, 
		     int flags,
		     int mode,
		     uid_t uid,
		     gid_t gid,
		     char **pfn, 
		     void **ctx,
		     int *need_user_check) {

  *ctx = (void *)strdup(lfn);
  *pfn = (char *)strdup(lfn);
  return 0;
}

int rfio_handle_firstwrite(void *ctx) {

}

int rfio_handle_close(void *ctx,
		      struct stat *filestat,
		      int close_status) {
}
