/*
 * $Id: stager_client_api_open.cpp,v 1.9 2007/02/21 09:46:22 sponcec3 Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

/* ============== */
/* System headers */
/* ============== */
#include <sys/types.h>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
/* ============= */
/* Local headers */
/* ============= */

#include "stager_api.h"

/* ================= */
/* Internal routines */
/* ================= */

/* ================= */
/* External routines */
/* ================= */


////////////////////////////////////////////////////////////
//    stage_open                                          //
////////////////////////////////////////////////////////////


EXTERN_C int DLL_DECL stage_open(const char *userTag,
                                 const char *protocol,
                                 const char *filename,
                                 int flags,
                                 mode_t mode,
				 u_signed64 size,
				 struct stage_io_fileresp **response,
                                 char **requestId,
                                 struct stage_options* opts) {
  
  if ((flags == O_RDONLY) ||
      (flags == (O_TRUNC|O_RDONLY)) ||
      (flags == (O_CREAT|O_RDONLY)) ||
      (flags == (O_LARGEFILE|O_RDONLY)) ||
      (flags == (O_LARGEFILE|O_TRUNC|O_RDONLY)) ||
      (flags == (O_LARGEFILE|O_CREAT|O_RDONLY))) {
    /* Always use stage_get for read-only mode */
    return stage_get(userTag, 
                     protocol, 
                     filename, 
                     response,
                     requestId, 
                     opts);
  } else if (((flags & O_TRUNC) == O_TRUNC) &&
	     ((flags & O_CREAT) == O_CREAT)) {
    /* We should always use stage_put when O_TRUNC is requested but we do it only is O_CREAT is
       also set. Otherwise, we do an update and it will be handled as a put due to the O_TRUNC flag.
       The reason for this is that a put is always first removing the file before recreating it.
       This leads to a problem when the O_CREAT flag is not set that the recreation fails. Thus
       the O_CREAT flag is forced in rfio_hsmif when O_TRUNC is set. We rely here on the fact
       that in case of no O_CREAT, the request is stopped at the stager level. However, this is
       not the case if we use put, only if we use update and set the flags. */
    return stage_put(userTag, 
                     protocol, 
                     filename,
                     mode,
		     size,
                     response, 
                     requestId, 
                     opts);
  } else {
    /* In the rest of the cases, use stage_update, passing the flags for further
       checks in the stager ! */
    return stage_update(userTag, 
                        protocol,
                        filename, 
                        flags,
                        mode, 
			size,
                        response,
                        requestId, 
                        opts);
  }
  // We should never arrive here !
  return -1;

}

