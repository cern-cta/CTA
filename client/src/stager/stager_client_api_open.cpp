/*
 * $Id: stager_client_api_open.cpp,v 1.5 2006/05/03 15:34:49 sponcec3 Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: stager_client_api_open.cpp,v $ $Revision: 1.5 $ $Date: 2006/05/03 15:34:49 $ CERN IT-ADC/CA Benjamin Couturier";
#endif

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
  
  char *func = "stage_open_ext";

  if ((flags == O_RDONLY) ||
      (flags == O_TRUNC) ||
      (flags == O_CREAT)) {
    //std::cout << "-------------> STAGE_GET" << std::endl;
    /* Always use stage_get for read-only mode */
    return stage_get(userTag, 
                     protocol, 
                     filename, 
                     response,
                     requestId, 
                     opts);
  } else if ((flags & O_TRUNC) == O_TRUNC) {
    //std::cout << "-------------> STAGE_PUT" << std::endl;
    /* Always use stage_put when O_TRUNC is requested */
    return stage_put(userTag, 
                     protocol, 
                     filename,
                     mode,
		     size,
                     response, 
                     requestId, 
                     opts);
  } else {
    //std::cout << "-------------> STAGE_UPDATE" << std::endl;
    /* In the rest of the cases, use stage_update, checking for O_CREAT
       to see whether the create option of update should be set ! */
    return stage_update(userTag, 
                        protocol,
                        filename, 
                        ((flags & O_CREAT) == O_CREAT),
                        mode, 
			size,
                        response,
                        requestId, 
                        opts);
  }
  // We should never arriver here !
  return -1;

}

