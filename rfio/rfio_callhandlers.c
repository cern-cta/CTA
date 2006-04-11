/*
 * rfio_callhandlers.c,v 1.6 2005/07/21 09:13:07 itglp Exp
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)rfio_callhandlers.c,v 1.6 2005/07/21 09:13:07 CERN IT-ADC/CA Benjamin Couturier";
#endif /* not lint */

#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include "rfio.h"
#include "u64subr.h"
#include "log.h"
#include "castor/BaseObject.h"
#include "castor/Constants.h"

struct internal_context {
  int one_byte_at_least;
  mode_t mode;
  int flags;
  u_signed64 subrequest_id;
  char *pfn;
};

extern u_signed64 subrequest_id;
extern int forced_mover_exit_error;

int rfio_handle_open(const char *lfn, 
		     int flags,
		     int mode,
		     uid_t uid,
		     gid_t gid,
		     char **pfn, 
		     void **ctx,
		     int *need_user_check) {

  if (subrequest_id > 0) {
    /* rfiod started with -Z option and option value > 0 */
    struct internal_context *internal_context = calloc(1,sizeof(struct internal_context));

    if (internal_context != NULL) {
      internal_context->mode = (mode_t) mode;
      internal_context->flags = flags;
      internal_context->pfn = strdup(lfn);
      internal_context->subrequest_id = subrequest_id;
    } else {
      log(LOG_ERR, "rfio_handle_open : calloc error (%s)\n", strerror(errno));
    }
    *ctx = (void *)internal_context;
  }

  *pfn = (char *)strdup(lfn);
  return 0;
}

int rfio_handle_firstwrite(void *ctx) {
  struct internal_context *internal_context = (struct internal_context *) ctx;

  if (internal_context != NULL) {
    internal_context->one_byte_at_least = 1;
  }

}

int rfio_handle_close(void *ctx,
		      struct stat *filestat,
		      int close_status) {

  struct internal_context *internal_context = (struct internal_context *) ctx;

  if (internal_context != NULL) {
    struct Cstager_IJobSvc_t **jobSvc;
    struct C_Services_t **dbService;

    C_BaseObject_initLog("rfio", SVC_STDMSG);
    if (stager_getRemJobAndDbSvc(&jobSvc,&dbService) == 0) {
      char tmpbuf[21];

      if (((internal_context->flags & O_WRONLY) == O_WRONLY) ||
	  ((internal_context->flags & O_RDWR)) == O_RDWR) {
	/* This is a write */
	struct stat64 statbuf;
	int rcstat;

	if (stat64(internal_context->pfn,&statbuf) == 0) {
	  struct Cstager_SubRequest_t *subrequest;
	  /* File still exist - this is a candidate for migration regardless of its size (zero-length are ignored in the stager) */
	  if (Cstager_SubRequest_create(&subrequest) == 0) {
	    if (Cstager_SubRequest_setId(subrequest,internal_context->subrequest_id) == 0) {
	      log(LOG_INFO, "rfio_handle_close : Calling Cstager_IJobSvc_prepareForMigration on subrequest_id=%s\n", u64tostr(internal_context->subrequest_id, tmpbuf, 0));
	      if (Cstager_IJobSvc_prepareForMigration(*jobSvc,subrequest,(u_signed64) statbuf.st_size) != 0) {
		log(LOG_ERR, "rfio_handle_close : Cstager_IJobSvc_prepareForMigration error for subrequest_id=%s (%s)\n", u64tostr(internal_context->subrequest_id, tmpbuf, 0), Cstager_IJobSvc_errorMsg(*jobSvc));
	      } else {
		forced_mover_exit_error = 0;
	      }
	    }
	  }
	} else {
	  /* File does not exit !? */
	  log(LOG_ERR, "rfio_handle_close : stat64() error (%s)\n", strerror(errno));
	  log(LOG_INFO, "rfio_handle_close : Calling Cstager_IJobSvc_putFailed on subrequest_id=%s\n", u64tostr(internal_context->subrequest_id, tmpbuf, 0));
	  if (Cstager_IJobSvc_putFailed(*jobSvc,subrequest_id) != 0) {
	    log(LOG_ERR, "rfio_handle_close : Cstager_IJobSvc_putFailed error for subrequest_id=%s (%s)\n", u64tostr(internal_context->subrequest_id, tmpbuf, 0), Cstager_IJobSvc_errorMsg(*jobSvc));
	  } else {
	    forced_mover_exit_error = 0;
	  }
	}
      } else {
	/* This is a read: the close() status is enough to decide on a getUpdateDone/Failed */
	if (close_status == 0) {
	  log(LOG_INFO, "rfio_handle_close : Calling Cstager_IJobSvc_getUpdateDone on subrequest_id=%s\n", u64tostr(internal_context->subrequest_id, tmpbuf, 0));
	  if (Cstager_IJobSvc_getUpdateDone(*jobSvc,internal_context->subrequest_id) != 0) {
	    log(LOG_ERR, "rfio_handle_close : Cstager_IJobSvc_getUpdateDone error for subrequest_id=%s (%s)\n", u64tostr(internal_context->subrequest_id, tmpbuf, 0), Cstager_IJobSvc_errorMsg(*jobSvc));
	  } else {
	    forced_mover_exit_error = 0;
	  }
	} else {
	  log(LOG_INFO, "rfio_handle_close : Calling Cstager_IJobSvc_getUpdateFailed on subrequest_id=%s\n", u64tostr(internal_context->subrequest_id, tmpbuf, 0));
	  if (Cstager_IJobSvc_getUpdateFailed(*jobSvc,subrequest_id) != 0) {
	    log(LOG_ERR, "rfio_handle_close : Cstager_IJobSvc_getUpdateFailed error for subrequest_id=%s (%s)\n", u64tostr(internal_context->subrequest_id, tmpbuf, 0), Cstager_IJobSvc_errorMsg(*jobSvc));
	  } else {
	    forced_mover_exit_error = 0;
	  }
	}
      }
    }
  }

}
