
/*
 * rfio_callhandlers.c,v 1.6 2005/07/21 09:13:07 itglp Exp
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include "rfio.h"
#include "u64subr.h"
#include "log.h"
#include "serrno.h"
#include "getconfent.h"
#include "Castor_limits.h"
#include <sys/xattr.h>
#include "castor/stager/IJobSvc.h"

struct internal_context {
  int one_byte_at_least;
  mode_t mode;
  int flags;
  u_signed64 subrequest_id;
  char *pfn;
  u_signed64 fileId;
  char *nsHost;
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
  (void)uid;
  (void)gid;
  (void)need_user_check;
  if (subrequest_id > 0) {
    /* rfiod started with -Z option and option value > 0 */
    struct internal_context *internal_context = calloc(1, sizeof(struct internal_context));

    if (internal_context != NULL) {
      internal_context->mode = (mode_t) mode;
      internal_context->flags = flags;
      internal_context->pfn = strdup(lfn);
      internal_context->subrequest_id = subrequest_id;
      internal_context->nsHost = NULL;
      internal_context->fileId = 0;

      /* Extract the file id and name server host from the pfn */
      char *cpfn = strdup(internal_context->pfn);
      if (cpfn != NULL) {

        /* Get the filename from the path */
        char *buf = strrchr(cpfn, '/');
        if (buf != NULL) {
          *buf++ = '\0';

          /* Extract the file id */
          char *pnh = strchr(buf, '@');
          if (pnh != NULL) {
            *pnh++ = '\0';
            internal_context->fileId = strtou64(buf);

            /* Extract the name server host */
            char *p = strrchr(pnh, '.');
            if (p != NULL) {
              *p++ = '\0';
              internal_context->nsHost = strdup(pnh);
              if (internal_context->nsHost == NULL) {
                log(LOG_ERR, "rfio_handle_open : memory allocation error duplicating "
                    "nameserver host (%s)\n", strerror(errno));
                serrno = errno;
                return -1;
              }
            }
          }
        }
        free(cpfn);

        /* If we got this far and the name server host is not defined then the
         * pfn was not as we expected
         */
        if (internal_context->nsHost == NULL) {
          log(LOG_ERR, "rfio_handle_open : error parsing the physical filename %s: (format unknown)\n",
              internal_context->pfn);
          serrno = EINVAL;
          return -1;
        }
      } else {
        log(LOG_ERR, "rfio_handle_open : error parsing the physical filename %s: (%s)\n",
            internal_context->pfn, strerror(errno));
        serrno = errno;
        return -1;
      }
    } else {
      log(LOG_ERR, "rfio_handle_open : calloc error (%s)\n", strerror(errno));
      serrno = errno;
      return -1;
    }

    struct stat64 statbuf;
    if (stat64(internal_context->pfn,&statbuf) < 0) {
      /* local file does not exist: here we assume that it's going to be created,
         and this is equivalent to a write operation regardless the mode bits.
         So we set one_byte_at_least to 1 */
      internal_context->one_byte_at_least = 1;
    } else {
      internal_context->one_byte_at_least = 0;
    }

    *ctx = (void *)internal_context;
  }

  *pfn = (char *)strdup(lfn);
  return 0;
}

int rfio_handle_firstwrite(void *ctx) {
  struct internal_context *internal_context = (struct internal_context *) ctx;
  if (internal_context != NULL) {

    /* In case of an update, we should call firstByteWritten, so we ignore pure Get and Put cases */
    if (!((internal_context->flags & O_ACCMODE) == O_RDONLY)  /* Get case (should actually never happen!) */
        && !((internal_context->flags & O_TRUNC) == O_TRUNC)) {   /* Put case */
      char tmpbuf[21];
      int rc;
      log(LOG_INFO,
          "rfio_handle_firstwrite : Calling Cstager_IJobSvc_firstByteWritten on subrequest_id=%s\n",
          u64tostr(internal_context->subrequest_id, tmpbuf, 0));
      int error_code;
      char* error_msg;
      rc = Cstager_IJobSvc_firstByteWritten(internal_context->subrequest_id,internal_context->fileId, internal_context->nsHost,&error_code,&error_msg);
      if (rc != 0) {
        serrno = error_code;
        log(LOG_ERR,
            "rfio_handle_firstwrite : Cstager_IJobSvc_firstByteWritten error for subrequest_id=%s (%s)\n",
            u64tostr(internal_context->subrequest_id, tmpbuf, 0),
            error_msg);
        free(error_msg);
        return rc;
      }
    }
    internal_context->one_byte_at_least = 1;
  }
  return 0;
}

int rfio_handle_close(void *ctx,
                      struct stat *filestat,
                      int close_status) {

  struct internal_context *internal_context = (struct internal_context *) ctx;

  char*    csumtypeDiskNs[]={"ADLER32","AD","CRC32","CS","MD5","MD"}; /* in future move it in the one of the headers file */
  char     csumalgnum=3;
  int      useCksum;
  int      xattr_len;
  char     csumvalue[CA_MAXCKSUMLEN+1];
  char     csumtype[CA_MAXCKSUMNAMELEN+1];
  char     *conf_ent;
  int      error_code;
  char*    error_msg;

  (void)filestat;
  if (internal_context != NULL) {
    char tmpbuf[21];

    if (((internal_context->flags & O_TRUNC) == O_TRUNC) ||
        (internal_context->one_byte_at_least)) {   /* see also comment in rfio_handle_open */
      /* This is a write */
      struct stat64 statbuf;

      if (stat64(internal_context->pfn, &statbuf) == 0) {
        /* File still exists - this is a candidate for migration regardless of its size (zero-length are ignored in the stager) */
        useCksum = 1;
        csumtype[0] = '\0';
        csumvalue[0] = '\0';
        if ((conf_ent = getconfent("RFIOD", "USE_CKSUM", 0)) != NULL) {
          if (!strncasecmp(conf_ent, "no", 2)) {
            useCksum = 0;
          }
        }
        if (useCksum) {
          /* first we try to read a file xattr for checksum */
          if ((xattr_len = getxattr(internal_context->pfn, "user.castor.checksum.value", csumvalue, CA_MAXCKSUMLEN)) == -1) {
            log(LOG_ERR, "rfio_handle_close : fgetxattr failed for user.castor.checksum.value, error=%d\n", errno);
            log(LOG_ERR, "rfio_handle_close : skipping checksums for castor NS\n");
            useCksum = 0; /* we don't have the file checksum, and will not fill it in castor NS database */
          } else {
            csumvalue[xattr_len] = '\0';
            log(LOG_DEBUG,"rfio_handle_close : csumvalue for the file on the disk=0x%s\n", csumvalue);
            if ((xattr_len = getxattr(internal_context->pfn, "user.castor.checksum.type", csumtype, CA_MAXCKSUMNAMELEN)) == -1) {
              log(LOG_ERR, "rfio_handle_close : fgetxattr failed for user.castor.checksum.type, error=%d\n", errno);
              log(LOG_ERR, "rfio_handle_close : skipping checksums for castor NS\n");
              useCksum = 0; /* we don't have the file checksum, and will not fill it in castor NS database */
            } else {
              csumtype[xattr_len] = '\0';
              log(LOG_DEBUG, "rfio_handle_close : csumtype is %s\n", csumtype);
              /* now we have csumtype from disk and have to convert it for castor name server database */
              for (xattr_len = 0; xattr_len < csumalgnum; xattr_len++) {
                if (strncmp(csumtype, csumtypeDiskNs[xattr_len * 2], CA_MAXCKSUMNAMELEN) == 0) {
          	/* we have found something */
          	strcpy(csumtype, csumtypeDiskNs[xattr_len * 2 + 1]);
          	useCksum = 2;
          	break;
                }
              }
              if (useCksum != 2) {
                log(LOG_ERR, "rfio_handle_close : unknown checksum type %s\n", csumtype);
                log(LOG_ERR, "rfio_handle_close : skipping checksums for castor NS\n");
                useCksum = 0; /* we don't have the file checksum, and will not fill it in castor NS database */
              }
            }
          }
        }
        if (useCksum == 0) {
          csumtype[0] = '\0';
          csumvalue[0] = '\0';
        }
        log(LOG_INFO, "rfio_handle_close : Calling Cstager_IJobSvc_prepareForMigration on subrequest_id=%s\n", u64tostr(internal_context->subrequest_id, tmpbuf, 0));
        if (Cstager_IJobSvc_prepareForMigration(internal_context->subrequest_id,(u_signed64) statbuf.st_size, (u_signed64) time(NULL),internal_context->fileId, internal_context->nsHost, csumtype, csumvalue, &error_code, &error_msg) != 0) {
          serrno = error_code;
          log(LOG_ERR, "rfio_handle_close : Cstager_IJobSvc_prepareForMigration error for subrequest_id=%s (%s)\n", u64tostr(internal_context->subrequest_id, tmpbuf, 0), error_msg);
          free(error_msg);
        } else {
          forced_mover_exit_error = 0;
        }
      } else {
        /* File does not exist !? */
        log(LOG_ERR, "rfio_handle_close : stat64() error (%s)\n", strerror(errno));
        log(LOG_INFO, "rfio_handle_close : Calling Cstager_IJobSvc_putFailed on subrequest_id=%s\n", u64tostr(internal_context->subrequest_id, tmpbuf, 0));
        if (Cstager_IJobSvc_putFailed(subrequest_id,internal_context->fileId, internal_context->nsHost, &error_code, &error_msg) != 0) {
          serrno = error_code;
          log(LOG_ERR, "rfio_handle_close : Cstager_IJobSvc_putFailed error for subrequest_id=%s (%s)\n", u64tostr(internal_context->subrequest_id, tmpbuf, 0), error_msg);
          free(error_msg);
        } else {
          forced_mover_exit_error = 0;
        }
      }
    } else {
      /* This is a read: the close() status is enough to decide on a getUpdateDone/Failed */
      if (close_status == 0) {
        log(LOG_INFO, "rfio_handle_close : Calling Cstager_IJobSvc_getUpdateDone on subrequest_id=%s\n", u64tostr(internal_context->subrequest_id, tmpbuf, 0));
        if (Cstager_IJobSvc_getUpdateDone(internal_context->subrequest_id,internal_context->fileId, internal_context->nsHost, &error_code, &error_msg) != 0) {
          serrno = error_code;
          log(LOG_ERR, "rfio_handle_close : Cstager_IJobSvc_getUpdateDone error for subrequest_id=%s (%s)\n", u64tostr(internal_context->subrequest_id, tmpbuf, 0), error_msg);
          free(error_msg);
        } else {
          forced_mover_exit_error = 0;
        }
      } else {
        log(LOG_INFO, "rfio_handle_close : Calling Cstager_IJobSvc_getUpdateFailed on subrequest_id=%s\n", u64tostr(internal_context->subrequest_id, tmpbuf, 0));
        if (Cstager_IJobSvc_getUpdateFailed(subrequest_id,internal_context->fileId, internal_context->nsHost, &error_code, &error_msg) != 0) {
          serrno = error_code;
          log(LOG_ERR, "rfio_handle_close : Cstager_IJobSvc_getUpdateFailed error for subrequest_id=%s (%s)\n", u64tostr(internal_context->subrequest_id, tmpbuf, 0), error_msg);
          free(error_msg);
        } else {
          forced_mover_exit_error = 0;
        }
      }
    }
  } else {
    return 0;
  }

  if (forced_mover_exit_error != 0){
    return -1;
  }
  return 0;

}
