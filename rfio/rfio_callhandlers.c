
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
#include "ceph/ceph_posix.h"
#include "movers/moverclose.h"

struct internal_context {
  int one_byte_at_least;
  mode_t mode;
  int flags;
  char* transferid;
  char *pfn;
  u_signed64 fileId;
  char *nsHost;
};

extern char* transferid;
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
  if (transferid != NULL) {
    /* rfiod started with -i option and option value > 0 */
    struct internal_context *internal_context = calloc(1, sizeof(struct internal_context));

    if (internal_context != NULL) {
      internal_context->mode = (mode_t) mode;
      internal_context->flags = flags;
      internal_context->pfn = strdup(lfn);
      internal_context->transferid = transferid;
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
                (*logfunc)(LOG_ERR, "rfio_handle_open: memory allocation error duplicating "
                    "nameserver host (%s)\n", strerror(errno));
                serrno = errno;
                free(internal_context);
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
          (*logfunc)(LOG_ERR, "rfio_handle_open: error parsing the physical filename %s: (format unknown)\n",
              internal_context->pfn);
          serrno = EINVAL;
          free(internal_context);
          return -1;
        }
      } else {
        (*logfunc)(LOG_ERR, "rfio_handle_open: error parsing the physical filename %s: (%s)\n",
            internal_context->pfn, strerror(errno));
        serrno = errno;
        free(internal_context);
        return -1;
      }
    } else {
      (*logfunc)(LOG_ERR, "rfio_handle_open: calloc error (%s)\n", strerror(errno));
      serrno = errno;
      return -1;
    }

    if (flags && O_WRONLY != 0) {
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
    if (!((internal_context->flags & O_ACCMODE) == O_RDONLY)  /* Get case (should actually never happen!) */
        && !((internal_context->flags & O_TRUNC) == O_TRUNC)) {   /* Put case */
      /* This is the case of a real update, which is not supported any longer, therefore throw error */
      (*logfunc)(LOG_INFO, "rfio_handle_firstwrite: update not supported\n");
      serrno = ENOTSUP;
      return -1;
    }
    /* otherwise this is a first write without a read, so we are effectively in a Put request */
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
  int      xattr_len;
  char     csumvalue[CA_MAXCKSUMLEN+1] = "0";
  char     csumtype[CA_MAXCKSUMNAMELEN+1] = "AD";
  char*    error_msg = NULL;
  int      port = MOVERHANDLERPORT;

  if (internal_context != NULL) {
    if (close_status) {
      error_msg = strdup(strerror(errno));
    }
    if (((internal_context->flags & O_TRUNC) == O_TRUNC) ||
        (internal_context->one_byte_at_least)) {   /* see also comment in rfio_handle_open */
      /* This is a write */
      /* File still exists - this is a candidate for migration regardless of its size (zero-length are ignored in the stager) */
      /* first we try to read a file xattr for checksum */
      if ((xattr_len = ceph_posix_getxattr(internal_context->pfn, "user.castor.checksum.value", csumvalue, CA_MAXCKSUMLEN)) == -1) {
        (*logfunc)(LOG_ERR, "rfio_handle_close: getxattr failed for user.castor.checksum.value, skipping checksum. Error=%d\n", errno);
      } else {
        csumvalue[xattr_len] = '\0';
        (*logfunc)(LOG_DEBUG,"rfio_handle_close: csumvalue for the file on the disk=0x%s\n", csumvalue);
        if ((xattr_len = ceph_posix_getxattr(internal_context->pfn, "user.castor.checksum.type", csumtype, CA_MAXCKSUMNAMELEN)) == -1) {
          (*logfunc)(LOG_ERR, "rfio_handle_close: getxattr failed for user.castor.checksum.type, skipping checksum. Error=%d\n", errno);
        } else {
          csumtype[xattr_len] = '\0';
          (*logfunc)(LOG_DEBUG, "rfio_handle_close: csumtype is %s\n", csumtype);
          /* now we have csumtype from disk and have to convert it for castor name server database */
          for (xattr_len = 0; xattr_len < csumalgnum; xattr_len++) {
            if (strncmp(csumtype, csumtypeDiskNs[xattr_len * 2], CA_MAXCKSUMNAMELEN) == 0) {
              /* we have found something */
              strcpy(csumtype, csumtypeDiskNs[xattr_len * 2 + 1]);
              break;
            }
          }
        }
      }
    } /* else this is a read, the checksum value is irrelevant, and the close_status decides whether it was successful or not */
    (*logfunc)(LOG_INFO, "rfio_handle_close: calling mover_close_file on transferid=%s\n", internal_context->transferid);
    if(getconfent("DiskManager", "MoverHandlerPort", 0) != NULL) {
      port = atoi(getconfent("DiskManager", "MoverHandlerPort", 0));
    }
    if (mover_close_file(port, internal_context->transferid, (u_signed64)filestat->st_size, csumtype, csumvalue, &close_status, &error_msg) != 0) {
      serrno = close_status;
      (*logfunc)(LOG_ERR, "rfio_handle_close: mover_close_file failed for transferid=%s with error: %s\n", internal_context->transferid, error_msg);
      free(error_msg);
    } else {
      forced_mover_exit_error = 0;
    }
  } else {
    (*logfunc)(LOG_DEBUG, "rfio_handle_close: no context given, nothing to do\n");
    return 0;
  }

  if (forced_mover_exit_error != 0) {
    return -1;
  }
  return 0;
}
