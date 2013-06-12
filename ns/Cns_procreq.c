/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <uuid/uuid.h>

#include "Cgrp.h"
#include "Cns.h"
#include "Cns_server.h"
#include "Cpwd.h"
#include "Cupv_api.h"
#include "marshall.h"
#include "patchlevel.h"
#include "rfcntl.h"
#include "serrno.h"
#include "u64subr.h"

/* Externs */
extern int being_shutdown;
extern char localhost[CA_MAXHOSTNAMELEN+1];
extern char nshostname[CA_MAXHOSTNAMELEN+1];
extern int rdonly;

void get_cwd_path (struct Cns_srv_thread_info *thip,
                   u_signed64 cwd,
                   char *cwdpath)
{
  char path[CA_MAXPATHLEN + 1];
  char *p = path;
  cwdpath[0] = path[0] = '\0';
  if (cwd == 2) {
    p = "/";
  } else if (cwd > 0) {
    Cns_getpath_by_fileid (&thip->dbfd, cwd, &p);
  }
  if (path[0] != '\0') {
    cwdpath = p;
  }
}

void get_client_actual_id (struct Cns_srv_thread_info *thip)
{
  struct passwd *pw;

  /* If security is enabled, ignore the uid and gid transmitted by the client
   * and use the authenticated values.
   */
  if (thip->reqinfo.secure) {
    thip->reqinfo.uid      = thip->Csec_uid;
    thip->reqinfo.gid      = thip->Csec_gid;
    thip->reqinfo.username = thip->Csec_auth_id;
  } else {
    if ((pw = Cgetpwuid(thip->reqinfo.uid)) == NULL) {
      thip->reqinfo.username = "UNKNOWN";
    } else {
      thip->reqinfo.username = pw->pw_name;
    }
  }
}

int marshall_DIRX (char **sbpp,
                   int magic,
                   struct Cns_file_metadata *fmd_entry)
{
  char *sbp = *sbpp;

  marshall_HYPER (sbp, fmd_entry->fileid);
  if (magic >= CNS_MAGIC2)
    marshall_STRING (sbp, fmd_entry->guid);
  marshall_WORD (sbp, fmd_entry->filemode);
  marshall_LONG (sbp, fmd_entry->nlink);
  marshall_LONG (sbp, fmd_entry->uid);
  marshall_LONG (sbp, fmd_entry->gid);
  marshall_HYPER (sbp, fmd_entry->filesize);
  marshall_TIME_T (sbp, fmd_entry->atime);
  marshall_TIME_T (sbp, fmd_entry->mtime);
  marshall_TIME_T (sbp, fmd_entry->ctime);
  marshall_WORD (sbp, fmd_entry->fileclass);
  marshall_BYTE (sbp, fmd_entry->status);
  if (magic >= CNS_MAGIC2) {
    marshall_STRING (sbp, fmd_entry->csumtype);
    marshall_STRING (sbp, fmd_entry->csumvalue);
  }
  marshall_STRING (sbp, fmd_entry->name);
  *sbpp = sbp;
  return (0);
}

int marshall_DIRXT (char **sbpp,
                    int magic,
                    struct Cns_file_metadata *fmd_entry,
                    struct Cns_seg_metadata *smd_entry)
{
  char *sbp = *sbpp;

  marshall_HYPER (sbp, fmd_entry->parent_fileid);
  if (magic >= CNS_MAGIC3)
    marshall_HYPER (sbp, smd_entry->s_fileid);
  marshall_WORD (sbp, smd_entry->copyno);
  marshall_WORD (sbp, smd_entry->fsec);
  marshall_HYPER (sbp, smd_entry->segsize);
  marshall_LONG (sbp, smd_entry->compression);
  marshall_BYTE (sbp, smd_entry->s_status);
  marshall_STRING (sbp, smd_entry->vid);
  if (magic >= CNS_MAGIC4) {
    marshall_STRING (sbp, smd_entry->checksum_name);
    marshall_LONG (sbp, smd_entry->checksum);
  }
  if (magic >= CNS_MAGIC2)
    marshall_WORD (sbp, smd_entry->side);
  marshall_LONG (sbp, smd_entry->fseq);
  marshall_OPAQUE (sbp, smd_entry->blockid, 4);
  marshall_STRING (sbp, fmd_entry->name);
  *sbpp = sbp;
  return (0);
}

/* Cns_srv_aborttrans - abort transaction */

int Cns_srv_aborttrans(char *req_data,
                       struct Cns_srv_thread_info *thip,
                       struct Cns_srv_request_info *reqinfo)
{
  char *func = "aborttrans";
  char *rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  (void) Cns_abort_tr (&thip->dbfd);
  RETURN (0);
}

/* Cns_srv_access - check accessibility of a file/directory */

int Cns_srv_access(char *req_data,
                   struct Cns_srv_thread_info *thip,
                   struct Cns_srv_request_info *reqinfo)
{
  int amode;
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  char *func = "access";
  mode_t mode;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);
  unmarshall_LONG (rbp, amode);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\" Amode=%o",
           cwdpath, path, amode);

  if (amode & ~(R_OK | W_OK | X_OK | F_OK))
    RETURNQ (EINVAL);

  /* check parent directory components for search permission and get basename
   * entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, NULL, NULL, &fmd_entry, NULL,
                     CNS_MUST_EXIST))
    RETURNQ (serrno);

  /* Record the fileid being processed */
  reqinfo->fileid = fmd_entry.fileid;

  /* Check permissions for basename */
  if (amode == F_OK)
    RETURNQ (0);
  mode = (amode & (R_OK|W_OK|X_OK)) << 6;
  if (Cns_chkentryperm (&fmd_entry, mode, reqinfo->uid, reqinfo->gid,
                        reqinfo->clienthost))
    RETURNQ (EACCES);
  RETURNQ (0);
}

/* Cns_srv_chclass - change class on directory */

int Cns_srv_chclass(char *req_data,
                    struct Cns_srv_thread_info *thip,
                    struct Cns_srv_request_info *reqinfo)
{
  char class_name[CA_MAXCLASNAMELEN+1];
  int classid;
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  char *func = "chclass";
  struct Cns_class_metadata new_class_entry;
  Cns_dbrec_addr new_rec_addrc;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  int count;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_LONG (rbp, classid);
  if (unmarshall_STRINGN (rbp, class_name, CA_MAXCLASNAMELEN+1))
    RETURN (EINVAL);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct logging paramaters */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf,
           "Cwd=\"%s\" Path=\"%s\" ClassId=%d ClassName=\"%s\"",
           cwdpath, path, classid, class_name);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check parent directory components for search permission and get/lock
   * basename entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, NULL, NULL, &fmd_entry, &rec_addr,
                     CNS_MUST_EXIST))
    RETURN (serrno);

  /* Record the fileid being processed */
  reqinfo->fileid = fmd_entry.fileid;

  /* Check if the class is valid? */
  if (classid > 0) {
    if (Cns_get_class_by_id (&thip->dbfd, classid, &new_class_entry, 0,
                             &new_rec_addrc)) {
      if (serrno == ENOENT) {
        sendrep (thip->s, MSG_ERR, "No such class\n");
        RETURN (EINVAL);
      } else {
        RETURN (serrno);
      }
    }
    if (*class_name && strcmp (class_name, new_class_entry.name))
      RETURN (EINVAL);
  } else {
    if (Cns_get_class_by_name (&thip->dbfd, class_name, &new_class_entry, 0,
                               &new_rec_addrc)) {
      if (serrno == ENOENT) {
        sendrep (thip->s, MSG_ERR, "No such class\n");
        RETURN (EINVAL);
      } else {
        RETURN (serrno);
      }
    }
  }

  /* Check if the user is authorized to chclass this entry */
  if (fmd_entry.filemode & S_IFDIR) {

    /* Must be the owner, GRP_ADMIN or ADMIN for directory chclass */
    if ((reqinfo->uid != fmd_entry.uid) &&
        Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                    localhost, P_GRP_ADMIN) &&
        Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                    localhost, P_ADMIN))
      RETURN (EPERM);
  } else if (fmd_entry.filemode & S_IFREG) {

    /* Must be an ADMIN for file chclass */
    if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                    localhost, P_ADMIN))
      RETURN (ENOTDIR);
  } else {
    RETURN (ENOTDIR);
  }

  /* Update entries */
  if (fmd_entry.fileclass != new_class_entry.classid) {

    /* If the file has segments make sure the new fileclass allows them! */
    if (Cns_get_smd_copy_count_by_pfid
        (&thip->dbfd, fmd_entry.fileid, &count, 1))
      RETURN (serrno);
    if (count && (new_class_entry.nbcopies == 0))
      RETURN (ENSCLASSNOSEGS); /* File class does not allow a copy on tape */
    if (count > new_class_entry.nbcopies)
      RETURN (ENSTOOMANYSEGS); /* Too many copies on tape */

    fmd_entry.fileclass = new_class_entry.classid;
    fmd_entry.ctime = time (0);
    if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &fmd_entry))
      RETURN (serrno);
  }
  RETURN (0);
}

/* Cns_srv_chdir - change current working directory */

int Cns_srv_chdir(char *req_data,
                  struct Cns_srv_thread_info *thip,
                  struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  struct Cns_file_metadata direntry;
  char *func = "chdir";
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  char repbuf[8];
  char *sbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);

  /* Construct logging paramaters */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  /* Check parent directory components for search permission and get basename
   * entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, NULL, NULL, &direntry, NULL,
                     CNS_MUST_EXIST))
    RETURN (serrno);

  if ((direntry.filemode & S_IFDIR) == 0)
    RETURN (ENOTDIR);
  if (Cns_chkentryperm (&direntry, S_IEXEC, reqinfo->uid, reqinfo->gid,
                        reqinfo->clienthost))
    RETURN (EACCES);

  /* Return directory fileid */
  sbp = repbuf;
  marshall_HYPER (sbp, direntry.fileid);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/* Cns_srv_chmod - change file/directory permissions */

int Cns_srv_chmod(char *req_data,
                  struct Cns_srv_thread_info *thip,
                  struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  char *func = "chmod";
  mode_t mode;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addr;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_LONG (rbp, mode);
  /* mode must be within 0 and 07777 */
  if (mode > 4095) RETURN (EINVAL);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\" Mode=%o",
           cwdpath, path, mode);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check parent directory components for search permission and get/lock
   * basename entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, NULL, NULL, &fmd_entry, &rec_addr,
                     CNS_MUST_EXIST))
    RETURN (serrno);

  /* Record the fileid being processed */
  reqinfo->fileid = fmd_entry.fileid;

  /* Check if the user is authorized to chmod this entry */
  if ((reqinfo->uid != fmd_entry.uid) &&
      Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                  localhost, P_GRP_ADMIN) &&
      Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                  localhost, P_ADMIN))
    RETURN (EPERM);
  if ((fmd_entry.filemode & S_IFDIR) == 0 && reqinfo->uid != 0)
    mode &= ~S_ISVTX;
  if (reqinfo->gid != fmd_entry.gid && reqinfo->uid != 0)
    mode &= ~S_ISGID;

  /* Update entry */
  fmd_entry.filemode = (fmd_entry.filemode & S_IFMT) | (mode & ~S_IFMT);
  if (*fmd_entry.acl)
    Cns_acl_chmod (&fmd_entry);
  fmd_entry.ctime = time (0);
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &fmd_entry))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_chown - change owner and group of a file or a directory, and update segments as well */

int Cns_srv_chown(char *req_data,
                  struct Cns_srv_thread_info *thip,
                  struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  int found;
  char *func = "chown";
  struct group *gr;
  char **membername;
  int need_p_admin = 0;
  int need_p_expt_admin = 0;
  gid_t new_gid;
  uid_t new_uid;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  struct passwd *pw;
  char *rbp;
  Cns_dbrec_addr rec_addr;
  Cns_dbrec_addr rec_addrs; /* Segment record address */
  int bof, c;
  struct Cns_seg_metadata smd_entry;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_LONG (rbp, new_uid);
  unmarshall_LONG (rbp, new_gid);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\" NewUid=%d NewGid=%d",
           cwdpath, path, new_uid, new_gid);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check parent directory components for search permission and get/lock
   * basename entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, NULL, NULL, &fmd_entry, &rec_addr,
                     CNS_MUST_EXIST))
    RETURN (serrno);

  /* Record the fileid being processed */
  reqinfo->fileid = fmd_entry.fileid;

  /* Check if the user is authorized to change ownership this entry */
  if (fmd_entry.uid != new_uid && (int)new_uid != -1) {
    if (reqinfo->gid != fmd_entry.gid)
      need_p_admin = 1;
    else if ((pw = Cgetpwuid (new_uid)) == NULL)
      need_p_admin = 1;
    else if (pw->pw_gid == reqinfo->gid) /* New owner belongs to same group */
      need_p_expt_admin = 1;
    else
      need_p_admin = 1;
  }
  if (fmd_entry.gid != new_gid && (int)new_gid != -1) {
    if (reqinfo->uid != fmd_entry.uid) {
      need_p_admin = 1;
    } else if ((pw = Cgetpwuid (reqinfo->uid)) == NULL) {
      need_p_admin = 1;
    } else if ((gr = Cgetgrgid (new_gid)) == NULL) {
      need_p_admin = 1;
    } else {
      if (new_gid == pw->pw_gid) {
        found = 1;
      } else {
        found = 0;
        membername = gr->gr_mem;
        while (*membername) {
          if (strcmp (pw->pw_name, *membername) == 0) {
            found = 1;
            break;
          }
          membername++;
        }
      }
      if (!found)
        need_p_admin = 1;
    }
  }
  if (need_p_admin) {
    if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                    localhost, P_ADMIN))
      RETURN (EPERM);
  } else if (need_p_expt_admin) {
    if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                    localhost, P_ADMIN) &&
        Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                    localhost, P_GRP_ADMIN))
      RETURN (EPERM);
  }

  /* Update file entry */
  if ((int)new_uid != -1)
    fmd_entry.uid = new_uid;
  if ((int)new_gid != -1)
    fmd_entry.gid = new_gid;
  if (*fmd_entry.acl)
    Cns_acl_chown (&fmd_entry);
  fmd_entry.ctime = time (0);
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &fmd_entry))
    RETURN (serrno);
  /* Update segments if any */
  bof = 1;
  while ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof, fmd_entry.fileid,
                                   &smd_entry, 1, &rec_addrs, 0)) == 0) {
    smd_entry.gid = new_gid;
    if (Cns_update_smd_entry (&thip->dbfd, &rec_addrs, &smd_entry))
      RETURN (serrno);
    bof = 0;
  }
  (void) Cns_get_smd_by_pfid (&thip->dbfd, bof, fmd_entry.fileid,
                              &smd_entry, 1, &rec_addrs, 1);
  if (c < 0)
    RETURN (serrno);

  RETURN (0);
}

/* Cns_delete_segment_metadata - drop segment metadata entry logging all attributes */

int Cns_delete_segment_metadata(struct Cns_srv_thread_info *thip,
                                struct Cns_seg_metadata *smd_entry,
                                Cns_dbrec_addr *rec_addr)
{
  nslogit("MSG=\"Unlinking segment\" REQID=%s NSHOSTNAME=%s NSFILEID=%llu "
          "CopyNo=%d Fsec=%d SegmentSize=%llu Compression=%d Status=\"%c\" "
          "TPVID=%s Side=%d Fseq=%d BlockId=\"%02x%02x%02x%02x\" "
          "ChecksumType=\"%s\" ChecksumValue=\"%lx\"",
          thip->reqinfo.requuid, nshostname, smd_entry->s_fileid,
          smd_entry->copyno, smd_entry->fsec, smd_entry->segsize,
          smd_entry->compression, smd_entry->s_status, smd_entry->vid,
          smd_entry->side, smd_entry->fseq, smd_entry->blockid[0],
          smd_entry->blockid[1], smd_entry->blockid[2], smd_entry->blockid[3],
          smd_entry->checksum_name, smd_entry->checksum);
  if (Cns_delete_smd_entry (&thip->dbfd, rec_addr))
    return (serrno);
  return (0);
}

/* Cns_delete_file_metadata - drop file metadata entry logging all attributes */

int Cns_delete_file_metadata(struct Cns_srv_thread_info *thip,
                             struct Cns_file_metadata *fmd_entry,
                             Cns_dbrec_addr *rec_addr)
{
  if (fmd_entry->filemode & S_IFREG) {
    nslogit("MSG=\"Unlinking file\" REQID=%s NSHOSTNAME=%s NSFILEID=%llu "
            "ParentFileId=%llu Guid=\"%s\" Name=\"%s\" FileMode=%d Nlink=%d "
            "OwnerUid=%d OwnerGid=%d FileSize=%d Atime=%lld Mtime=%lld "
            "Ctime=%lld FileClass=%d Status=\"%c\" ChecksumType=\"%s\" "
            "ChecksumValue=\"%s\"",
            thip->reqinfo.requuid, nshostname, fmd_entry->fileid,
            fmd_entry->parent_fileid, fmd_entry->guid, fmd_entry->name,
            fmd_entry->filemode, fmd_entry->nlink, fmd_entry->uid,
            fmd_entry->gid, fmd_entry->filesize,
            (long long int)fmd_entry->atime, (long long int) fmd_entry->mtime,
            (long long int)fmd_entry->ctime, fmd_entry->fileclass,
            fmd_entry->status, fmd_entry->csumtype, fmd_entry->csumvalue);
  }
  if (Cns_delete_fmd_entry (&thip->dbfd, rec_addr))
    return (serrno);
  return (0);
}

/* Cns_delete_segs - delete the segments associated to a file */

int Cns_delete_segs(struct Cns_srv_thread_info *thip,
                    struct Cns_file_metadata *filentry,
                    int copyno)
{
  struct Cns_seg_metadata smd_entry;
  Cns_dbrec_addr rec_addrs;
  int       found = 0;
  int       bof = 1;
  int       c;

  /* Loop over the segments for the file, filtering by copy number if
   * applicable.
   */
  while ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry->fileid,
                                   &smd_entry, 1, &rec_addrs, 0)) == 0) {
    bof = 0;
    if (copyno && (smd_entry.copyno != copyno)) {
      continue;
    }
    found++;
    if (Cns_delete_segment_metadata (thip, &smd_entry, &rec_addrs))
      return (serrno);
  }
  (void) Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry->fileid,
                              &smd_entry, 1, &rec_addrs, 1);
  if (c < 0)
    return (serrno);
  if (!found) {
    serrno = SEENTRYNFND;
    return (serrno);
  }
  return (0);
}

/* Cns_srv_creat - create a file entry */

int Cns_srv_creat(int magic,
                  char *req_data,
                  struct Cns_srv_thread_info *thip,
                  struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char *func = "creat";
  char guid[CA_MAXGUIDLEN+1];
  mode_t mask;
  mode_t mode;
  struct Cns_file_metadata parent_dir;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addr;  /* File record address */
  Cns_dbrec_addr rec_addrp; /* Parent record address */
  char repbuf[8];
  char *sbp;
  uuid_t uuid;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_WORD (rbp, mask);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_LONG (rbp, mode);
  /* mode must be within 0 and 07777 */
  if (mode > 4095) RETURN (EINVAL);
  if (magic >= CNS_MAGIC2) {
    if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN+1))
      RETURN (EINVAL);
    if (uuid_parse (guid, uuid) < 0)
      RETURN (EINVAL);
  } else
    *guid = '\0';

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf,
           "Mask=%o Cwd=\"%s\" Path=\"%s\" Mode=%o Guid=\"%s\"",
           mask, cwdpath, path, mode, guid);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check parent directory components for write/search permission and get/lock
   * basename entry if it exists
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, &parent_dir, &rec_addrp, &filentry,
                     &rec_addr, 0))
    RETURN (serrno);
  reqinfo->fileid = filentry.fileid;

  if (*filentry.name == '/') /* Cns_creat / */
    RETURN (EISDIR);

  if (filentry.fileid) { /* File exists */
    if (filentry.filemode & S_IFDIR)
      RETURN (EISDIR);
    if (strcmp (filentry.guid, guid))
      RETURN (EEXIST);

    /* Check write permission in basename entry */
    if (Cns_chkentryperm (&filentry, S_IWRITE, reqinfo->uid, reqinfo->gid,
                          reqinfo->clienthost))
      RETURN (EACCES);

    /* Delete file segments if any */
    if (Cns_delete_segs(thip, &filentry, 0) != 0)
      if (serrno != SEENTRYNFND)
        RETURN (serrno);

    /* Update basename entry */
    if (*guid)
      strcpy (filentry.guid, guid);
    filentry.filesize = 0;
    filentry.mtime = time (0);
    filentry.ctime = filentry.mtime;
    filentry.stagertime = 0.0;
    filentry.status = '-';
    if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
      RETURN (serrno);

    /* Amend logging paramaters */
    sprintf (reqinfo->logbuf + strlen(reqinfo->logbuf), " Truncated=\"True\"");
  } else { /* Must create the file */
    if (parent_dir.fileclass <= 0)
      RETURN (EINVAL);
    if (Cns_unique_id (&thip->dbfd, &filentry.fileid) < 0)
      RETURN (serrno);
    reqinfo->fileid = filentry.fileid;

    /* parent_fileid and name have been set by Cns_parsepath */
    strcpy (filentry.guid, guid);
    filentry.filemode = S_IFREG | ((mode & ~S_IFMT) & ~mask);
    filentry.nlink = 1;
    filentry.uid = reqinfo->uid;
    if (parent_dir.filemode & S_ISGID) {
      filentry.gid = parent_dir.gid;
      if (reqinfo->gid == filentry.gid)
        filentry.filemode |= S_ISGID;
    } else
      filentry.gid = reqinfo->gid;
    filentry.atime = time (0);
    filentry.mtime = filentry.atime;
    filentry.ctime = filentry.atime;
    filentry.fileclass = parent_dir.fileclass;
    filentry.status = '-';
    if (*parent_dir.acl)
      Cns_acl_inherit (&parent_dir, &filentry, mode);

    /* Write new file entry */
    if (Cns_insert_fmd_entry (&thip->dbfd, &filentry))
      RETURN (serrno);

    /* Update parent directory entry */
    parent_dir.nlink++;
    parent_dir.mtime = time (0);
    parent_dir.ctime = parent_dir.mtime;
    if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
      RETURN (serrno);

    /* Amend log message */
    sprintf (reqinfo->logbuf + strlen(reqinfo->logbuf), " NewFile=\"True\"");
  }
  sbp = repbuf;
  marshall_HYPER (sbp, filentry.fileid);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/* Cns_srv_delcomment - delete a comment associated with a file/directory */

int Cns_srv_delcomment(char *req_data,
                       struct Cns_srv_thread_info *thip,
                       struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char *func = "delcomment";
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addru;
  struct Cns_user_metadata umd_entry;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check parent directory components for search permission and get basename
   * entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, NULL, NULL, &filentry, NULL,
                     CNS_MUST_EXIST))
    RETURN (serrno);

  /* Record the fileid being processed */
  reqinfo->fileid = filentry.fileid;

  /* Check if the user is authorized to delete the comment on this entry */
  if (reqinfo->uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, reqinfo->uid, reqinfo->gid,
                        reqinfo->clienthost))
    RETURN (EACCES);

  /* Delete the comment if it exists */
  if (Cns_get_umd_by_fileid (&thip->dbfd, filentry.fileid, &umd_entry, 1,
                             &rec_addru))
    RETURN (serrno);
  if (Cns_delete_umd_entry (&thip->dbfd, &rec_addru))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_delete - logically remove a file entry */

int Cns_srv_delete(char *req_data,
                   struct Cns_srv_thread_info *thip,
                   struct Cns_srv_request_info *reqinfo)
{
  int bof = 1;
  int c;
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char *func = "delete";
  struct Cns_file_metadata parent_dir;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addr; /* File record address */
  Cns_dbrec_addr rec_addrp; /* Parent record address */
  Cns_dbrec_addr rec_addrs; /* Segment record address */
  struct Cns_seg_metadata smd_entry;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check parent directory components for write/search permission and
   * get/lock basename entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, &parent_dir, &rec_addrp, &filentry,
                     &rec_addr, CNS_MUST_EXIST|CNS_NOFOLLOW))
    RETURN (serrno);

  /* Record the fileid being processed */
  reqinfo->fileid = filentry.fileid;

  if (*filentry.name == '/') /* Cns_delete / */
    RETURN (EINVAL);

  if (filentry.filemode & S_IFDIR)
    RETURN (EPERM);

  /* If the parent has the sticky bit set, the user must own the file or the
   * parent or the basename entry must have write permission
   */
  if (parent_dir.filemode & S_ISVTX &&
      reqinfo->uid != parent_dir.uid && reqinfo->uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, reqinfo->uid, reqinfo->gid,
                        reqinfo->clienthost))
    RETURN (EACCES);

  /* Mark file segments if any as logically deleted */
  bof = 1;
  while ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid,
                                   &smd_entry, 1, &rec_addrs, 0)) == 0) {
    smd_entry.s_status = 'D';
    if (Cns_update_smd_entry (&thip->dbfd, &rec_addrs, &smd_entry))
      RETURN (serrno);
    bof = 0;
  }
  (void) Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid, &smd_entry,
                              1, &rec_addrs, 1);
  if (c < 0)
    RETURN (serrno);

  /* Mark file entry as logically deleted */
  filentry.status = 'D';
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
    RETURN (serrno);

  /* Update parent directory entry */
  parent_dir.mtime = time (0);
  parent_dir.ctime = parent_dir.mtime;
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_deleteclass - delete a file class definition */

int Cns_srv_deleteclass(char *req_data,
                        struct Cns_srv_thread_info *thip,
                        struct Cns_srv_request_info *reqinfo)
{
  int bol = 1;
  struct Cns_class_metadata class_entry;
  char class_name[CA_MAXCLASNAMELEN+1];
  int classid;
  char *func = "deleteclass";
  char *rbp;
  Cns_dbrec_addr rec_addr;
  Cns_dbrec_addr rec_addrt;
  struct Cns_tp_pool tppool_entry;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_LONG (rbp, classid);
  if (unmarshall_STRINGN (rbp, class_name, CA_MAXCLASNAMELEN+1))
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "ClassId=%d ClassName=\"%s\"", classid, class_name);

  /* Check if the user is authorized to delete a class */
  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                  localhost, P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  if (classid > 0) {
    if (Cns_get_class_by_id (&thip->dbfd, classid, &class_entry,
                             1, &rec_addr))
      RETURN (serrno);
    if (*class_name && strcmp (class_name, class_entry.name))
      RETURN (EINVAL);
  } else {
    if (Cns_get_class_by_name (&thip->dbfd, class_name, &class_entry,
                               1, &rec_addr))
      RETURN (serrno);
  }
  while (Cns_get_tppool_by_cid (&thip->dbfd, bol, class_entry.classid,
                                &tppool_entry, 1, &rec_addrt, 0) == 0) {
    if (Cns_delete_tppool_entry (&thip->dbfd, &rec_addrt))
      RETURN (serrno);
    bol = 0;
  }
  (void) Cns_get_tppool_by_cid (&thip->dbfd, bol, class_entry.classid,
                                &tppool_entry, 1, &rec_addrt, 1);
  if (Cns_delete_class_entry (&thip->dbfd, &rec_addr))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_delsegbycopyno - delete file segments by copno */

int Cns_srv_delsegbycopyno(char *req_data,
                           struct Cns_srv_thread_info *thip,
                           struct Cns_srv_request_info *reqinfo)
{
  char       *func = "delsegbycopyno";
  char       *rbp;
  char       path[CA_MAXPATHLEN+1];
  char       cwdpath[CA_MAXPATHLEN+1];
  u_signed64 cwd;
  u_signed64 fileid = 0;
  int        copyno = 0;
  int        bof = 1;
  int        c;
  struct Cns_file_metadata fmd_entry;
  struct Cns_seg_metadata  smd_entry;
  Cns_dbrec_addr rec_addr;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (fileid > 0)
    reqinfo->fileid = fileid;
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_LONG (rbp, copyno);
  if (copyno < 0)
    RETURN (EINVAL);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\" CopyNo=%d",
           cwdpath, path, copyno);

  /* Check if the user is authorized to delete a segment by copy number */
  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                  localhost, P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  if (fileid) {

    /* Get/lock basename entry */
    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &fmd_entry, 1, &rec_addr))
      RETURN (serrno);
  } else {

    /* Check parent directory components for search permission and get/lock
     * basename entry
     */
    if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                       reqinfo->clienthost, NULL, NULL, &fmd_entry, &rec_addr,
                       CNS_MUST_EXIST))
      RETURN (serrno);

    /* Record the fileid being processed */
    reqinfo->fileid = fmd_entry.fileid;
  }

  /* Check if the entry is a regular file */
  if (fmd_entry.filemode & S_IFDIR)
    RETURN (EISDIR);
  if ((fmd_entry.filemode & S_IFREG) != S_IFREG)
    RETURN (SEINTERNAL);
  if (*fmd_entry.name == '/')
    RETURN (SEINTERNAL);

  /* Delete file segments */
  if (Cns_delete_segs(thip, &fmd_entry, copyno) != 0)
    RETURN (serrno);

  /* Count the number of file segments left */
  while ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof, fmd_entry.fileid,
                                   &smd_entry, 0, NULL, 0)) == 0) {
    bof = 0;
  }
  (void) Cns_get_smd_by_pfid (&thip->dbfd, bof, fmd_entry.fileid,
                              &smd_entry, 0, NULL, 1);
  if (c < 0)
    RETURN (serrno);

  /* If the file has no segments remove the 'm' status */
  if (bof) {
    fmd_entry.status = '-';
    if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &fmd_entry))
      RETURN (serrno);
  }

  RETURN (0);
}

/* Cns_srv_du - summarize file space usage */

int compute_du4dir (struct Cns_srv_thread_info *thip,
                    struct Cns_file_metadata *direntry,
                    int Lflag,
                    uid_t uid,
                    gid_t gid,
                    const char *clienthost,
                    u_signed64 *nbbytes,
                    u_signed64 *nbentries)
{
  int bod = 1;
  int c;
  struct dirlist {
    u_signed64 fileid;
    struct dirlist *next;
  };
  struct dirlist *dlc;  /* Pointer to current directory in the list */
  struct dirlist *dlf = NULL; /* Pointer to first directory in the list */
  struct dirlist *dll;  /* Pointer to last directory in the list */
  struct Cns_file_metadata fmd_entry;

  if (Cns_chkentryperm (direntry, S_IREAD|S_IEXEC, uid, gid, clienthost))
    return (EACCES);
  while ((c = Cns_get_fmd_by_pfid (&thip->dbfd, bod, direntry->fileid,
                                   &fmd_entry, 1, 0)) == 0) {
    if (fmd_entry.filemode & S_IFDIR) {
      if ((dlc = (struct dirlist *)
           malloc (sizeof(struct dirlist))) == NULL) {
        serrno = errno;
        c = -1;
        break;
      }
      dlc->fileid = fmd_entry.fileid;
      dlc->next = 0;
      if (dlf == NULL)
        dlf = dlc;
      else
        dll->next = dlc;
      dll = dlc;
    } else { /* Regular file */
      *nbbytes += fmd_entry.filesize;
      *nbentries += 1;
    }
    bod = 0;
  }
  (void) Cns_get_fmd_by_pfid (&thip->dbfd, bod, direntry->fileid,
                              &fmd_entry, 1, 1);
  while (dlf) {
    if (c > 0 && Cns_get_fmd_by_fileid (&thip->dbfd, dlf->fileid,
                                        &fmd_entry, 0, NULL) == 0)
      (void) compute_du4dir (thip, &fmd_entry, Lflag, uid, gid,
                             clienthost, nbbytes, nbentries);
    dlc = dlf;
    dlf = dlf->next;
    free (dlc);
  }
  return (c < 0 ? serrno : 0);
}

int Cns_srv_du(char *req_data,
               struct Cns_srv_thread_info *thip,
               struct Cns_srv_request_info *reqinfo)
{
  int c;
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  char *func = "du";
  int Lflag;
  u_signed64 nbbytes = 0;
  u_signed64 nbentries = 0;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  char repbuf[16];
  char *sbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);
  unmarshall_WORD (rbp, Lflag);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  if (! cwd && *path == 0)
    RETURNQ (ENOENT);
  if (! cwd && *path != '/')
    RETURNQ (EINVAL);

  /* Check parent directory components for search permission and get basename
   * entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, NULL, NULL, &fmd_entry, NULL,
                     CNS_MUST_EXIST))
    RETURNQ (serrno);

  /* Record the fileid being processed */
  reqinfo->fileid = fmd_entry.fileid;

  if (fmd_entry.filemode & S_IFDIR) {
    if ((c = compute_du4dir (thip, &fmd_entry, Lflag, reqinfo->uid,
                             reqinfo->gid, reqinfo->clienthost, &nbbytes,
                             &nbentries)))
      RETURNQ (c);
  } else { /* Regular file */
    nbbytes += fmd_entry.filesize;
    nbentries += 1;
  }
  sbp = repbuf;
  marshall_HYPER (sbp, nbbytes);
  marshall_HYPER (sbp, nbentries);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_endsess - end session */

int Cns_srv_endsess(char *req_data,
                    struct Cns_srv_thread_info *thip,
                    struct Cns_srv_request_info *reqinfo)
{
  char *func = "endsess";
  char *rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  RETURN (0);
}

/* Cns_srv_endtrans - end transaction mode */

int Cns_srv_endtrans(char *req_data,
                     struct Cns_srv_thread_info *thip,
                     struct Cns_srv_request_info *reqinfo)
{
  char *func = "endtrans";
  char *rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  (void) Cns_end_tr (&thip->dbfd);
  RETURN (0);
}

/* Cns_srv_enterclass - define a new file class */

int Cns_srv_enterclass(char *req_data,
                       struct Cns_srv_thread_info *thip,
                       struct Cns_srv_request_info *reqinfo)
{
  struct Cns_class_metadata class_entry;
  char *func = "enterclass";
  int i;
  int nbtppools;
  char *rbp;
  struct Cns_tp_pool tppool_entry;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  memset ((char *) &class_entry, 0, sizeof(class_entry));
  unmarshall_LONG (rbp, class_entry.classid);
  if (unmarshall_STRINGN (rbp, class_entry.name, CA_MAXCLASNAMELEN+1))
    RETURN (EINVAL);
  unmarshall_LONG (rbp, class_entry.uid);
  unmarshall_LONG (rbp, class_entry.gid);
  unmarshall_LONG (rbp, class_entry.min_filesize);
  unmarshall_LONG (rbp, class_entry.max_filesize);
  unmarshall_LONG (rbp, class_entry.flags);
  unmarshall_LONG (rbp, class_entry.maxdrives);
  unmarshall_LONG (rbp, class_entry.max_segsize);
  unmarshall_LONG (rbp, class_entry.migr_time_interval);
  unmarshall_LONG (rbp, class_entry.mintime_beforemigr);
  unmarshall_LONG (rbp, class_entry.nbcopies);
  unmarshall_LONG (rbp, class_entry.retenp_on_disk);
  unmarshall_LONG (rbp, nbtppools);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "ClassId=%d ClassName=\"%s\" NbCopies=%d",
           class_entry.classid, class_entry.name, class_entry.nbcopies);

  /* Check if the user is authorized to enter a new file class */
  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                  localhost, P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  if (class_entry.classid <= 0 || *class_entry.name == '\0')
    RETURN (EINVAL);
  if (class_entry.max_filesize < class_entry.min_filesize)
    RETURN (EINVAL);
  (void) Cns_start_tr (&thip->dbfd);

  if (Cns_insert_class_entry (&thip->dbfd, &class_entry))
    RETURN (serrno);

  /* Receive/store tppool entries */
  tppool_entry.classid = class_entry.classid;
  for (i = 0; i < nbtppools; i++) {
    if (unmarshall_STRINGN (rbp, tppool_entry.tape_pool, CA_MAXPOOLNAMELEN+1))
      RETURN (EINVAL);
    if (Cns_insert_tppool_entry (&thip->dbfd, &tppool_entry))
      RETURN (serrno);
  }
  RETURN (0);
}

/* Cns_srv_getacl - get the Access Control List for a file/directory */

int Cns_srv_getacl(char *req_data,
                   struct Cns_srv_thread_info *thip,
                   struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  char *func = "getacl";
  char *iacl;
  int nentries = 0;
  char *p;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  char repbuf[REPBUFSZ];
  char *sbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  /* Check parent directory components for search permission and get basename
   * entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, NULL, NULL, &fmd_entry, NULL,
                     CNS_MUST_EXIST))
    RETURNQ (serrno);

  /* Record the fileid being processed */
  reqinfo->fileid = fmd_entry.fileid;

  sbp = repbuf;
  marshall_WORD (sbp, nentries);  /* Will be updated */
  if (*fmd_entry.acl == 0) {
    marshall_BYTE (sbp, CNS_ACL_USER_OBJ);
    marshall_LONG (sbp, fmd_entry.uid);
    marshall_BYTE (sbp, fmd_entry.filemode >> 6 & 07);
    nentries++;
    marshall_BYTE (sbp, CNS_ACL_GROUP_OBJ);
    marshall_LONG (sbp, fmd_entry.gid);
    marshall_BYTE (sbp, fmd_entry.filemode >> 3 & 07);
    nentries++;
    marshall_BYTE (sbp, CNS_ACL_OTHER);
    marshall_LONG (sbp, 0);
    marshall_BYTE (sbp, fmd_entry.filemode & 07);
    nentries++;
  } else {
    for (iacl = fmd_entry.acl; iacl; iacl = p) {
      if ((p = strchr (iacl, ',')))
        p++;
      marshall_BYTE (sbp, *iacl - '@');
      marshall_LONG (sbp, atoi (iacl + 2));
      marshall_BYTE (sbp, *(iacl + 1) - '0');
      nentries++;
    }
  }
  p = repbuf;
  marshall_WORD (p, nentries);  /* Update nentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_getcomment - get the comment associated with a file/directory */

int Cns_srv_getcomment(char *req_data,
                       struct Cns_srv_thread_info *thip,
                       struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char *func = "getcomment";
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  char repbuf[CA_MAXCOMMENTLEN+1];
  char *sbp;
  struct Cns_user_metadata umd_entry;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  /* Check parent directory components for search permission and get basename
   * entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, NULL, NULL, &filentry, NULL,
                     CNS_MUST_EXIST))
    RETURNQ (serrno);

  /* Record the fileid being processed */
  reqinfo->fileid = filentry.fileid;

  /* Check if the user is authorized to get the comment for this entry */
  if (reqinfo->uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IREAD, reqinfo->uid, reqinfo->gid,
                        reqinfo->clienthost))
    RETURNQ (EACCES);

  /* Get the comment if it exists */
  if (Cns_get_umd_by_fileid (&thip->dbfd, filentry.fileid, &umd_entry, 0, NULL))
    RETURNQ (serrno);

  sbp = repbuf;
  marshall_STRING (sbp, umd_entry.comments);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_getlinks - get the link entries associated with a given file */

int Cns_srv_getlinks(char *req_data,
                     struct Cns_srv_thread_info *thip,
                     struct Cns_srv_request_info *reqinfo)
{
  int bol = 1;
  int c;
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  char *func = "getlinks";
  char guid[CA_MAXGUIDLEN+1];
  struct Cns_symlinks lnk_entry;
  int n;
  char *p;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  char repbuf[REPBUFSZ];
  char *sbp = repbuf;
  char tmp_path[CA_MAXPATHLEN+1];

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);
  if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN + 1) < 0)
    RETURNQ (EINVAL);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\" Guid=\"%s\"",
           cwdpath, path, guid);

  if (*path) {

    /* Check parent directory components for search permission and get basename
     * entry
     */
    if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                       reqinfo->clienthost, NULL, NULL, &fmd_entry, NULL,
                       CNS_MUST_EXIST))
      RETURNQ (serrno);
    if (*guid && strcmp (guid, fmd_entry.guid))
      RETURNQ (EINVAL);
  } else {
    if (! *guid)
      RETURNQ (ENOENT);

    /* Get basename entry */
    if (Cns_get_fmd_by_guid (&thip->dbfd, guid, &fmd_entry, 0, NULL))
      RETURNQ (serrno);

    /* Do not check parent directory components for search permission as
     * symlinks can anyway point directly at a file
     */
  }

  /* Record the fileid being processed */
  reqinfo->fileid = fmd_entry.fileid;

  if ((fmd_entry.filemode & S_IFMT) == S_IFLNK) {
    if (Cns_get_lnk_by_fileid (&thip->dbfd, fmd_entry.fileid,
                               &lnk_entry, 0, NULL))
      RETURNQ (serrno);
  } else {
    if (*path != '/') { /* Need to get path */
      p = tmp_path;
      if (Cns_getpath_by_fileid (&thip->dbfd, fmd_entry.fileid, &p))
        RETURNQ (serrno);
      strcpy (lnk_entry.linkname, p);
    } else
      strcpy (lnk_entry.linkname, path);
  }
  marshall_STRING (sbp, lnk_entry.linkname);
  while ((c = Cns_list_lnk_entry (&thip->dbfd, bol, lnk_entry.linkname,
                                  &lnk_entry, 0)) == 0) {
    bol = 0;
    p = tmp_path;
    if (Cns_getpath_by_fileid (&thip->dbfd, lnk_entry.fileid, &p))
      RETURNQ (serrno);
    n = strlen (p) + 1;
    if (sbp - repbuf + n > REPBUFSZ) {
      sendrep (thip->s, MSG_LINKS, sbp - repbuf, repbuf);
      sbp = repbuf;
    }
    marshall_STRING (sbp, p);
  }
  (void) Cns_list_lnk_entry (&thip->dbfd, bol, lnk_entry.linkname, &lnk_entry,
                             1);
  if (c < 0)
    RETURNQ (serrno);
  if (sbp > repbuf)
    sendrep (thip->s, MSG_LINKS, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_getpath - resolve a file id to a path */

int Cns_srv_getpath(char *req_data,
                    struct Cns_srv_thread_info *thip,
                    struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cur_fileid;
  char *func = "getpath";
  char *p;
  char path[CA_MAXPATHLEN+1];
  char *rbp;
  char repbuf[CA_MAXPATHLEN+1];
  char *sbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cur_fileid);

  if (cur_fileid == 2)
    p = "/";
  else {
    p = path;
    if (Cns_getpath_by_fileid (&thip->dbfd, cur_fileid, &p))
      RETURNQ (serrno);
  }

  /* Record the fileid being processed */
  reqinfo->fileid = cur_fileid;

  sbp = repbuf;
  marshall_STRING (sbp, p);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_getsegattrs - get file segments attributes */

int Cns_srv_getsegattrs(int magic,
                        char *req_data,
                        struct Cns_srv_thread_info *thip,
                        struct Cns_srv_request_info *reqinfo)
{
  int bof = 1;
  int c;
  u_signed64 cwd;
  u_signed64 fileid;
  struct Cns_file_metadata filentry;
  char *func = "getsegattrs";
  int nbseg = 0;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *q;
  char *rbp;
  char repbuf[REPBUFSZ];
  char *sbp;
  struct Cns_seg_metadata smd_entry;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (fileid > 0)
    reqinfo->fileid = fileid;
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  if (fileid) {

    /* Get basename entry */
    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &filentry, 0, NULL))
      RETURN (serrno);

    /* Check parent directory components for search permission */
    if (Cns_chkbackperm (&thip->dbfd, filentry.parent_fileid,
                         reqinfo->uid, reqinfo->gid, reqinfo->clienthost))
      RETURN (serrno);
  } else {

    /* Check parent directory components for search permission and get basename
     * entry
     */
    if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                       reqinfo->clienthost, NULL, NULL, &filentry, NULL,
                       CNS_MUST_EXIST))
      RETURN (serrno);

    /* Record the fileid being processed */
    reqinfo->fileid = filentry.fileid;
  }

  /* Check if the entry is a regular file */
  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);

  /* Get/send file segment entries */
  sbp = repbuf;
  marshall_WORD (sbp, nbseg); /* Will be updated */
  while ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid,
                                   &smd_entry, 0, NULL, 0)) == 0) {
    marshall_WORD (sbp, smd_entry.copyno);
    marshall_WORD (sbp, smd_entry.fsec);
    marshall_HYPER (sbp, smd_entry.segsize);
    marshall_LONG (sbp, smd_entry.compression);
    marshall_BYTE (sbp, smd_entry.s_status);
    marshall_STRING (sbp, smd_entry.vid);
    if (magic >= CNS_MAGIC2)
      marshall_WORD (sbp, smd_entry.side);
    marshall_LONG (sbp, smd_entry.fseq);
    marshall_OPAQUE (sbp, smd_entry.blockid, 4);
    if (magic >= CNS_MAGIC4) {
      marshall_STRING (sbp, smd_entry.checksum_name);
      marshall_LONG (sbp, smd_entry.checksum);
    }
    nbseg++;
    bof = 0;
  }
  (void) Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid, &smd_entry,
                              0, NULL, 1);
  if (c < 0)
    RETURN (serrno);

  q = repbuf;
  marshall_WORD (q, nbseg);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/* Cns_srv_lchown - change owner and group of a symbolic link to file or a directory */

int Cns_srv_lchown(char *req_data,
                   struct Cns_srv_thread_info *thip,
                   struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  int found;
  char *func = "lchown";
  struct group *gr;
  char **membername;
  int need_p_admin = 0;
  int need_p_expt_admin = 0;
  gid_t new_gid;
  uid_t new_uid;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  struct passwd *pw;
  char *rbp;
  Cns_dbrec_addr rec_addr;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_LONG (rbp, new_uid);
  unmarshall_LONG (rbp, new_gid);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\" NewUid=%d NewGid=%d",
           cwdpath, path, new_uid, new_gid);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check parent directory components for search permission and get/lock
   * basename entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, NULL, NULL, &fmd_entry, &rec_addr,
                     CNS_NOFOLLOW|CNS_MUST_EXIST))
    RETURN (serrno);

  /* Record the fileid being processed */
  reqinfo->fileid = fmd_entry.fileid;

  /* Check if the user is authorized to change ownership this entry */
  if (fmd_entry.uid != new_uid && (int)new_uid != -1) {
    if (reqinfo->gid != fmd_entry.gid)
      need_p_admin = 1;
    else if ((pw = Cgetpwuid (new_uid)) == NULL)
      need_p_admin = 1;
    else if (pw->pw_gid == reqinfo->gid) /* New owner belongs to same group */
      need_p_expt_admin = 1;
    else
      need_p_admin = 1;
  }
  if (fmd_entry.gid != new_gid && (int)new_gid != -1) {
    if (reqinfo->uid != fmd_entry.uid) {
      need_p_admin = 1;
    } else if ((pw = Cgetpwuid (reqinfo->uid)) == NULL) {
      need_p_admin = 1;
    } else if ((gr = Cgetgrgid (new_gid)) == NULL) {
      need_p_admin = 1;
    } else {
      if (new_gid == pw->pw_gid) {
        found = 1;
      } else {
        found = 0;
        membername = gr->gr_mem;
        while (*membername) {
          if (strcmp (pw->pw_name, *membername) == 0) {
            found = 1;
            break;
          }
          membername++;
        }
      }
      if (!found)
        need_p_admin = 1;
    }
  }
  if (need_p_admin) {
    if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                    localhost, P_ADMIN))
      RETURN (EPERM);
  } else if (need_p_expt_admin) {
    if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                    localhost, P_ADMIN) &&
        Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                    localhost, P_GRP_ADMIN))
      RETURN (EPERM);
  }

  /* Update entry */
  if ((int)new_uid != -1)
    fmd_entry.uid = new_uid;
  if ((int)new_gid != -1)
    fmd_entry.gid = new_gid;
  if (*fmd_entry.acl)
    Cns_acl_chmod (&fmd_entry);
  fmd_entry.ctime = time (0);
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &fmd_entry))
    RETURN (serrno);
  
  RETURN (0);
}

/* Cns_srv_listclass - list file classes */

int Cns_srv_listclass(char *req_data,
                      struct Cns_srv_thread_info *thip,
                      struct Cns_srv_request_info *reqinfo,
                      struct Cns_class_metadata *class_entry,
                      int endlist)
{
  int bol; /* Beginning of class list flag */
  int bot; /* Beginning of tape pools list flag */
  int c;
  int eol = 0; /* End of list flag */
  char *func = "listclass";
  int listentsz; /* Size of client machine Cns_fileclass structure */
  int maxsize;
  int nbentries = 0;
  int nbtppools;
  char outbuf[LISTBUFSZ+4];
  char *p;
  char *q;
  char *rbp;
  char *sav_sbp;
  char *sbp;
  struct Cns_tp_pool tppool_entry;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_WORD (rbp, listentsz);
  unmarshall_WORD (rbp, bol);

  bol = 0;
  if (! class_entry->classid)
    bol = 1; /* Do not rely on client */

  /* Return as many entries as possible to the client */
  maxsize = LISTBUFSZ;
  sbp = outbuf;
  marshall_WORD (sbp, nbentries);  /* Will be updated */

  if (bol || endlist)
    c = Cns_list_class_entry (&thip->dbfd, bol, class_entry, endlist);
  else
    c = 0;
  while (c == 0) {
    if (listentsz > maxsize) break;
    sav_sbp = sbp;
    marshall_LONG (sbp, class_entry->classid);
    marshall_STRING (sbp, class_entry->name);
    marshall_LONG (sbp, class_entry->uid);
    marshall_LONG (sbp, class_entry->gid);
    marshall_LONG (sbp, class_entry->min_filesize);
    marshall_LONG (sbp, class_entry->max_filesize);
    marshall_LONG (sbp, class_entry->flags);
    marshall_LONG (sbp, class_entry->maxdrives);
    marshall_LONG (sbp, class_entry->max_segsize);
    marshall_LONG (sbp, class_entry->migr_time_interval);
    marshall_LONG (sbp, class_entry->mintime_beforemigr);
    marshall_LONG (sbp, class_entry->nbcopies);
    marshall_LONG (sbp, class_entry->retenp_on_disk);

    /* Get/send tppool entries */
    bot = 1;
    nbtppools = 0;
    q = sbp;
    marshall_LONG (sbp, nbtppools); /* Will be updated */
    maxsize -= listentsz;
    while ((c = Cns_get_tppool_by_cid (&thip->dbfd, bot, class_entry->classid,
                                       &tppool_entry, 0, NULL, 0)) == 0) {
      maxsize -= CA_MAXPOOLNAMELEN + 1;
      if (maxsize < 0) {
        sbp = sav_sbp;
        goto reply;
      }
      marshall_STRING (sbp, tppool_entry.tape_pool);
      nbtppools++;
      bot = 0;
    }
    (void) Cns_get_tppool_by_cid (&thip->dbfd, bot, class_entry->classid,
                                  &tppool_entry, 0, NULL, 1);
    if (c < 0)
      RETURN (serrno);

    marshall_LONG (q, nbtppools);
    nbentries++;
    bol = 0;
    c = Cns_list_class_entry (&thip->dbfd, bol, class_entry, endlist);
  }
  if (c < 0)
    RETURN (serrno);
  if (c == 1)
    eol = 1;

 reply:
  marshall_WORD (sbp, eol);
  p = outbuf;
  marshall_WORD (p, nbentries);  /* Update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
  RETURN (0);
}

int Cns_srv_listlinks(char *req_data,
                      struct Cns_srv_thread_info *thip,
                      struct Cns_srv_request_info *reqinfo,
                      struct Cns_symlinks *lnk_entry,
                      int endlist)
{
  int bol; /* Beginning of list flag */
  int c;
  u_signed64 cwd;
  int eol = 0; /* End of list flag */
  struct Cns_file_metadata fmd_entry;
  char *func = "listlinks";
  char guid[CA_MAXGUIDLEN+1];
  int listentsz; /* Size of client machine Cns_linkinfo structure */
  int maxsize;
  int nbentries = 0;
  char outbuf[LISTBUFSZ+4];
  char *p;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  char *sbp;
  char tmp_path[CA_MAXPATHLEN+1];

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_WORD (rbp, listentsz);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN + 1) < 0)
    RETURN (EINVAL);
  unmarshall_WORD (rbp, bol);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\" Guid=\"%s\"",
           cwdpath, path, guid);

  bol = 0;
  if (! lnk_entry->fileid)
    bol = 1; /* Do not rely on client */

  if (bol) {
    if (*path) {

      /* Check parent directory components for search permission and get
       * basename entry
       */
      if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                         reqinfo->clienthost, NULL, NULL, &fmd_entry, NULL,
                         CNS_MUST_EXIST|CNS_NOFOLLOW))
        RETURN (serrno);
      if (*guid && strcmp (guid, fmd_entry.guid))
        RETURN (EINVAL);
    } else {
      if (! *guid)
        RETURN (ENOENT);

      /* Get basename entry */
      if (Cns_get_fmd_by_guid (&thip->dbfd, guid, &fmd_entry, 0, NULL))
        RETURN (serrno);

      /* Do not check parent directory components for search permission as
       * symlinks can anyway point directly at a file
       */
    }
    if ((fmd_entry.filemode & S_IFMT) == S_IFLNK) {
      if (Cns_get_lnk_by_fileid (&thip->dbfd, fmd_entry.fileid,
                                 lnk_entry, 0, NULL))
        RETURN (serrno);
    } else {
      if (*path != '/') { /* Need to get path */
        p = tmp_path;
        if (Cns_getpath_by_fileid (&thip->dbfd, fmd_entry.fileid, &p))
          RETURN (serrno);
        strcpy (lnk_entry->linkname, p);
      } else
        strcpy (lnk_entry->linkname, path);
    }
  }

  /* Return as many entries as possible to the client */
  maxsize = LISTBUFSZ;
  sbp = outbuf;
  marshall_WORD (sbp, nbentries);  /* Will be updated */

  if (bol) {
    marshall_STRING (sbp, lnk_entry->linkname);
    maxsize -= listentsz;
    nbentries++;
  }
  if (bol || endlist)
    c = Cns_list_lnk_entry (&thip->dbfd, bol, lnk_entry->linkname, lnk_entry,
                            endlist);
  else
    c = 0;
  while (c == 0) {
    if (listentsz > maxsize) break;
    p = tmp_path;
    if (Cns_getpath_by_fileid (&thip->dbfd, lnk_entry->fileid, &p))
      RETURN (serrno);
    marshall_STRING (sbp, p);
    maxsize -= listentsz;
    nbentries++;
    bol = 0;
    c = Cns_list_lnk_entry (&thip->dbfd, bol, lnk_entry->linkname, lnk_entry,
                            endlist);
  }
  if (c < 0)
    RETURN (serrno);
  if (c == 1)
    eol = 1;

  marshall_WORD (sbp, eol);
  p = outbuf;
  marshall_WORD (p, nbentries);  /* Update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
  RETURN (0);
}

int Cns_srv_lastfseq(int magic,
                     char *req_data,
                     struct Cns_srv_thread_info *thip,
                     struct Cns_srv_request_info *reqinfo)
{
  struct Cns_seg_metadata smd_entry;
  char  *func = "lastfseq";
  char  vid[CA_MAXVIDLEN+1];
  char  repbuf[REPBUFSZ];
  char  *rbp;
  char  *sbp;
  int   side;
  int   c;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN(rbp, vid, CA_MAXVIDLEN + 1)) {
    RETURN (EINVAL);
  }
  unmarshall_LONG(rbp, side);

  /* Construct log message */
  sprintf(reqinfo->logbuf, "TPVID=%s Side=%d", vid, side);

  /* Find the last file sequence number for the volume */
  c = Cns_get_last_smd_by_vid(&thip->dbfd, vid, side, &smd_entry);
  if (c < 0) {
    RETURN (serrno);
  }

  /* Marshall to segattrs structure */
  sbp = repbuf;
  marshall_WORD(sbp, smd_entry.copyno);
  marshall_WORD(sbp, smd_entry.fsec);
  marshall_HYPER(sbp, smd_entry.segsize);
  marshall_LONG(sbp, smd_entry.compression);
  marshall_BYTE(sbp, smd_entry.s_status);
  marshall_STRING(sbp, smd_entry.vid);
  if (magic >= CNS_MAGIC2) {
    marshall_WORD(sbp, smd_entry.side);
  }
  marshall_LONG(sbp, smd_entry.fseq);
  marshall_OPAQUE(sbp, smd_entry.blockid, 4);
  if (magic >= CNS_MAGIC4) {
    marshall_STRING(sbp, smd_entry.checksum_name);
  }
  marshall_LONG(sbp, smd_entry.checksum);

  /* Send response */
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

int Cns_srv_bulkexist(char *req_data,
                      struct Cns_srv_thread_info *thip,
                      struct Cns_srv_request_info *reqinfo)
{
  char  *func = "bulkexist";
  char  *repbuf;
  char  *rbp;
  char  *sbp;
  u_signed64 *fileIds;
  int nbFileIds, i, c, count = 0;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_LONG(rbp, nbFileIds);

  /* Construct log message */
  sprintf(reqinfo->logbuf, "NbFileIds=%d LongOps=\"False\"", nbFileIds);

  if (nbFileIds > 3000) {
    RETURN (EINVAL);
  }
  fileIds = (u_signed64*) malloc(nbFileIds * HYPERSIZE);
  for (i = 0; i < nbFileIds; i++) {
    unmarshall_HYPER(rbp, fileIds[i]);
  }

  /* Check for long database operations e.g. backups */
  c = Cns_count_long_ops(&thip->dbfd, &count, 1);
  if (c < 0) {
    free(fileIds);
    RETURN (serrno);
  } else if (count == 0) {
    /* Check file existence */
    c = Cns_check_files_exist(&thip->dbfd, fileIds, &nbFileIds);
    if (c < 0) {
      free(fileIds);
      RETURN (serrno);
    }
  } else {
    sprintf(reqinfo->logbuf, "NbFileIds=%d LongOps=\"True\"", nbFileIds);
    nbFileIds = 0;
  }

  /* Marshall list of non existent files */
  repbuf = (char*) malloc(LONGSIZE + nbFileIds * HYPERSIZE);
  if (repbuf == NULL) {
    free(fileIds);
    RETURN (serrno);
  }
  sbp = repbuf;
  marshall_LONG(sbp, nbFileIds);
  for (i = 0; i < nbFileIds; i++) {
    marshall_HYPER(sbp, fileIds[i]);
  }
  /* Send response */
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  free(repbuf);
  free(fileIds);
  RETURN (0);
}

int Cns_srv_tapesum(char *req_data,
                    struct Cns_srv_thread_info *thip,
                    struct Cns_srv_request_info *reqinfo)
{
  char  *func = "tapesum";
  char  vid[CA_MAXVIDLEN+1];
  char  repbuf[REPBUFSZ];
  u_signed64 count = 0;
  u_signed64 size = 0;
  u_signed64 maxfileid = 0;
  u_signed64 avgcompression = 0;
  char  *rbp;
  char  *sbp;
  int   c;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN(rbp, vid, CA_MAXVIDLEN + 1)) {
    RETURN (EINVAL);
  }

  /* Construct log message */
  sprintf(reqinfo->logbuf, "TPVID=%s", vid);

  /* Get tape summary information */
  c = Cns_get_tapesum_by_vid(&thip->dbfd, vid, &count, &size, &maxfileid,
                             &avgcompression);
  if (c < 0) {
    RETURN (serrno);
  }

  /* Marshall response */
  sbp = repbuf;
  marshall_HYPER(sbp, count);
  marshall_HYPER(sbp, size);
  marshall_HYPER(sbp, maxfileid);
  marshall_HYPER(sbp, avgcompression);

  /* Send response */
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

int Cns_srv_listtape(int magic,
                     char *req_data,
                     struct Cns_srv_thread_info *thip,
                     struct Cns_srv_request_info *reqinfo,
                     struct Cns_file_metadata *fmd_entry,
                     struct Cns_seg_metadata *smd_entry,
                     int endlist)
{
  int bov; /* Beginning of volume flag */
  int c;
  char dirbuf[DIRBUFSZ+4];
  int direntsz; /* Size of client machine dirent structure excluding d_name */
  int eov = 0; /* End of volume flag */
  char *func = "listtape";
  int fseq = 0;   /* File sequence number to filter on */
  int maxsize;
  int nbentries = 0;
  char *p;
  char *rbp;
  char *sbp;
  char vid[CA_MAXVIDLEN+1];

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_WORD (rbp, direntsz);
  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1))
    RETURN (EINVAL);
  unmarshall_WORD (rbp, bov);
  if (magic >= CNS_MAGIC5) {
    unmarshall_LONG (rbp, fseq);
  }

  bov = 0;
  if (! smd_entry->s_fileid)
    bov = 1; /* Do not rely on client */

  /* Construct log message */
  sprintf (reqinfo->logbuf, "TPVID=%s Bov=%d Fseq=%d", vid, bov, fseq);

  /* Return as many entries as possible to the client */
  maxsize = DIRBUFSZ - direntsz;
  sbp = dirbuf;
  marshall_WORD (sbp, nbentries);  /* Will be updated */

  if (! bov && ! endlist) {
    marshall_DIRXT (&sbp, magic, fmd_entry, smd_entry);
    nbentries++;
    maxsize -= ((direntsz + strlen (fmd_entry->name) + 8) / 8) * 8;
  }
  while ((c = Cns_get_smd_by_vid (&thip->dbfd, bov, vid, fseq, smd_entry,
                                  endlist)) == 0) {
    bov = 0;
    if (Cns_get_fmd_by_fileid (&thip->dbfd, smd_entry->s_fileid,
                               fmd_entry, 0, NULL) < 0) {
      /* Ignore files deleted during the listing */
      if (serrno == ENOENT)
        continue;
      RETURN (serrno);
    }
    if ((int) strlen (fmd_entry->name) > maxsize) break;
    marshall_DIRXT (&sbp, magic, fmd_entry, smd_entry);
    nbentries++;
    maxsize -= ((direntsz + strlen (fmd_entry->name) + 8) / 8) * 8;
  }
  if (c < 0)
    RETURN (serrno);
  if (c == 1)
    eov = 1;

  marshall_WORD (sbp, eov);
  p = dirbuf;
  marshall_WORD (p, nbentries);  /* Update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - dirbuf, dirbuf);
  RETURN (0);
}

/* Cns_srv_lstat - get information about a symbolic link */

int Cns_srv_lstat(char *req_data,
                  struct Cns_srv_thread_info *thip,
                  struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  u_signed64 fileid;
  struct Cns_file_metadata fmd_entry;
  char *func = "lstat";
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  char repbuf[57];
  char *sbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (fileid > 0)
    reqinfo->fileid = fileid;
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  if (fileid) {

    /* Get basename entry */
    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &fmd_entry, 0, NULL))
      RETURNQ (serrno);

    /* Check parent directory components for search permission */
    if (Cns_chkbackperm (&thip->dbfd, fmd_entry.parent_fileid, reqinfo->uid,
                         reqinfo->gid, reqinfo->clienthost))
      RETURNQ (serrno);
  } else {

    /* Check parent directory components for search permission and get basename
     * entry
     */
    if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                       reqinfo->clienthost, NULL, NULL, &fmd_entry, NULL,
                       CNS_NOFOLLOW|CNS_MUST_EXIST))
      RETURNQ (serrno);

    /* Record the fileid being processed */
    reqinfo->fileid = fmd_entry.fileid;
  }

  sbp = repbuf;
  marshall_HYPER (sbp, fmd_entry.fileid);
  marshall_WORD (sbp, fmd_entry.filemode);
  marshall_LONG (sbp, fmd_entry.nlink);
  marshall_LONG (sbp, fmd_entry.uid);
  marshall_LONG (sbp, fmd_entry.gid);
  marshall_HYPER (sbp, fmd_entry.filesize);
  marshall_TIME_T (sbp, fmd_entry.atime);
  marshall_TIME_T (sbp, fmd_entry.mtime);
  marshall_TIME_T (sbp, fmd_entry.ctime);
  marshall_WORD (sbp, fmd_entry.fileclass);
  marshall_BYTE (sbp, fmd_entry.status);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_mkdir - create a directory entry */

int Cns_srv_mkdir(int magic,
                  char *req_data,
                  struct Cns_srv_thread_info *thip,
                  struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  struct Cns_file_metadata direntry;
  char *func = "mkdir";
  char guid[CA_MAXGUIDLEN+1];
  mode_t mask;
  mode_t mode;
  struct Cns_file_metadata parent_dir;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addrp;
  uuid_t uuid;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_WORD (rbp, mask);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_LONG (rbp, mode);
  /* mode must be within 0 and 07777 */
  if (mode > 4095) RETURN (EINVAL);
  if (magic >= CNS_MAGIC2) {
    if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN+1))
      RETURN (EINVAL);
    if (uuid_parse (guid, uuid) < 0)
      RETURN (EINVAL);
  } else
    *guid = '\0';

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Mask=%o Cwd=\"%s\" Path=\"%s\" Mode=%o "
           "Guid=\"%s\"",
           mask, cwdpath, path, mode, guid);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check parent directory components for write/search permission and get
   * basename entry if it exists
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, &parent_dir, &rec_addrp, &direntry,
                     NULL, CNS_NOFOLLOW))
    RETURN (serrno);

  if (*direntry.name == '/') /* Cns_mkdir / */
    RETURN (EEXIST);

  /* Check if basename entry exists already */
  if (direntry.fileid)
    RETURN (EEXIST);

  /* Build new directory entry */
  if (Cns_unique_id (&thip->dbfd, &direntry.fileid) < 0)
    RETURN (serrno);

  /* parent_fileid and name have been set by Cns_parsepath */
  strcpy (direntry.guid, guid);
  direntry.filemode = S_IFDIR | ((mode & ~S_IFMT) & ~mask);
  direntry.nlink = 0;
  direntry.uid = reqinfo->uid;
  if (parent_dir.filemode & S_ISGID) {
    direntry.gid = parent_dir.gid;
    if (reqinfo->gid == direntry.gid)
      direntry.filemode |= S_ISGID;
  } else
    direntry.gid = reqinfo->gid;
  direntry.atime = time (0);
  direntry.mtime = direntry.atime;
  direntry.ctime = direntry.atime;
  direntry.fileclass = parent_dir.fileclass;
  direntry.status = '-';
  if (*parent_dir.acl)
    Cns_acl_inherit (&parent_dir, &direntry, mode);

  /* Write new directory entry */
  if (Cns_insert_fmd_entry (&thip->dbfd, &direntry))
    RETURN (serrno);

  /* Update parent directory entry */
  parent_dir.nlink++;
  parent_dir.mtime = time (0);
  parent_dir.ctime = parent_dir.mtime;
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_modifyclass - modify an existing fileclass definition */

int Cns_srv_modifyclass(char *req_data,
                        struct Cns_srv_thread_info *thip,
                        struct Cns_srv_request_info *reqinfo)
{
  int bol = 1;
  struct Cns_class_metadata class_entry;
  gid_t class_group;
  char class_name[CA_MAXCLASNAMELEN+1];
  uid_t class_user;
  int classid;
  int flags;
  char *func = "modifyclass";
  int i;
  int maxdrives;
  int max_filesize;
  int max_segsize;
  int migr_time_interval;
  int mintime_beforemigr;
  int min_filesize;
  int nbcopies;
  int nbtppools;
  char new_class_name[CA_MAXCLASNAMELEN+1];
  char *p;
  char *rbp;
  Cns_dbrec_addr rec_addr;
  Cns_dbrec_addr rec_addrt;
  int retenp_on_disk;
  struct Cns_tp_pool tppool_entry;
  char *tppools;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_LONG (rbp, classid);
  if (unmarshall_STRINGN (rbp, class_name, CA_MAXCLASNAMELEN+1))
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, new_class_name, CA_MAXCLASNAMELEN+1))
    RETURN (EINVAL);
  unmarshall_LONG (rbp, class_user);
  unmarshall_LONG (rbp, class_group);
  unmarshall_LONG (rbp, min_filesize);
  unmarshall_LONG (rbp, max_filesize);
  unmarshall_LONG (rbp, flags);
  unmarshall_LONG (rbp, maxdrives);
  unmarshall_LONG (rbp, max_segsize);
  unmarshall_LONG (rbp, migr_time_interval);
  unmarshall_LONG (rbp, mintime_beforemigr);
  unmarshall_LONG (rbp, nbcopies);
  unmarshall_LONG (rbp, retenp_on_disk);
  unmarshall_LONG (rbp, nbtppools);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "ClassId=%d ClassName=\"%s\"", classid, class_name);

  /* Check if the user is authorized to modify a file class */
  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                  localhost, P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Get and lock entry */
  memset((void *) &class_entry, 0, sizeof(struct Cns_class_metadata));
  if (classid > 0) {
    if (Cns_get_class_by_id (&thip->dbfd, classid, &class_entry,
                             1, &rec_addr))
      RETURN (serrno);
    if (*class_name && strcmp (class_name, class_entry.name))
      RETURN (EINVAL);
  } else {
    if (Cns_get_class_by_name (&thip->dbfd, class_name, &class_entry,
                               1, &rec_addr))
      RETURN (serrno);
  }

  /* Update entry */
  if (*new_class_name)
    strcpy (class_entry.name, new_class_name);
  if ((int)class_user != -1)
    class_entry.uid = class_user;
  if ((int)class_group != -1)
    class_entry.gid = class_group;
  if (min_filesize >= 0)
    class_entry.min_filesize = min_filesize;
  if (max_filesize >= 0)
    class_entry.max_filesize = max_filesize;
  if (flags >= 0)
    class_entry.flags = flags;
  if (maxdrives >= 0)
    class_entry.maxdrives = maxdrives;
  if (max_segsize >= 0)
    class_entry.max_segsize = max_segsize;
  if (migr_time_interval >= 0)
    class_entry.migr_time_interval = migr_time_interval;
  if (mintime_beforemigr >= 0)
    class_entry.mintime_beforemigr = mintime_beforemigr;
  if (nbcopies >= 0)
    class_entry.nbcopies = nbcopies;
  if (retenp_on_disk >= 0)
    class_entry.retenp_on_disk = retenp_on_disk;

  if (Cns_update_class_entry (&thip->dbfd, &rec_addr, &class_entry))
    RETURN (serrno);

  if (nbtppools > 0) {
    if ((tppools = calloc (nbtppools, CA_MAXPOOLNAMELEN+1)) == NULL)
      RETURN (ENOMEM);
    p = tppools;
    for (i = 0; i < nbtppools; i++) {
      if (unmarshall_STRINGN (rbp, p, CA_MAXPOOLNAMELEN+1)) {
        free (tppools);
        RETURN (EINVAL);
      }
      p += (CA_MAXPOOLNAMELEN+1);
    }

    /* Delete the entries which are not needed anymore */
    while (Cns_get_tppool_by_cid (&thip->dbfd, bol, class_entry.classid,
                                  &tppool_entry, 1, &rec_addrt, 0) == 0) {
      p = tppools;
      for (i = 0; i < nbtppools; i++) {
        if (strcmp (tppool_entry.tape_pool, p) == 0) break;
        p += (CA_MAXPOOLNAMELEN+1);
      }
      if (i >= nbtppools) {
        if (Cns_delete_tppool_entry (&thip->dbfd, &rec_addrt)) {
          free (tppools);
          RETURN (serrno);
        }
      } else
        *p = '\0';
      bol = 0;
    }
    (void) Cns_get_tppool_by_cid (&thip->dbfd, bol, class_entry.classid,
                                  &tppool_entry, 1, &rec_addrt, 1);

    /* Add the new entries if any */
    tppool_entry.classid = class_entry.classid;
    p = tppools;
    for (i = 0; i < nbtppools; i++) {
      if (*p) {
        strcpy (tppool_entry.tape_pool, p);
        if (Cns_insert_tppool_entry (&thip->dbfd, &tppool_entry)) {
          free (tppools);
          RETURN (serrno);
        }
      }
      p += (CA_MAXPOOLNAMELEN+1);
    }
    free (tppools);
  }
  RETURN (0);
}

/* Cns_srv_opendir - open a directory entry */

int Cns_srv_opendir(int magic,
                    char *req_data,
                    struct Cns_srv_thread_info *thip,
                    struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  struct Cns_file_metadata direntry;
  char *func = "opendir";
  char guid[CA_MAXGUIDLEN+1];
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  char repbuf[8];
  char *sbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  if (magic >= CNS_MAGIC2) {
    if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN+1))
      RETURN (EINVAL);
  } else
    *guid = '\0';

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\" Guid=\"%s\"",
           cwdpath, path, guid);

  if (*guid) {

    /* Get basename entry */
    if (Cns_get_fmd_by_guid (&thip->dbfd, guid, &direntry, 0, NULL))
      RETURN (serrno);

    /* Do not check parent directory components for search permission as
     * symlinks can anyway point directly at a file
     */
  } else {
    if (! cwd && *path == 0)
      RETURN (ENOENT);
    if (! cwd && *path != '/')
      RETURN (EINVAL);

    /* Check parent directory components for search permission and get basename
     * entry
     */
    if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                       reqinfo->clienthost, NULL, NULL, &direntry, NULL,
                       CNS_MUST_EXIST))
      RETURN (serrno);
  }

  if ((direntry.filemode & S_IFDIR) == 0)
    RETURN (ENOTDIR);
  if (Cns_chkentryperm (&direntry, S_IREAD|S_IEXEC, reqinfo->uid, reqinfo->gid,
                        reqinfo->clienthost))
    RETURN (EACCES);

  /* Return directory fileid */
  sbp = repbuf;
  marshall_HYPER (sbp, direntry.fileid);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/* Cns_srv_ping - check server alive and return version number */

int Cns_srv_ping(char *req_data,
                 struct Cns_srv_thread_info *thip,
                 struct Cns_srv_request_info *reqinfo)
{
  char *func = "ping";
  char info[256];
  char repbuf[REPBUFSZ];
  char *sbp;
  char *rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  sprintf (info, "%d.%d.%d-%d", MAJORVERSION, MINORVERSION, MAJORRELEASE, MINORRELEASE);
  sbp = repbuf;
  marshall_STRING (sbp, info);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/* Cns_srv_queryclass - query about a file class */

int Cns_srv_queryclass(char *req_data,
                       struct Cns_srv_thread_info *thip,
                       struct Cns_srv_request_info *reqinfo)
{
  int bol = 1;
  int c;
  struct Cns_class_metadata class_entry;
  char class_name[CA_MAXCLASNAMELEN+1];
  int classid;
  char *func = "queryclass";
  int nbtppools = 0;
  char *q;
  char *rbp;
  char repbuf[LISTBUFSZ];
  char *sbp;
  struct Cns_tp_pool tppool_entry;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_LONG (rbp, classid);
  if (unmarshall_STRINGN (rbp, class_name, CA_MAXCLASNAMELEN+1))
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "ClassId=%d ClassName=\"%s\"", classid, class_name);

  memset((void *) &class_entry, 0, sizeof(struct Cns_class_metadata));
  if (classid > 0) {
    if (Cns_get_class_by_id (&thip->dbfd, classid, &class_entry, 0, NULL))
      RETURN (serrno);
    if (*class_name && strcmp (class_name, class_entry.name))
      RETURN (EINVAL);
  } else {
    if (Cns_get_class_by_name (&thip->dbfd, class_name, &class_entry, 0, NULL))
      RETURN (serrno);
  }

  sbp = repbuf;
  marshall_LONG (sbp, class_entry.classid);
  marshall_STRING (sbp, class_entry.name);
  marshall_LONG (sbp, class_entry.uid);
  marshall_LONG (sbp, class_entry.gid);
  marshall_LONG (sbp, class_entry.min_filesize);
  marshall_LONG (sbp, class_entry.max_filesize);
  marshall_LONG (sbp, class_entry.flags);
  marshall_LONG (sbp, class_entry.maxdrives);
  marshall_LONG (sbp, class_entry.max_segsize);
  marshall_LONG (sbp, class_entry.migr_time_interval);
  marshall_LONG (sbp, class_entry.mintime_beforemigr);
  marshall_LONG (sbp, class_entry.nbcopies);
  marshall_LONG (sbp, class_entry.retenp_on_disk);

  /* Get/send tppool entries */
  q = sbp;
  marshall_LONG (sbp, nbtppools); /* Will be updated */
  while ((c = Cns_get_tppool_by_cid (&thip->dbfd, bol, class_entry.classid,
                                     &tppool_entry, 0, NULL, 0)) == 0) {
    marshall_STRING (sbp, tppool_entry.tape_pool);
    nbtppools++;
    bol = 0;
  }
  (void) Cns_get_tppool_by_cid (&thip->dbfd, bol, class_entry.classid,
                                &tppool_entry, 0, NULL, 1);
  if (c < 0)
    RETURN (serrno);

  marshall_LONG (q, nbtppools);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/* Cns_srv_readdir - read directory entries */

int Cns_srv_readdir(int magic,
                    char *req_data,
                    struct Cns_srv_thread_info *thip,
                    struct Cns_srv_request_info *reqinfo,
                    struct Cns_file_metadata *fmd_entry,
                    struct Cns_seg_metadata *smd_entry,
                    struct Cns_user_metadata *umd_entry,
                    int endlist,
                    int *beginp)
{
  int bod; /* Beginning of directory flag */
  int bof; /* Beginning of file flag */
  int c;
  int cml; /* Comment length */
  char dirbuf[DIRBUFSZ+4];
  struct Cns_file_metadata direntry;
  int direntsz; /* Size of client machine dirent structure excluding d_name */
  u_signed64 dir_fileid;
  int eod = 0; /* End of directory flag */
  int fnl; /* Filename length */
  char *func = "readdir";
  int getattr;
  int maxsize;
  int nbentries = 0;
  char *p;
  char *rbp;
  Cns_dbrec_addr rec_addr;
  char *sbp;
  char se[CA_MAXHOSTNAMELEN+1];

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (endlist)
    func = "closedir";
  unmarshall_WORD (rbp, getattr);
  unmarshall_WORD (rbp, direntsz);
  unmarshall_HYPER (rbp, dir_fileid);
  unmarshall_WORD (rbp, bod);

  bod = *beginp; /* Do not rely on client */
  *beginp = 0;
  if (getattr == 5 && unmarshall_STRINGN (rbp, se, CA_MAXHOSTNAMELEN+1))
    RETURN (EINVAL);

  /* Return as many entries as possible to the client */
  if (getattr == 1 || getattr == 4)
    if (DIRXSIZE > direntsz)
      direntsz = DIRXSIZE;
  maxsize = DIRBUFSZ - direntsz;
  sbp = dirbuf;
  marshall_WORD (sbp, nbentries);  /* Will be updated */

  if (endlist && getattr == 2)
    (void) Cns_get_smd_by_pfid (&thip->dbfd, 0, fmd_entry->fileid,
                                smd_entry, 0, NULL, 1);
  if (! bod && ! endlist) {
    fnl = strlen (fmd_entry->name);
    if (getattr == 0) {  /* readdir */
      marshall_STRING (sbp, fmd_entry->name);
      nbentries++;
      maxsize -= ((direntsz + fnl + 8) / 8) * 8;
    } else if (getattr == 1) { /* readdirx */
      marshall_DIRX (&sbp, magic, fmd_entry);
      nbentries++;
      maxsize -= ((direntsz + fnl + 8) / 8) * 8;
    } else if (getattr == 2) { /* readdirxt */
      bof = 0;
      /* Loop on segments */
      while (1) {
        marshall_DIRXT (&sbp, magic, fmd_entry, smd_entry);
        nbentries++;
        maxsize -= ((direntsz + fnl + 8) / 8) * 8;
        if ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof, fmd_entry->fileid,
                                      smd_entry, 0, NULL, 0))) break;
        if (fnl >= maxsize)
          goto reply;
      }
      (void) Cns_get_smd_by_pfid (&thip->dbfd, bof, fmd_entry->fileid,
                                  smd_entry, 0, NULL, 1);
      if (c < 0)
        RETURN (serrno);
    } else if (getattr == 3) { /* readdirc */
      cml = strlen (umd_entry->comments);
      marshall_STRING (sbp, fmd_entry->name);
      marshall_STRING (sbp, umd_entry->comments);
      nbentries++;
      maxsize -= ((direntsz + fnl + cml + 9) / 8) * 8;
    } else if (getattr == 4) { /* readdirxc */
      cml = strlen (umd_entry->comments);
      marshall_DIRX (&sbp, magic, fmd_entry);
      marshall_STRING (sbp, umd_entry->comments);
      nbentries++;
      maxsize -= ((direntsz + fnl + cml + 9) / 8) * 8;
    }
  }

  while ((c = Cns_get_fmd_by_pfid (&thip->dbfd, bod, dir_fileid, fmd_entry,
                                   getattr, endlist)) == 0) {
    fnl = strlen (fmd_entry->name);
    if (getattr == 0) {  /* readdir */
      if (fnl >= maxsize) break;
      marshall_STRING (sbp, fmd_entry->name);
      nbentries++;
      maxsize -= ((direntsz + fnl + 8) / 8) * 8;
    } else if (getattr == 1) { /* readdirx */
      if (fnl >= maxsize) break;
      marshall_DIRX (&sbp, magic, fmd_entry);
      nbentries++;
      maxsize -= ((direntsz + fnl + 8) / 8) * 8;
    } else if (getattr == 2) { /* readdirxt */
      bof = 1;
      /* Loop on segments */
      while (1) {
        if ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof, fmd_entry->fileid,
                                      smd_entry, 0, NULL, 0)))
          break;
        if (fnl >= maxsize)
          goto reply;
        marshall_DIRXT (&sbp, magic, fmd_entry, smd_entry);
        nbentries++;
        bof = 0;
        maxsize -= ((direntsz + fnl + 8) / 8) * 8;
      }
      (void) Cns_get_smd_by_pfid (&thip->dbfd, bof, fmd_entry->fileid,
                                  smd_entry, 0, NULL, 1);
      if (c < 0)
        RETURN (serrno);
    } else if (getattr == 3) { /* readdirc */
      *umd_entry->comments = '\0';
      if (Cns_get_umd_by_fileid (&thip->dbfd, fmd_entry->fileid,
                                 umd_entry, 0, NULL) && serrno != ENOENT)
        RETURN (serrno);
      cml = strlen (umd_entry->comments);
      if (fnl + cml + 1 >= maxsize) break;
      marshall_STRING (sbp, fmd_entry->name);
      marshall_STRING (sbp, umd_entry->comments);
      nbentries++;
      maxsize -= ((direntsz + fnl + cml + 9) / 8) * 8;
    } else if (getattr == 4) { /* readdirxc */
      *umd_entry->comments = '\0';
      if (Cns_get_umd_by_fileid (&thip->dbfd, fmd_entry->fileid,
                                 umd_entry, 0, NULL) && serrno != ENOENT)
        RETURN (serrno);
      cml = strlen (umd_entry->comments);
      if (fnl + cml + 1 >= maxsize) break;
      marshall_DIRX (&sbp, magic, fmd_entry);
      marshall_STRING (sbp, umd_entry->comments);
      nbentries++;
      maxsize -= ((direntsz + fnl + cml + 9) / 8) * 8;
    }
    bod = 0;
  }

  if (c < 0)
    RETURN (serrno);
  if (c == 1) {
    eod = 1;

    /* Start transaction */
    (void) Cns_start_tr (&thip->dbfd);

    /* Update directory access time */
    if (!rdonly) {
      if (Cns_get_fmd_by_fileid (&thip->dbfd, dir_fileid, &direntry, 1,
                                 &rec_addr))
        RETURN (serrno);
      direntry.atime = time (0);
      if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &direntry))
        RETURN (serrno);
    }
  }

 reply:
  marshall_WORD (sbp, eod);
  p = dirbuf;
  marshall_WORD (p, nbentries);  /* Update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - dirbuf, dirbuf);
  RETURN (0);
}

/* Cns_srv_readlink - read value of symbolic link */

int Cns_srv_readlink(char *req_data,
                     struct Cns_srv_thread_info *thip,
                     struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char *func = "readlink";
  struct Cns_symlinks lnk_entry;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  char repbuf[CA_MAXPATHLEN+1];
  char *sbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  /* Check parent directory components for search permission and get basename
   * entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, NULL, NULL, &filentry, NULL,
                     CNS_MUST_EXIST|CNS_NOFOLLOW))
    RETURNQ (serrno);

  /* Check if the user is authorized to get link value for this entry */
  if (reqinfo->uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IREAD, reqinfo->uid, reqinfo->gid,
                        reqinfo->clienthost))
    RETURNQ (EACCES);

  if ((filentry.filemode & S_IFLNK) != S_IFLNK)
    RETURNQ (EINVAL);

  /* Get link value */
  if (Cns_get_lnk_by_fileid (&thip->dbfd, filentry.fileid, &lnk_entry, 0,
                             NULL))
    RETURNQ (serrno);

  sbp = repbuf;
  marshall_STRING (sbp, lnk_entry.linkname);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_rename - rename a file or a directory */

int Cns_srv_rename(char *req_data,
                   struct Cns_srv_thread_info *thip,
                   struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  u_signed64 fileid;
  char *func = "rename";
  struct Cns_symlinks lnk_entry;
  int new_exists = 0;
  struct Cns_file_metadata new_fmd_entry;
  struct Cns_file_metadata new_parent_dir;
  Cns_dbrec_addr new_rec_addr;
  Cns_dbrec_addr new_rec_addrp;
  char newpath[CA_MAXPATHLEN+1];
  struct Cns_file_metadata old_fmd_entry;
  struct Cns_file_metadata old_parent_dir;
  Cns_dbrec_addr old_rec_addr;
  Cns_dbrec_addr old_rec_addrp;
  char oldpath[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addrl; /* Symlink record address */
  Cns_dbrec_addr rec_addru; /* Comment record address */
  struct Cns_file_metadata tmp_fmd_entry;
  struct Cns_user_metadata umd_entry;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, oldpath, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  if (unmarshall_STRINGN (rbp, newpath, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" OldPath=\"%s\" NewPath=\"%s\"",
           cwdpath, oldpath, newpath);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check 'old' parent directory components for write/search permission and
   * get/lock basename entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, oldpath, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, &old_parent_dir, &old_rec_addrp,
                     &old_fmd_entry, &old_rec_addr,
                     CNS_MUST_EXIST|CNS_NOFOLLOW))
    RETURN (serrno);

  /* Check 'new' parent directory components for write/search permission and
   * get/lock basename entry if it exists
   */
  if (Cns_parsepath (&thip->dbfd, cwd, newpath, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, &new_parent_dir, &new_rec_addrp,
                     &new_fmd_entry, &new_rec_addr, CNS_NOFOLLOW))
    RETURN (serrno);

  if (old_fmd_entry.fileid == new_fmd_entry.fileid)
    RETURN (0);
  if (old_fmd_entry.fileid == cwd)
    RETURN (EINVAL);

  if (*old_fmd_entry.name == '/' || *new_fmd_entry.name == '/') /* nsrename / */
    RETURN (EINVAL);

  /* If renaming a directory, 'new' must not be a descendant of 'old' */
  if (old_fmd_entry.filemode & S_IFDIR) {
    fileid = new_fmd_entry.parent_fileid;
    while (fileid) {
      if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &tmp_fmd_entry, 0, NULL))
        RETURN (serrno);
      if (old_fmd_entry.fileid == tmp_fmd_entry.fileid)
        RETURN (EINVAL);
      fileid = tmp_fmd_entry.parent_fileid;
    }
  }

  if (new_fmd_entry.fileid) { /* 'new' basename entry exists already */
    new_exists++;

    /* 'old' and 'new' must be of the same type */
    if ((old_fmd_entry.filemode & S_IFDIR) == 0 &&
        new_fmd_entry.filemode & S_IFDIR)
      RETURN (EISDIR);
    if (old_fmd_entry.filemode & S_IFDIR &&
        (new_fmd_entry.filemode & S_IFDIR) == 0)
      RETURN (ENOTDIR);

    /* If the existing 'new' entry is a directory, the directory must be
     * empty
     */
    if (new_fmd_entry.filemode & S_IFDIR && new_fmd_entry.nlink)
      RETURN (EEXIST);

    /* If parent of 'new' has the sticky bit set, the user must own 'new' or
     * the parent of 'new' or the basename entry must have write permission
     */
    if (new_parent_dir.filemode & S_ISVTX  &&
        reqinfo->uid != new_parent_dir.uid &&
        reqinfo->uid != new_fmd_entry.uid  &&
        Cns_chkentryperm (&new_fmd_entry, S_IWRITE, reqinfo->uid, reqinfo->gid,
                          reqinfo->clienthost))
      RETURN (EACCES);
  }

  /* if 'old' is a directory, its basename entry must have write permission */
  if (old_fmd_entry.filemode & S_IFDIR)
    if (Cns_chkentryperm (&old_fmd_entry, S_IWRITE, reqinfo->uid, reqinfo->gid,
                          reqinfo->clienthost))
      RETURN (EACCES);

  /* If parent of 'old' has the sticky bit set, the user must own 'old' or the
   * parent of 'old' or the basename entry must have write permission
   */
  if (old_parent_dir.filemode & S_ISVTX  &&
      reqinfo->uid != old_parent_dir.uid &&
      reqinfo->uid != old_fmd_entry.uid  &&
      Cns_chkentryperm (&old_fmd_entry, S_IWRITE, reqinfo->uid, reqinfo->gid,
                        reqinfo->clienthost))
    RETURN (EACCES);

  if (new_exists) { /* Must remove it */

    /* Delete file segments if any */
    if (Cns_delete_segs(thip, &new_fmd_entry, 0) != 0)
      if (serrno != SEENTRYNFND)
        RETURN (serrno);

    /* If the existing 'new' entry is a symlink, delete it */
    if ((new_fmd_entry.filemode & S_IFLNK) == S_IFLNK) {
      if (Cns_get_lnk_by_fileid (&thip->dbfd, new_fmd_entry.fileid,
                                 &lnk_entry, 1, &rec_addrl))
        RETURN (serrno);
      if (Cns_delete_lnk_entry (&thip->dbfd, &rec_addrl))
        RETURN (serrno);
    }

    /* Delete the comment if it exists */
    if (Cns_get_umd_by_fileid (&thip->dbfd, new_fmd_entry.fileid,
                               &umd_entry, 1, &rec_addru) == 0) {
      if (Cns_delete_umd_entry (&thip->dbfd, &rec_addru))
        RETURN (serrno);
    } else if (serrno != ENOENT)
      RETURN (serrno);

    if (Cns_delete_file_metadata (thip, &old_fmd_entry, &new_rec_addr))
      RETURN (serrno);
  }

  /* Update directory nlink value */
  if (old_parent_dir.fileid != new_parent_dir.fileid) {

    /* Rename across different directories */
    old_parent_dir.nlink--;
    if (!new_exists) {
      new_parent_dir.nlink++;
    }
    new_parent_dir.mtime = time (0);
    new_parent_dir.ctime = new_parent_dir.mtime;
    if (Cns_update_fmd_entry (&thip->dbfd, &new_rec_addrp, &new_parent_dir))
      RETURN (serrno);
  } else if (new_exists) {

    /* Rename within the same directory on an existing file */
    old_parent_dir.nlink--;
  }

  /* Update 'old' basename entry */
  old_fmd_entry.parent_fileid = new_parent_dir.fileid;
  strcpy (old_fmd_entry.name, new_fmd_entry.name);
  old_fmd_entry.ctime = time (0);
  if (Cns_update_fmd_entry (&thip->dbfd, &old_rec_addr, &old_fmd_entry))
    RETURN (serrno);

  /* Update parent directory entry */
  old_parent_dir.mtime = time (0);
  old_parent_dir.ctime = old_parent_dir.mtime;
  if (Cns_update_fmd_entry (&thip->dbfd, &old_rec_addrp, &old_parent_dir))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_updateseg_status - updates the segment status */

int Cns_srv_updateseg_status(char *req_data,
                             struct Cns_srv_thread_info *thip,
                             struct Cns_srv_request_info *reqinfo)
{
  char *rbp;
  char *func = "updateseg_status";
  char newstatus;
  char oldstatus;
  char vid[CA_MAXVIDLEN+1];
  Cns_dbrec_addr rec_addr;
  Cns_dbrec_addr rec_addrs;
  int copyno;
  int count;
  int fsec;
  int fseq;
  int side;
  struct Cns_file_metadata filentry;
  struct Cns_class_metadata class_entry;
  struct Cns_seg_metadata old_smd_entry;
  u_signed64 fileid;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, fileid);
  if (fileid > 0)
    reqinfo->fileid = fileid;
  unmarshall_WORD (rbp, copyno);
  unmarshall_WORD (rbp, fsec);
  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1))
    RETURN (EINVAL);
  unmarshall_WORD (rbp, side);
  unmarshall_LONG (rbp, fseq);
  unmarshall_BYTE (rbp, oldstatus);
  unmarshall_BYTE (rbp, newstatus);

  /* Construct log message */
  sprintf (reqinfo->logbuf,
           "CopyNo=%d Fsec=%d TPVID=%s Side=%d Fseq=%d OldStatus=\"%c\" "
           "NewStatus=\"%c\"",
           copyno, fsec, vid, side, fseq, oldstatus, newstatus);

  /* Check if the user is authorized to set segment status */
  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                  localhost, P_ADMIN))
    RETURN (serrno);

  /* Check for valid status */
  if (((newstatus != '-') && (newstatus != 'D')) || (oldstatus == newstatus))
    RETURN (EINVAL);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Get/lock basename entry */
  if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &filentry, 1, &rec_addr))
    RETURN (serrno);

  /* Check if the entry is a regular file */
  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);

  /* Check if the segment is allowed to be enabled */
  if (filentry.status == 'D')
    RETURN (EPERM);

  /* Get/lock segment metadata entry to be updated */
  if (Cns_get_smd_by_fullid (&thip->dbfd, fileid, copyno, fsec,
                             &old_smd_entry, 1, &rec_addrs))
    RETURN (serrno);

  /* Verify that the old segment metadata is what we expected */
  if (strcmp (old_smd_entry.vid, vid) ||
      (old_smd_entry.side != side) ||
      (old_smd_entry.fseq != fseq) ||
      (old_smd_entry.s_status != oldstatus))
    RETURN (SEENTRYNFND);

  /* Update file segment entry */
  old_smd_entry.s_status = newstatus;
  if (Cns_update_smd_entry (&thip->dbfd, &rec_addrs, &old_smd_entry))
    RETURN (serrno);

  /* Verify that we don't have too many enabled segments for this file */
  if (Cns_get_class_by_id
      (&thip->dbfd, filentry.fileclass, &class_entry, 0, NULL))
    RETURN (serrno);
  if (Cns_get_smd_copy_count_by_pfid
      (&thip->dbfd, filentry.fileid, &count, 1))
    RETURN (serrno);
  if (count > class_entry.nbcopies)
    RETURN (ENSTOOMANYSEGS); /* Too many copies on tape */

  RETURN (0);
}

/* Cns_srv_updateseg_checksum - updates file segment checksum
   when previous value is NULL */

int Cns_srv_updateseg_checksum(int magic,
                               char *req_data,
                               struct Cns_srv_thread_info *thip,
                               struct Cns_srv_request_info *reqinfo)
{
  int copyno;
  u_signed64 fileid;
  struct Cns_file_metadata filentry;
  int fsec;
  int fseq;
  char *func = "updateseg_checksum";
  struct Cns_seg_metadata old_smd_entry;
  char *rbp;
  Cns_dbrec_addr rec_addr;
  Cns_dbrec_addr rec_addrs;
  int side;
  struct Cns_seg_metadata smd_entry;
  char vid[CA_MAXVIDLEN+1];
  int checksum_ok;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, fileid);
  if (fileid > 0)
    reqinfo->fileid = fileid;
  unmarshall_WORD (rbp, copyno);
  unmarshall_WORD (rbp, fsec);
  /* Construct log message */
  sprintf (reqinfo->logbuf, "CopyNo=%d Fsec=%d", copyno, fsec);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Get/lock basename entry */
  if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &filentry, 1, &rec_addr))
    RETURN (serrno);

  /* Check if the entry is a regular file */
  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);

  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1))
    RETURN (EINVAL);
  unmarshall_WORD (rbp, side);
  unmarshall_LONG (rbp, fseq);

  /* Get/lock segment metadata entry to be updated */
  if (Cns_get_smd_by_fullid (&thip->dbfd, fileid, copyno, fsec,
                             &old_smd_entry, 1, &rec_addrs))
    RETURN (serrno);

  if (strcmp (old_smd_entry.vid, vid) || old_smd_entry.side != side ||
      old_smd_entry.fseq != fseq)
    RETURN (SEENTRYNFND);

  nslogit("MSG=\"Old segment information\" REQID=%s NSHOSTNAME=%s "
          "NSFILEID=%llu CopyNo=%d Fsec=%d SegmentSize=%llu Compression=%d "
          "Status=\"%c\" TPVID=%s Side=%d Fseq=%d "
          "BlockId=\"%02x%02x%02x%02x\" ChecksumType=\"%s\" "
          "ChecksumValue=\"%lx\"",
          reqinfo->requuid, nshostname, old_smd_entry.s_fileid,
          old_smd_entry.copyno, old_smd_entry.fsec, old_smd_entry.segsize,
          old_smd_entry.compression, old_smd_entry.s_status, old_smd_entry.vid,
          old_smd_entry.side, old_smd_entry.fseq, old_smd_entry.blockid[0],
          old_smd_entry.blockid[1], old_smd_entry.blockid[2],
          old_smd_entry.blockid[3], old_smd_entry.checksum_name,
          old_smd_entry.checksum);

  /* Checking that the segment has no checksum */
  if (old_smd_entry.checksum_name[0] != '\0') {
    nslogit("MSG=\"Missing checksum information, segment metadata cannot be "
            "updated\" REQID=%s NSHOSTNAME=%s NSFILEID=%llu ",
            reqinfo->requuid, nshostname, old_smd_entry.s_fileid);
    RETURN(EPERM);
  }

  memset ((char *) &smd_entry, 0, sizeof(smd_entry));
  smd_entry.s_fileid = fileid;
  smd_entry.copyno = copyno;
  smd_entry.fsec = fsec;
  smd_entry.segsize = old_smd_entry.segsize;
  smd_entry.compression = old_smd_entry.compression;
  smd_entry.s_status = old_smd_entry.s_status;
  strcpy(smd_entry.vid, old_smd_entry.vid);
  smd_entry.side = old_smd_entry.side;
  smd_entry.fseq = old_smd_entry.fseq;
  memcpy(smd_entry.blockid, old_smd_entry.blockid, 4);
  unmarshall_STRINGN (rbp, smd_entry.checksum_name, CA_MAXCKSUMNAMELEN);
  smd_entry.checksum_name[CA_MAXCKSUMNAMELEN] = '\0';
  unmarshall_LONG (rbp, smd_entry.checksum);

  if (strlen(smd_entry.checksum_name) == 0) {
    checksum_ok = 0;
  } else {
    checksum_ok = 1;
  }

  if (magic >= CNS_MAGIC4) {
    /* Checking that we can't have a NULL checksum name when a checksum is
     * specified
     */
    if (!checksum_ok && smd_entry.checksum != 0) {
      nslogit("MSG=\"No checksum value defined for checksum type, setting "
              "checksum to 0\" REQID=%s NSHOSTNAME=%s NSFILEID=%llu",
              reqinfo->requuid, nshostname, smd_entry.s_fileid);
      smd_entry.checksum = 0;
    }
  }

  nslogit("MSG=\"New segment information\" REQID=%s NSHOSTNAME=%s "
          "NSFILEID=%llu CopyNo=%d Fsec=%d SegmentSize=%llu Compression=%d "
          "Status=\"%c\" TPVID=%s Side=%d Fseq=%d "
          "BlockId=\"%02x%02x%02x%02x\" ChecksumType=\"%s\" "
          "ChecksumValue=\"%lx\"",
          reqinfo->requuid, nshostname, smd_entry.s_fileid, smd_entry.copyno,
          smd_entry.fsec, smd_entry.segsize, smd_entry.compression,
          smd_entry.s_status, smd_entry.vid, smd_entry.side, smd_entry.fseq,
          smd_entry.blockid[0], smd_entry.blockid[1], smd_entry.blockid[2],
          smd_entry.blockid[3], smd_entry.checksum_name, smd_entry.checksum);

  /* Update file segment entry */
  if (Cns_update_smd_entry (&thip->dbfd, &rec_addrs, &smd_entry))
    RETURN (serrno);

  RETURN (0);
}

/* Cns_srv_replaceseg - replace file segment */

int Cns_srv_replaceseg(int magic,
                       char *req_data,
                       struct Cns_srv_thread_info *thip,
                       struct Cns_srv_request_info *reqinfo)
{
  int copyno;
  u_signed64 fileid;
  struct Cns_file_metadata filentry;
  int fsec;
  int fseq;
  char *func = "replaceseg";
  struct Cns_seg_metadata old_smd_entry;
  char *rbp;
  Cns_dbrec_addr rec_addr;
  Cns_dbrec_addr rec_addrs;
  int side;
  struct Cns_seg_metadata smd_entry;
  char vid[CA_MAXVIDLEN+1];
  int checksum_ok;
  time_t last_mod_time = 0;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, fileid);
  if (fileid > 0)
    reqinfo->fileid = fileid;
  if (magic >= CNS_MAGIC5) {
    unmarshall_TIME_T (rbp, last_mod_time);
  }
  unmarshall_WORD (rbp, copyno);
  unmarshall_WORD (rbp, fsec);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "LastModTime=%lld CopyNo=%d Fsec=%d",
           (long long int)last_mod_time, copyno, fsec);

  /* Check if the user is authorized to replace segment attributes */
  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                  localhost, P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Get/lock basename entry */
  if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &filentry, 1, &rec_addr))
    RETURN (serrno);

  /* Check if the entry is a regular file */
  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);

  /* DON'T check for concurrent modifications in another stager:
   * we can be here only from a command line client with admin rights,
   * thus that check is not applicable. */

  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1))
    RETURN (EINVAL);
  unmarshall_WORD (rbp, side);
  unmarshall_LONG (rbp, fseq);

  /* Get/lock segment metadata entry to be updated */
  if (Cns_get_smd_by_fullid (&thip->dbfd, fileid, copyno, fsec,
                             &old_smd_entry, 1, &rec_addrs))
    RETURN (serrno);

  if (strcmp (old_smd_entry.vid, vid) || old_smd_entry.side != side ||
      old_smd_entry.fseq != fseq)
    RETURN (SEENTRYNFND);

  nslogit("MSG=\"Old segment information\" REQID=%s NSHOSTNAME=%s "
          "NSFILEID=%llu CopyNo=%d Fsec=%d SegmentSize=%llu Compression=%d "
          "Status=\"%c\" TPVID=%s Side=%d Fseq=%d "
          "BlockId=\"%02x%02x%02x%02x\" ChecksumType=\"%s\" "
          "ChecksumValue=\"%lx\"",
          reqinfo->requuid, nshostname, old_smd_entry.s_fileid,
          old_smd_entry.copyno, old_smd_entry.fsec, old_smd_entry.segsize,
          old_smd_entry.compression, old_smd_entry.s_status, old_smd_entry.vid,
          old_smd_entry.side, old_smd_entry.fseq, old_smd_entry.blockid[0],
          old_smd_entry.blockid[1], old_smd_entry.blockid[2],
          old_smd_entry.blockid[3], old_smd_entry.checksum_name,
          old_smd_entry.checksum);

  memset ((char *) &smd_entry, 0, sizeof(smd_entry));
  smd_entry.s_fileid = fileid;
  smd_entry.copyno = copyno;
  smd_entry.fsec = fsec;
  unmarshall_LONG (rbp, smd_entry.compression);
  smd_entry.s_status = old_smd_entry.s_status;
  if (unmarshall_STRINGN (rbp, smd_entry.vid, CA_MAXVIDLEN+1))
    RETURN (EINVAL);
  unmarshall_WORD (rbp, smd_entry.side);
  unmarshall_LONG (rbp, smd_entry.fseq);
  unmarshall_OPAQUE (rbp, smd_entry.blockid, 4);
  if (magic >= CNS_MAGIC3) {
    unmarshall_STRINGN (rbp, smd_entry.checksum_name, CA_MAXCKSUMNAMELEN);
    smd_entry.checksum_name[CA_MAXCKSUMNAMELEN] = '\0';
    unmarshall_LONG (rbp, smd_entry.checksum);
  } else {
    smd_entry.checksum_name[0] = '\0';
    smd_entry.checksum = 0;
  }
  if (magic >= CNS_MAGIC6) {
    unmarshall_HYPER (rbp, smd_entry.segsize);
  } else {
    smd_entry.segsize = old_smd_entry.segsize;
  }

  if (strlen(smd_entry.checksum_name) == 0) {
    checksum_ok = 0;
  } else {
    checksum_ok = 1;
  }

  if (magic >= CNS_MAGIC4) {

    /* Checking that we can't have a NULL checksum name when a checksum is
     * specified
     */
    if (!checksum_ok && smd_entry.checksum != 0) {
      nslogit("MSG=\"No checksum value defined for checksum type, setting "
              "checksum to 0\" REQID=%s NSHOSTNAME=%s NSFILEID=%llu",
              reqinfo->requuid, nshostname, smd_entry.s_fileid);
      smd_entry.checksum = 0;
    }
  }

  nslogit("MSG=\"New segment information\" REQID=%s NSHOSTNAME=%s "
          "NSFILEID=%llu CopyNo=%d Fsec=%d SegmentSize=%llu Compression=%d "
          "Status=\"%c\" TPVID=%s Side=%d Fseq=%d "
          "BlockId=\"%02x%02x%02x%02x\" ChecksumType=\"%s\" "
          "ChecksumValue=\"%lx\"",
          reqinfo->requuid, nshostname, smd_entry.s_fileid, smd_entry.copyno,
          smd_entry.fsec, smd_entry.segsize, smd_entry.compression,
          smd_entry.s_status, smd_entry.vid, smd_entry.side, smd_entry.fseq,
          smd_entry.blockid[0], smd_entry.blockid[1], smd_entry.blockid[2],
          smd_entry.blockid[3], smd_entry.checksum_name, smd_entry.checksum);

  /* Update file segment entry */
  if (Cns_update_smd_entry (&thip->dbfd, &rec_addrs, &smd_entry))
    RETURN (serrno);

  RETURN (0);
}

/* Cns_srv_rmdir - remove a directory entry */

int Cns_srv_rmdir(char *req_data,
                  struct Cns_srv_thread_info *thip,
                  struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  struct Cns_file_metadata direntry;
  char *func = "rmdir";
  struct Cns_file_metadata parent_dir;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  Cns_dbrec_addr rec_addrp;
  Cns_dbrec_addr rec_addru;
  struct Cns_user_metadata umd_entry;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check parent directory components for write/search permission and get/lock
   * basename entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, &parent_dir, &rec_addrp, &direntry,
                     &rec_addr, CNS_MUST_EXIST|CNS_NOFOLLOW))
    RETURN (serrno);

  if (*direntry.name == '/') /* Cns_rmdir / */
    RETURN (EINVAL);

  if ((direntry.filemode & S_IFDIR) == 0)
    RETURN (ENOTDIR);
  if (direntry.fileid == cwd)
    RETURN (EINVAL); /* Cannot remove current working directory */
  if (direntry.nlink)
    RETURN (EEXIST);

  /* If the parent has the sticky bit set, the user must own the directory or
   * the parent or the basename entry must have write permission
   */
  if (parent_dir.filemode & S_ISVTX &&
      reqinfo->uid != parent_dir.uid && reqinfo->uid != direntry.uid &&
      Cns_chkentryperm (&direntry, S_IWRITE, reqinfo->uid, reqinfo->gid,
                        reqinfo->clienthost))
    RETURN (EACCES);

  /* Delete the comment if it exists */
  if (Cns_get_umd_by_fileid (&thip->dbfd, direntry.fileid, &umd_entry, 1,
                             &rec_addru) == 0) {
    if (Cns_delete_umd_entry (&thip->dbfd, &rec_addru))
      RETURN (serrno);
  } else if (serrno != ENOENT)
    RETURN (serrno);

  /* Delete directory entry */
  if (Cns_delete_fmd_entry (&thip->dbfd, &rec_addr))
    RETURN (serrno);

  /* Update parent directory entry */
  parent_dir.nlink--;
  parent_dir.mtime = time (0);
  parent_dir.ctime = parent_dir.mtime;
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_setacl - set the Access Control List for a file/directory */

int Cns_srv_setacl(char *req_data,
                   struct Cns_srv_thread_info *thip,
                   struct Cns_srv_request_info *reqinfo)
{
  struct Cns_acl acl[CA_MAXACLENTRIES];
  struct Cns_acl *aclp;
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  char *func = "setacl";
  int i;
  char *iacl;
  int nentries;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addr;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  unmarshall_WORD (rbp, nentries);
  if (nentries > CA_MAXACLENTRIES)
    RETURN (EINVAL);
  for (i = 0, aclp = acl; i < nentries; i++, aclp++) {
    unmarshall_BYTE (rbp, aclp->a_type);
    unmarshall_LONG (rbp, aclp->a_id);
    unmarshall_BYTE (rbp, aclp->a_perm);
  }

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check parent directory components for search permission and get/lock
   * basename entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, NULL, NULL, &fmd_entry, &rec_addr,
                     CNS_MUST_EXIST))
    RETURN (serrno);

  /* Record the fileid being processed */
  reqinfo->fileid = fmd_entry.fileid;

  /* Check if the user is authorized to setacl for this entry */
  if (reqinfo->uid != fmd_entry.uid &&
      Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                  localhost, P_GRP_ADMIN))
    RETURN (EPERM);

  qsort (acl, nentries, sizeof(struct Cns_acl), Cns_acl_compare);
  for (i = 0, aclp = acl; i < nentries; i++, aclp++) {
    if (aclp->a_type == CNS_ACL_USER_OBJ)
      aclp->a_id = fmd_entry.uid;
    else if (aclp->a_type == CNS_ACL_GROUP_OBJ)
      aclp->a_id = fmd_entry.gid;
    else if ((aclp->a_type & CNS_ACL_DEFAULT) &&
             (fmd_entry.filemode & S_IFDIR) == 0)
      RETURN (EINVAL);
  }
  if (Cns_acl_validate (acl, nentries))
    RETURN (EINVAL);

  /* Build access ACL */
  iacl = fmd_entry.acl;
  if (nentries == 3) {         /* No extended ACL, just update filemode */
    *iacl = '\0';
    for (i = 0, aclp = acl; i < nentries; i++, aclp++) {
      switch (aclp->a_type) {
      case CNS_ACL_USER_OBJ:
        fmd_entry.filemode = (fmd_entry.filemode & 0177077) |
          (aclp->a_perm << 6);
        break;
      case CNS_ACL_GROUP_OBJ:
        fmd_entry.filemode = (fmd_entry.filemode & 0177707) |
          (aclp->a_perm << 3);
        break;
      case CNS_ACL_OTHER:
        fmd_entry.filemode = (fmd_entry.filemode & 0177770) |
          (aclp->a_perm);
        break;
      }
    }
  } else {
    for (i = 0, aclp = acl; i < nentries; i++, aclp++) {
      if (iacl != fmd_entry.acl)
        *iacl++ = ',';
      *iacl++ = aclp->a_type + '@';
      *iacl++ = aclp->a_perm + '0';
      iacl += sprintf (iacl, "%d", aclp->a_id);
      switch (aclp->a_type) {
      case CNS_ACL_USER_OBJ:
        fmd_entry.filemode = (fmd_entry.filemode & 0177077) |
          (aclp->a_perm << 6);
        break;
      case CNS_ACL_GROUP_OBJ:
        fmd_entry.filemode = (fmd_entry.filemode & 0177707) |
          (aclp->a_perm << 3);
        break;
      case CNS_ACL_MASK:
        fmd_entry.filemode = (fmd_entry.filemode & ~070) |
          (fmd_entry.filemode & (aclp->a_perm << 3));
        break;
      case CNS_ACL_OTHER:
        fmd_entry.filemode = (fmd_entry.filemode & 0177770) |
          (aclp->a_perm);
        break;
      }
    }
  }

  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &fmd_entry))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_setatime - set last access time */

int Cns_srv_setatime(char *req_data,
                     struct Cns_srv_thread_info *thip,
                     struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  u_signed64 fileid;
  struct Cns_file_metadata filentry;
  char *func = "setatime";
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addr;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (fileid > 0)
    reqinfo->fileid = fileid;
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  if (fileid) {

    /* Get/lock basename entry */
    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &filentry, 1, &rec_addr))
      RETURN (serrno);

    /* Check parent directory components for search permission */
    if (Cns_chkbackperm (&thip->dbfd, filentry.parent_fileid,
                         reqinfo->uid, reqinfo->gid, reqinfo->clienthost))
      RETURN (serrno);
  } else {

    /* Check parent directory components for search permission and get/lock
     * basename entry
     */
    if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                       reqinfo->clienthost, NULL, NULL, &filentry, &rec_addr,
                       CNS_MUST_EXIST))
      RETURN (serrno);

    /* Record the fileid being processed */
    reqinfo->fileid = filentry.fileid;
  }

  /* Check if the entry is a regular file and if the user is authorized to set
   * access time for this entry
   */
  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);
  if (reqinfo->uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IREAD, reqinfo->uid, reqinfo->gid,
                        reqinfo->clienthost))
    RETURN (EACCES);

  /* Update entry */
  filentry.atime = time (0);

  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_setcomment - add/replace a comment associated with a file/directory */

int Cns_srv_setcomment(char *req_data,
                       struct Cns_srv_thread_info *thip,
                       struct Cns_srv_request_info *reqinfo)
{
  char comment[CA_MAXCOMMENTLEN+1];
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char *func = "setcomment";
  struct Cns_user_metadata old_umd_entry;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addru;
  struct Cns_user_metadata umd_entry;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  if (unmarshall_STRINGN (rbp, comment, CA_MAXCOMMENTLEN+1))
    RETURN (EINVAL);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Ccheck parent directory components for search permission and get basename
   * entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, NULL, NULL, &filentry, NULL,
                     CNS_MUST_EXIST))
    RETURN (serrno);

  /* Record the fileid being processed */
  reqinfo->fileid = filentry.fileid;

  /* Check if the user is authorized to add/replace the comment on this
   * entry
   */
  if (reqinfo->uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, reqinfo->uid, reqinfo->gid,
                        reqinfo->clienthost))
    RETURN (EACCES);

  if (*comment) { /* Add the comment or replace the comment if it exists */
    memset ((char *) &umd_entry, 0, sizeof(umd_entry));
    umd_entry.u_fileid = filentry.fileid;
    strcpy (umd_entry.comments, comment);
    if (Cns_insert_umd_entry (&thip->dbfd, &umd_entry)) {
      if (serrno != EEXIST ||
          Cns_get_umd_by_fileid (&thip->dbfd, filentry.fileid,
                                 &old_umd_entry, 1, &rec_addru) ||
          Cns_update_umd_entry (&thip->dbfd, &rec_addru, &umd_entry))
        RETURN (serrno);
    }
  } else { /* Delete the comment if it exists */
    if (Cns_get_umd_by_fileid (&thip->dbfd, filentry.fileid,
                               &old_umd_entry, 1, &rec_addru)) {
      if (serrno != ENOENT)
        RETURN (serrno);
    } else if (Cns_delete_umd_entry (&thip->dbfd, &rec_addru))
      RETURN (serrno);
  }
  RETURN (0);
}

/* Cns_srv_setfsize - set file size and last modification time */

int Cns_srv_setfsize(int magic,
                     char *req_data,
                     struct Cns_srv_thread_info *thip,
                     struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  u_signed64 fileid;
  struct Cns_file_metadata filentry;
  u_signed64 filesize;
  char *func = "setfsize";
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  time_t last_mod_time = 0;
  time_t new_mtime = 0;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (fileid > 0)
    reqinfo->fileid = fileid;
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_HYPER (rbp, filesize);
  if (magic >= CNS_MAGIC2) {
    unmarshall_TIME_T (rbp, new_mtime);
    unmarshall_TIME_T (rbp, last_mod_time);
  }

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf,
           "Cwd=\"%s\" Path=\"%s\" FileSize=%llu NewModTime=%lld "
           "LastModTime=%lld",
           cwdpath, path, filesize, (long long int)new_mtime,
           (long long int)last_mod_time);

  /* Check if the user is authorized to set the file size of a file */
  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                  localhost, P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  if (fileid) {

    /* Get/lock basename entry */
    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &filentry, 1, &rec_addr))
      RETURN (serrno);

    /* Check parent directory components for search permission */
    if (Cns_chkbackperm (&thip->dbfd, filentry.parent_fileid,
                         reqinfo->uid, reqinfo->gid, reqinfo->clienthost))
      RETURN (serrno);
  } else {

    /* Check parent directory components for search permission and get/lock
     * basename entry
     */
    if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                       reqinfo->clienthost, NULL, NULL, &filentry, &rec_addr,
                       CNS_MUST_EXIST))
      RETURN (serrno);

    /* Record the fileid being processed */
    reqinfo->fileid = filentry.fileid;
  }

  /* Check if the entry is a regular file and if the user is authorized to set
   * modification time for this entry
   */
  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);
  if (reqinfo->uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, reqinfo->uid, reqinfo->gid,
                        reqinfo->clienthost))
    RETURN (EACCES);

  /* Check for concurrent modifications */
  if (Cns_is_concurrent_open(&thip->dbfd, &filentry, last_mod_time, reqinfo->logbuf))
    RETURN (serrno);

  /* Update entry */
  filentry.filesize = filesize;
  if (magic >= CNS_MAGIC2) {
    filentry.mtime = new_mtime;
  } else {
    filentry.mtime = time (0);
  }
  filentry.ctime = filentry.mtime;
  *filentry.csumtype = '\0';
  *filentry.csumvalue = '\0';

  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_setfsizecs - set file size, last modification time and file checksum*/

int Cns_srv_setfsizecs(int magic,
                       char *req_data,
                       struct Cns_srv_thread_info *thip,
                       struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  u_signed64 fileid;
  struct Cns_file_metadata filentry;
  u_signed64 filesize;
  char *func = "setfsizecs";
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  char csumtype[3];
  char csumvalue[CA_MAXCKSUMLEN+1];
  time_t last_mod_time = 0;
  time_t new_mtime = 0;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (fileid > 0)
    reqinfo->fileid = fileid;
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_HYPER (rbp, filesize);
  if (unmarshall_STRINGN (rbp, csumtype, 3))
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, csumvalue, CA_MAXCKSUMLEN+1))
    RETURN (EINVAL);
  if (magic >= CNS_MAGIC2) {
    unmarshall_TIME_T (rbp, new_mtime);
    unmarshall_TIME_T (rbp, last_mod_time);
  }

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf,
           "Cwd=\"%s\" Path=\"%s\" FileSize=%llu ChecksumType=\"%s\" "
           "ChecksumValue=\"%s\" NewModTime=%lld LastModTime=%lld",
           cwdpath, path, filesize, csumtype, csumvalue,
           (long long int)new_mtime, (long long int)last_mod_time);

  if (*csumtype &&
      (strcmp (csumtype, "AD") && strcmp (csumtype, "PA")))
    RETURN (EINVAL);

  /* Check if the user is authorized to set the file size of a file */
  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                  localhost, P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  if (fileid) {

    /* Get/lock basename entry */
    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &filentry, 1, &rec_addr))
      RETURN (serrno);

    /* Check parent directory components for search permission */
    if (Cns_chkbackperm (&thip->dbfd, filentry.parent_fileid,
                         reqinfo->uid, reqinfo->gid, reqinfo->clienthost))
      RETURN (serrno);
  } else {

    /* Check parent directory components for search permission and get/lock
     * basename entry
     */
    if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                       reqinfo->clienthost, NULL, NULL, &filentry, &rec_addr,
                       CNS_MUST_EXIST))
      RETURN (serrno);

    /* Record the fileid being processed */
    reqinfo->fileid = filentry.fileid;
  }

  /* Check if the entry is a regular file and if the user is authorized to set
   * modification time for this entry
   */
  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);
  if (reqinfo->uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, reqinfo->uid, reqinfo->gid,
                        reqinfo->clienthost))
    RETURN (EACCES);

  /* Check for concurrent modifications */
  if (Cns_is_concurrent_open(&thip->dbfd, &filentry, last_mod_time, reqinfo->logbuf))
    RETURN (serrno);

  if ((strcmp(filentry.csumtype, "PA") == 0 && strcmp(csumtype, "AD") == 0)) {
    /* We have predefined checksums then should check them with new ones */
    if (strcmp(filentry.csumvalue,csumvalue)!=0) {
      nslogit("MSG=\"Predefined file checksum mismatch\" REQID=%s "
              "NSHOSTNAME=%s NSFILEID=%llu NewChecksum=\"0x%s\" "
              "ExpectedChecksum=\"0x%s\"",
              reqinfo->requuid, nshostname, filentry.fileid, csumvalue,
              filentry.csumvalue);
      RETURN (SECHECKSUM);
    }
  }

  /* Update entry */
  filentry.filesize = filesize;
  if (magic >= CNS_MAGIC2) {
    filentry.mtime = new_mtime;
  } else {
    filentry.mtime = time (0);
  }
  filentry.ctime = filentry.mtime;
  strcpy (filentry.csumtype, csumtype);
  strcpy (filentry.csumvalue, csumvalue);

  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_dropsegs - drops all segments of a file */

int Cns_srv_dropsegs(char *req_data,
                     struct Cns_srv_thread_info *thip,
                     struct Cns_srv_request_info *reqinfo)
{
  char       *func = "dropsegs";
  char       *rbp;
  char       path[CA_MAXPATHLEN+1];
  char       cwdpath[CA_MAXPATHLEN+1];
  u_signed64 cwd;
  u_signed64 fileid = 0;
  struct Cns_file_metadata fmd_entry;
  Cns_dbrec_addr rec_addr;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (fileid > 0)
    reqinfo->fileid = fileid;
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  /* Check if the user is authorized to drop segments */
  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                  localhost, P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  if (fileid) {

    /* Get/lock basename entry */
    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &fmd_entry, 1, &rec_addr))
      RETURN (serrno);
  } else {

    /* Check parent directory components for search permission and get/lock
     * basename entry
     */
    if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                       reqinfo->clienthost, NULL, NULL, &fmd_entry, &rec_addr,
                       CNS_MUST_EXIST))
      RETURN (serrno);

    /* Record the fileid being processed */
    reqinfo->fileid = fmd_entry.fileid;
  }

  /* Check if the entry is a regular file */
  if (fmd_entry.filemode & S_IFDIR)
    RETURN (EISDIR);
  if ((fmd_entry.filemode & S_IFREG) != S_IFREG)
    RETURN (SEINTERNAL);
  if (*fmd_entry.name == '/')
    RETURN (SEINTERNAL);

  /* Delete file segments */
  if (Cns_delete_segs(thip, &fmd_entry, 0) != 0)
    if (serrno != SEENTRYNFND)
      RETURN (serrno);

  RETURN (0);
}

/* Cns_srv_startsess - start session */

int Cns_srv_startsess(char *req_data,
                      struct Cns_srv_thread_info *thip,
                      struct Cns_srv_request_info *reqinfo)
{
  char comment[CA_MAXCOMMENTLEN+1];
  char *func = "startsess";
  char *rbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN (rbp, comment, CA_MAXCOMMENTLEN+1))
    RETURN (EINVAL);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "Comment=\"%s\"", comment);
  RETURN (0);
}

/* Cns_srv_starttrans - start transaction mode */

int Cns_srv_starttrans(int magic,
                       char *req_data,
                       struct Cns_srv_thread_info *thip,
                       struct Cns_srv_request_info *reqinfo)
{
  char comment[CA_MAXCOMMENTLEN+1];
  char *func = "starttrans";
  char *rbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (magic >= CNS_MAGIC2) {
    if (unmarshall_STRINGN (rbp, comment, CA_MAXCOMMENTLEN+1))
      RETURN (EINVAL);

    /* Construct log message */
    sprintf (reqinfo->logbuf, "Comment=\"%s\"", comment);
  }

  (void) Cns_start_tr (&thip->dbfd);
  thip->dbfd.tr_mode++;
  RETURN (0);
}

/* Cns_srv_stat - get information about a file or a directory */

int Cns_srv_stat(char *req_data,
                 struct Cns_srv_thread_info *thip,
                 struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  u_signed64 fileid;
  struct Cns_file_metadata fmd_entry;
  char *func = "stat";
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  char repbuf[57];
  char *sbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (fileid > 0)
    reqinfo->fileid = fileid;
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  if (fileid) {

    /* Get basename entry */
    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &fmd_entry, 0, NULL))
      RETURNQ (serrno);

    /* Check parent directory components for search permission */
    if (Cns_chkbackperm (&thip->dbfd, fmd_entry.parent_fileid,
                         reqinfo->uid, reqinfo->gid, reqinfo->clienthost))
      RETURNQ (serrno);
  } else {

    /* Check parent directory components for search permission and get basename
     * entry
     */
    if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                       reqinfo->clienthost, NULL, NULL, &fmd_entry, NULL,
                       CNS_MUST_EXIST))
      RETURNQ (serrno);

    /* Record the fileid being processed */
    reqinfo->fileid = fmd_entry.fileid;
  }

  sbp = repbuf;
  marshall_HYPER (sbp, fmd_entry.fileid);
  marshall_WORD (sbp, fmd_entry.filemode);
  marshall_LONG (sbp, fmd_entry.nlink);
  marshall_LONG (sbp, fmd_entry.uid);
  marshall_LONG (sbp, fmd_entry.gid);
  marshall_HYPER (sbp, fmd_entry.filesize);
  marshall_TIME_T (sbp, fmd_entry.atime);
  marshall_TIME_T (sbp, fmd_entry.mtime);
  marshall_TIME_T (sbp, fmd_entry.ctime);
  marshall_WORD (sbp, fmd_entry.fileclass);
  marshall_BYTE (sbp, fmd_entry.status);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_statcs - get information about a file or a directory */

int Cns_srv_statcs(char *req_data,
                   struct Cns_srv_thread_info *thip,
                   struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  u_signed64 fileid;
  struct Cns_file_metadata fmd_entry;
  char *func = "statcs";
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  char repbuf[93];
  char *sbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (fileid > 0)
    reqinfo->fileid = fileid;
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  if (fileid) {

    /* Get basename entry */
    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &fmd_entry, 0, NULL))
      RETURNQ (serrno);

    /* Check parent directory components for search permission */
    if (Cns_chkbackperm (&thip->dbfd, fmd_entry.parent_fileid,
                         reqinfo->uid, reqinfo->gid, reqinfo->clienthost))
      RETURNQ (serrno);
  } else {

    /* Check parent directory components for search permission and get basename
     * entry
     */
    if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                       reqinfo->clienthost, NULL, NULL, &fmd_entry, NULL,
                       CNS_MUST_EXIST))
      RETURNQ (serrno);

    /* Record the fileid being processed */
    reqinfo->fileid = fmd_entry.fileid;
  }

  sbp = repbuf;
  marshall_HYPER (sbp, fmd_entry.fileid);
  marshall_WORD (sbp, fmd_entry.filemode);
  marshall_LONG (sbp, fmd_entry.nlink);
  marshall_LONG (sbp, fmd_entry.uid);
  marshall_LONG (sbp, fmd_entry.gid);
  marshall_HYPER (sbp, fmd_entry.filesize);
  marshall_TIME_T (sbp, fmd_entry.atime);
  marshall_TIME_T (sbp, fmd_entry.mtime);
  marshall_TIME_T (sbp, fmd_entry.ctime);
  marshall_WORD (sbp, fmd_entry.fileclass);
  marshall_BYTE (sbp, fmd_entry.status);
  marshall_STRING (sbp, fmd_entry.csumtype);
  marshall_STRING (sbp, fmd_entry.csumvalue);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_statg - get information about a file or a directory */

int Cns_srv_statg(char *req_data,
                  struct Cns_srv_thread_info *thip,
                  struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  char *func = "statg";
  char guid[CA_MAXGUIDLEN+1];
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  char repbuf[130];
  char *sbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);
  if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN+1))
    RETURNQ (EINVAL);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\" Guid=\"%s\"",
           cwdpath, path, guid);

  if (*path) {

    /* Check parent directory components for search permission and get basename
     * entry
     */
    if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                       reqinfo->clienthost, NULL, NULL, &fmd_entry, NULL,
                       CNS_MUST_EXIST))
      RETURNQ (serrno);
    if (*guid && strcmp (guid, fmd_entry.guid))
      RETURNQ (EINVAL);
  } else {
    if (! *guid)
      RETURNQ (ENOENT);

    /* Get basename entry */
    if (Cns_get_fmd_by_guid (&thip->dbfd, guid, &fmd_entry, 0, NULL))
      RETURNQ (serrno);

    /* Do not check parent directory components for search permission as
     * symlinks can anyway point directly at a file
     */
  }

  /* Record the fileid being processed */
  reqinfo->fileid = fmd_entry.fileid;

  sbp = repbuf;
  marshall_HYPER (sbp, fmd_entry.fileid);
  marshall_STRING (sbp, fmd_entry.guid);
  marshall_WORD (sbp, fmd_entry.filemode);
  marshall_LONG (sbp, fmd_entry.nlink);
  marshall_LONG (sbp, fmd_entry.uid);
  marshall_LONG (sbp, fmd_entry.gid);
  marshall_HYPER (sbp, fmd_entry.filesize);
  marshall_TIME_T (sbp, fmd_entry.atime);
  marshall_TIME_T (sbp, fmd_entry.mtime);
  marshall_TIME_T (sbp, fmd_entry.ctime);
  marshall_WORD (sbp, fmd_entry.fileclass);
  marshall_BYTE (sbp, fmd_entry.status);
  marshall_STRING (sbp, fmd_entry.csumtype);
  marshall_STRING (sbp, fmd_entry.csumvalue);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_symlink - make a symbolic link to a file or a directory */

int Cns_srv_symlink(char *req_data,
                    struct Cns_srv_thread_info *thip,
                    struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  char *func = "symlink";
  struct Cns_file_metadata fmd_entry;
  char linkname[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  struct Cns_symlinks lnk_entry;
  struct Cns_file_metadata parent_dir;
  char *rbp;
  Cns_dbrec_addr rec_addrp;
  char target[CA_MAXPATHLEN+1];

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, target, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  if (unmarshall_STRINGN (rbp, linkname, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Target=\"%s\" LinkName=\"%s\"",
           cwdpath, target, linkname);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check parent directory components for write/search permission and get
   * basename entry if it exists
   */
  if (Cns_parsepath (&thip->dbfd, cwd, linkname, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, &parent_dir, &rec_addrp, &fmd_entry,
                     NULL, CNS_NOFOLLOW))
    RETURN (serrno);

  /* Check if 'linkname' basename entry exists already */
  if (fmd_entry.fileid)
    RETURN (EEXIST);

  /* Build new entry */
  if (Cns_unique_id (&thip->dbfd, &fmd_entry.fileid) < 0)
    RETURN (serrno);

  /* parent_fileid and name have been set by Cns_parsepath */
  fmd_entry.filemode = S_IFLNK | 0777;
  fmd_entry.nlink = 1;
  fmd_entry.uid = reqinfo->uid;
  if (parent_dir.filemode & S_ISGID)
    fmd_entry.gid = parent_dir.gid;
  else
    fmd_entry.gid = reqinfo->gid;
  fmd_entry.atime = time (0);
  fmd_entry.mtime = fmd_entry.atime;
  fmd_entry.ctime = fmd_entry.atime;
  fmd_entry.fileclass = 0;
  fmd_entry.status = '-';

  memset ((char *) &lnk_entry, 0, sizeof(lnk_entry));
  lnk_entry.fileid = fmd_entry.fileid;
  strcpy (lnk_entry.linkname, target);

  /* Write new entry */
  if (Cns_insert_fmd_entry (&thip->dbfd, &fmd_entry))
    RETURN (serrno);
  if (Cns_insert_lnk_entry (&thip->dbfd, &lnk_entry))
    RETURN (serrno);

  /* Update parent directory entry */
  parent_dir.nlink++;
  parent_dir.mtime = time (0);
  parent_dir.ctime = parent_dir.mtime;
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_undelete - logically restore a file entry */

int Cns_srv_undelete(char *req_data,
                     struct Cns_srv_thread_info *thip,
                     struct Cns_srv_request_info *reqinfo)
{
  int bof = 1;
  int c;
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char *func = "undelete";
  struct Cns_file_metadata parent_dir;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addr;  /* File record address */
  Cns_dbrec_addr rec_addrp; /* Parent record address */
  Cns_dbrec_addr rec_addrs; /* Segment record address */
  struct Cns_seg_metadata smd_entry;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check parent directory components for write/search permission and get/lock
   * basename entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, &parent_dir, &rec_addrp, &filentry,
                     &rec_addr, CNS_MUST_EXIST|CNS_NOFOLLOW))
    RETURN (serrno);

  /* Record the fileid being processed */
  reqinfo->fileid = filentry.fileid;

  if (*filentry.name == '/') /* Cns_undelete / */
    RETURN (EINVAL);

  if (filentry.filemode & S_IFDIR)
    RETURN (EPERM);

  /* If the parent has the sticky bit set, the user must own the file or the
   * parent or the basename entry must have write permission
   */
  if (parent_dir.filemode & S_ISVTX &&
      reqinfo->uid != parent_dir.uid && reqinfo->uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, reqinfo->uid, reqinfo->gid,
                        reqinfo->clienthost))
    RETURN (EACCES);

  /* Remove the mark "logically deleted" on the file segments if any */
  while ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid,
                                   &smd_entry, 1, &rec_addrs, 0)) == 0) {
    smd_entry.s_status = '-';
    if (Cns_update_smd_entry (&thip->dbfd, &rec_addrs, &smd_entry))
      RETURN (serrno);
    bof = 0;
  }
  (void) Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid,
                              &smd_entry, 1, &rec_addrs, 1);
  if (c < 0)
    RETURN (serrno);

  /* Remove the mark "logically deleted" */
  if (bof)
    filentry.status = '-';
  else
    filentry.status = 'm';
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
    RETURN (serrno);

  /* Update parent directory entry */
  parent_dir.mtime = time (0);
  parent_dir.ctime = parent_dir.mtime;
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_unlink - remove a file entry */

int Cns_srv_unlink(char *req_data,
                   struct Cns_srv_thread_info *thip,
                   struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char *func = "unlink";
  struct Cns_symlinks lnk_entry;
  struct Cns_file_metadata parent_dir;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addr;  /* File record address */
  Cns_dbrec_addr rec_addrl; /* Link record address */
  Cns_dbrec_addr rec_addrp; /* Parent record address */
  Cns_dbrec_addr rec_addru; /* Comment record address */
  struct Cns_user_metadata umd_entry;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check parent directory components for write/search permission and get/lock
   * basename entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, &parent_dir, &rec_addrp, &filentry,
                     &rec_addr, CNS_MUST_EXIST|CNS_NOFOLLOW))
    RETURN (serrno);

  /* Record the fileid being processed */
  reqinfo->fileid = filentry.fileid;

  if (*filentry.name == '/') /* Cns_unlink / */
    RETURN (EINVAL);

  if (filentry.filemode & S_IFDIR)
    RETURN (EPERM);

  /* If the parent has the sticky bit set, the user must own the file or the
   * parent or the basename entry must have write permission
   */
  if (parent_dir.filemode & S_ISVTX &&
      reqinfo->uid != parent_dir.uid && reqinfo->uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, reqinfo->uid, reqinfo->gid,
                        reqinfo->clienthost))
    RETURN (EACCES);

  if ((filentry.filemode & S_IFLNK) == S_IFLNK) {
    if (Cns_get_lnk_by_fileid (&thip->dbfd, filentry.fileid,
                               &lnk_entry, 1, &rec_addrl))
      RETURN (serrno);
    if (Cns_delete_lnk_entry (&thip->dbfd, &rec_addrl))
      RETURN (serrno);
  } else {

    /* Delete file segments if any */
    if (Cns_delete_segs(thip, &filentry, 0) != 0)
      if (serrno != SEENTRYNFND)
        RETURN (serrno);

    /* Delete the comment if it exists */
    if (Cns_get_umd_by_fileid (&thip->dbfd, filentry.fileid, &umd_entry, 1,
                               &rec_addru) == 0) {
      if (Cns_delete_umd_entry (&thip->dbfd, &rec_addru))
        RETURN (serrno);
    } else if (serrno != ENOENT)
      RETURN (serrno);
  }

  /* Delete file entry */
  if (Cns_delete_file_metadata (thip, &filentry, &rec_addr))
    RETURN (serrno);

  /* Update parent directory entry */
  parent_dir.nlink--;
  parent_dir.mtime = time (0);
  parent_dir.ctime = parent_dir.mtime;
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_unlinkbyvid - remove all file entries on a given volume */

int Cns_srv_unlinkbyvid(char *req_data,
                        struct Cns_srv_thread_info *thip,
                        struct Cns_srv_request_info *reqinfo)
{
  char  vid[CA_MAXVIDLEN+22];
  char  *func = "unlinkbyvid";
  char  path[CA_MAXPATHLEN+1];
  char  *rbp;
  char  *p;
  int   c;
  int   bov;
  int   count;
  struct Cns_seg_metadata  smd_entry;
  struct Cns_file_metadata fmd_entry;
  struct Cns_file_metadata dir_entry;
  struct Cns_user_metadata umd_entry;
  Cns_dbrec_addr rec_addr;
  Cns_dbrec_addr rec_addrp;
  Cns_dbrec_addr rec_addru;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  if (unmarshall_STRINGN(rbp, vid, CA_MAXVIDLEN + 1)) {
    RETURN (SENAMETOOLONG);
  }

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  sprintf (reqinfo->logbuf, "TPVID=%s", vid);

  /* Check if the user is authorized to unlink files by vid */
  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                  localhost, P_ADMIN))
    RETURN (serrno);

  /* Delete the files on the volume */
  bov   = 1;
  count = 0;
  while ((c = Cns_get_smd_by_vid(&thip->dbfd, bov, vid, 0,
                                 &smd_entry, 0)) == 0) {

    /* To avoid holding too many locks in the namespace we commit each file
     * unlink.
     */
    (void) Cns_start_tr (&thip->dbfd);

    /* Lock the file and parent directory entries */
    bov = 0;
    if (Cns_get_fmd_by_fileid (&thip->dbfd, smd_entry.s_fileid,
                               &fmd_entry, 1, &rec_addr) < 0) {
      if (serrno == ENOENT)
        continue;
      RETURNQ (serrno);
    }

    /* Make sure this is a regular file */
    if ((fmd_entry.filemode & S_IFREG) != S_IFREG)
      RETURNQ (SEINTERNAL);
    if (*fmd_entry.name == '/')
      RETURNQ (SEINTERNAL);

    if (Cns_get_fmd_by_fileid (&thip->dbfd, fmd_entry.parent_fileid,
                               &dir_entry, 1, &rec_addrp) < 0) {
      if (serrno == ENOENT)
        continue;
      RETURNQ (serrno);
    }

    /* Resolve fileid to filepath */
    p = path;
    if (Cns_getpath_by_fileid (&thip->dbfd, fmd_entry.fileid, &p))
      RETURNQ (serrno);

    /* Delete file segments if any */
    if (Cns_delete_segs(thip, &fmd_entry, 0) != 0) {
      if (serrno != SEENTRYNFND)
        RETURNQ (serrno);
    }

    /* Delete the comment if it exists */
    if (Cns_get_umd_by_fileid (&thip->dbfd, fmd_entry.fileid, &umd_entry, 1,
                               &rec_addru) == 0) {
      if (Cns_delete_umd_entry (&thip->dbfd, &rec_addru))
        RETURNQ (serrno);
    } else if (serrno != ENOENT)
      RETURNQ (serrno);

    /* Delete file entry */
    if (Cns_delete_file_metadata (thip, &fmd_entry, &rec_addr))
      RETURNQ (serrno);

    /* Update parent directory entry */
    dir_entry.nlink--;
    dir_entry.mtime = time (0);
    dir_entry.ctime = dir_entry.mtime;
    if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &dir_entry))
      RETURNQ (serrno);

    /* End transaction */
    (void) Cns_end_tr (&thip->dbfd);
    count++;

    /* Once we reach the deletion of 1000 files return control back to the
     * client, this allows us to make sure the client is still there and hasn't
     * hit CTRL-C with the intention of terminating the operation.
     */
    if (count >= 1000) {
      if ((c = Cns_get_smd_by_vid(&thip->dbfd, bov, vid, 0,
                                  &smd_entry, 0)) == 0) {
        RETURNQ(0);
      } else {
        break;
      }
    }
  }
  if (c < 0)
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_utime - set last access and modification times */

int Cns_srv_utime(char *req_data,
                  struct Cns_srv_thread_info *thip,
                  struct Cns_srv_request_info *reqinfo)
{
  time_t actime;
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char *func = "utime";
  time_t modtime;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  int user_specified_time;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_LONG (rbp, user_specified_time);
  if (user_specified_time) {
    unmarshall_TIME_T (rbp, actime);
    unmarshall_TIME_T (rbp, modtime);
  }

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  if (user_specified_time) {
    sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\" Atime=%lld Mtime=%lld",
             cwdpath, path, (long long int)actime, (long long int)modtime);
  } else {
    sprintf (reqinfo->logbuf, "Cwd=\"%s\" Path=\"%s\"", cwdpath, path);
  }

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check parent directory components for search permission and get/lock
   * basename entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, NULL, NULL, &filentry, &rec_addr,
                     CNS_MUST_EXIST))
    RETURN (serrno);

  /* Record the fileid being processed */
  reqinfo->fileid = filentry.fileid;

  /* Check if the user is authorized to set access/modification time for this
   * entry
   */
  if (user_specified_time) {
    if (reqinfo->uid != filentry.uid &&
        Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                    localhost, P_ADMIN))
      RETURN (EPERM);
  } else {
    if (reqinfo->uid != filentry.uid &&
        Cns_chkentryperm (&filentry, S_IWRITE, reqinfo->uid, reqinfo->gid,
                          reqinfo->clienthost))
      RETURN (EACCES);
  }

  /* Update entry */
  filentry.ctime = time (0);
  if (user_specified_time) {
    filentry.atime = actime;
    filentry.mtime = modtime;
  } else {
    filentry.atime = filentry.ctime;
    filentry.mtime = filentry.ctime;
  }

  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_updatefile_checksum - set checksums for the file */

int Cns_srv_updatefile_checksum(char *req_data,
                                struct Cns_srv_thread_info *thip,
                                struct Cns_srv_request_info *reqinfo)
{
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char *func = "updatefile_checksum";
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  char csumtype[3];
  char csumvalue[CA_MAXCKSUMLEN+1];
  int notAdmin = 1;
  unsigned int checksum = 0;
  char *dp   = NULL;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  if (unmarshall_STRINGN (rbp, csumtype, 3))
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, csumvalue, CA_MAXCKSUMLEN+1))
    RETURN (EINVAL);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf,
           "Cwd=\"%s\" Path=\"%s\" ChecksumType=\"%s\" ChecksumValue=\"%s\"",
           cwdpath, path, csumtype, csumvalue);

  if (*csumtype &&
      strcmp (csumtype, "PA") &&
      strcmp (csumtype, "AD"))
    RETURN (EINVAL);
  checksum = strtoul (csumvalue, &dp, 16);
  if (*dp != '\0') {
    RETURN (EINVAL);
  }

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check parent directory components for search permission and get/lock
   * basename entry
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, reqinfo->uid, reqinfo->gid,
                     reqinfo->clienthost, NULL, NULL, &filentry, &rec_addr,
                     CNS_MUST_EXIST))
    RETURN (serrno);

  /* Record the fileid being processed */
  reqinfo->fileid = filentry.fileid;

  /* Check if the entry is a regular file */
  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);

  /* Check if the user is authorized to set access/modification checksum for
   * this entry. The file must be 0 size or the user must be ADMIN. The
   * checksum must be a known one (i.e. adler) or empty (case of a reset, user
   * must be ADMIN then)
   */
  notAdmin = Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                         localhost, P_ADMIN);
  if (filentry.filesize > 0 && notAdmin) {
    RETURN (EPERM);
  }
  if (notAdmin && reqinfo->uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, reqinfo->uid, reqinfo->gid,
                        reqinfo->clienthost)) {
    RETURN (EACCES);
  }

  /* Update entry only if not admin */
  if (notAdmin) {
    filentry.mtime = time(0);
    filentry.ctime = filentry.mtime;
  }

  strcpy (filentry.csumtype, csumtype);
  if (*csumtype == '\0') {
    strcpy (filentry.csumvalue, ""); /* Reset value for empty types */
  } else {
    sprintf(filentry.csumvalue, "%x", checksum);
  }
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_openx - open and possibly create a new CASTOR file */

int Cns_srv_openx(char *req_data,
                  struct Cns_srv_thread_info *thip,
                  struct Cns_srv_request_info *reqinfo)
{
  /* Variables */
  struct Cns_file_metadata  fmd_entry;
  struct Cns_file_metadata  parent_dir;
  struct Cns_class_metadata class_entry;
  Cns_dbrec_addr rec_addr;  /* File record address */
  Cns_dbrec_addr rec_addrp; /* Parent record address */
  char       *func = "openx";
  char       *rbp;
  char       cwdpath[CA_MAXPATHLEN + 1];
  char       path[CA_MAXPATHLEN + 1];
  int        classid;
  int        flags;
  uid_t      owneruid;
  gid_t      ownergid;
  mode_t     mask;
  mode_t     mode;
  u_signed64 cwd;
  char       repbuf[REPBUFSZ];
  char       *sbp = repbuf;
  
  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);

  unmarshall_LONG (rbp, owneruid);
  unmarshall_LONG (rbp, ownergid);
  unmarshall_WORD (rbp, mask);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN + 1))
    RETURN (SENAMETOOLONG);
  unmarshall_LONG (rbp, flags);
  flags = ntohopnflg (flags);
  unmarshall_LONG (rbp, mode);
  /* mode must be within 0 and 07777 */
  if (mode > 4095) RETURN (EINVAL);
  unmarshall_LONG (rbp, classid);

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (reqinfo->logbuf,
           "OwnerUid=%d OwnerGid=%d Mask=%o Cwd=\"%s\" Path=\"%s\" Flags=%o "
           "Mode=%o ClassId=%d",
           owneruid, ownergid, mask, cwdpath, path, flags, mode, classid);

  /* The call to open files is a privileged one, so here we check that the
   * user issuing the call is authorized to do so
   */
  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                  localhost, P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Check parent directory components for (write)/search permission and get
   * basename entry if it exists
   */
  if (Cns_parsepath (&thip->dbfd, cwd, path, owneruid, ownergid,
                     reqinfo->clienthost,
                     &parent_dir, (flags & O_CREAT) ? &rec_addrp : NULL,
                     &fmd_entry,  (flags & O_TRUNC) ? &rec_addr  : NULL,
                     ((flags & O_CREAT) && (flags & O_EXCL)) ? CNS_NOFOLLOW : 0))
    RETURN (serrno);

  if (fmd_entry.fileid) {  /* File exists */
    reqinfo->fileid = fmd_entry.fileid;

    /* Marshall the new file flag */
    marshall_LONG (sbp, 0);

    /* Marshall the fileid */
    marshall_HYPER (sbp, fmd_entry.fileid);

    if ((flags & O_CREAT) && (flags & O_EXCL)) {
      sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
      RETURN (EEXIST);
    }
    if (*fmd_entry.name == '/') { /* Cns_creat / */
      sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
      RETURN (EISDIR);
    }
    if ((fmd_entry.filemode & S_IFDIR) == S_IFDIR) {  /* Is a directory */
      sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
      RETURN (EISDIR);
    }

    /* Check the file permissions */
    if (Cns_chkentryperm (&fmd_entry, (flags & O_WRONLY ||
                                       flags & O_RDWR   ||
                                       flags & O_TRUNC) ?
                          S_IWRITE : S_IREAD, owneruid, ownergid,
                          reqinfo->clienthost)) {
      sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
      RETURN (EACCES);
    }

    /* If the file was opened with O_TRUNC and has one of the writable access
     * modes delete the segments associated to the file and reset the file size
     */
    if ((flags & O_TRUNC) && ((flags & O_WRONLY) || (flags & O_RDWR))) {
      
      /* Drop segments if any, log their metadata */
      if (Cns_delete_segs(thip, &fmd_entry, 0)) {
        if (serrno != SEENTRYNFND) {
          sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
          RETURN (serrno);
        }
      }

      /* Reset the filesize and update the time attributes */
      fmd_entry.filesize = 0;
      fmd_entry.mtime = time(0);
      fmd_entry.ctime = fmd_entry.mtime;
      fmd_entry.status = '-';

      /* update the db and retrieve the new stagertime */
      if (Cns_update_fmd_entry_open (&thip->dbfd, &rec_addr, &fmd_entry)) {
        sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
        RETURN (serrno);
      }

      /* Amend logging parameters */
      sprintf (reqinfo->logbuf + strlen(reqinfo->logbuf),
               " NSOpenTime=%.6f Truncated=\"True\"", fmd_entry.stagertime);
    }
  } else {  /* New file */
    if ((flags & O_CREAT) == 0)
      RETURN (ENOENT);
    if (parent_dir.fileclass <= 0)
      RETURN (EINVAL);

    /* If a class id was supplied, verify the class exists otherwise use the
     * fileclass of the parent directory
     */
    fmd_entry.fileclass = parent_dir.fileclass;
    if (classid > 0) {
      if (Cns_get_class_by_id (&thip->dbfd, classid, &class_entry, 0, NULL)) {
        nslogit("MSG=\"Unable to find file class\" REQID=%s ClassId=%d "
                "RtnCode=%d", reqinfo->requuid, classid, serrno);
        RETURN (serrno);
      }
      fmd_entry.fileclass = classid;

      /* Amend logging parameters */
      sprintf (reqinfo->logbuf + strlen(reqinfo->logbuf), " ClassName=\"%s\"",
               class_entry.name);
    }

    /* Get a unique id for the file */
    if (Cns_unique_id (&thip->dbfd, &fmd_entry.fileid) < 0)
      RETURN (serrno);
    reqinfo->fileid = fmd_entry.fileid;

    /* Set the attributes of the new file */
    fmd_entry.filemode = S_IFREG | ((mode & ~S_IFMT) & ~mask);
    fmd_entry.nlink = 1;
    fmd_entry.uid = owneruid;
    if (parent_dir.filemode & S_ISGID) {
      fmd_entry.gid = parent_dir.gid;
      if (ownergid == fmd_entry.gid)
        fmd_entry.filemode |= S_ISGID;
    } else {
      fmd_entry.gid = ownergid;
    }
    fmd_entry.atime = time (0);
    fmd_entry.mtime = fmd_entry.atime;
    fmd_entry.ctime = fmd_entry.atime;
    fmd_entry.status = '-';

    /* Inherit the ACLs of the parent directory */
    if (*parent_dir.acl)
      Cns_acl_inherit (&parent_dir, &fmd_entry, mode);

    /* Write the new file entry and retrieve stagertime from db */
    if (Cns_insert_fmd_entry_open (&thip->dbfd, &fmd_entry))
      RETURN (serrno);

    /* Update the parent directory entry */
    parent_dir.nlink++;
    parent_dir.mtime = time (0);
    parent_dir.ctime = parent_dir.mtime;
    if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
      RETURN (serrno);

    /* Amend logging parameters */
    sprintf (reqinfo->logbuf + strlen(reqinfo->logbuf),
             " NSOpenTime=%.6f NewFile=\"True\"", fmd_entry.stagertime);

    /* Marshall the newfile flag */
    marshall_LONG (sbp, 1);

    /* Marshall the fileid */
    marshall_HYPER (sbp, fmd_entry.fileid);
  }

  /* Marshall the rest of the file attributes excluding the fileid which has
   * already been marshalled
   */
  marshall_WORD (sbp, fmd_entry.filemode);
  marshall_LONG (sbp, fmd_entry.nlink);
  marshall_LONG (sbp, fmd_entry.uid);
  marshall_LONG (sbp, fmd_entry.gid);
  marshall_HYPER (sbp, fmd_entry.filesize);
  marshall_TIME_T (sbp, fmd_entry.atime);
  marshall_TIME_T (sbp, fmd_entry.mtime);
  marshall_TIME_T (sbp, fmd_entry.ctime);
  marshall_WORD (sbp, fmd_entry.fileclass);
  marshall_BYTE (sbp, fmd_entry.status);
  marshall_STRING (sbp, fmd_entry.csumtype);
  marshall_STRING (sbp, fmd_entry.csumvalue);
  marshall_HYPER (sbp, fmd_entry.stagertime*1E6);  /* time in usecs as integer */
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/* Cns_srv_closex - close a file, setting its size and checksum attributes where appropriate */

int Cns_srv_closex(char *req_data,
                   struct Cns_srv_thread_info *thip,
                   struct Cns_srv_request_info *reqinfo)
{
  /* Variables */
  struct Cns_file_metadata fmd_entry;
  Cns_dbrec_addr rec_addr;  /* File record address */
  char         *func = "closex";
  char         *rbp;
  char         csumtype[3];
  char         csumvalue[CA_MAXCKSUMLEN + 1];
  double       last_stagertime = 0;
  time_t       new_mtime = 0;
  u_signed64   fileid;
  u_signed64   filesize;
  char         *dp = NULL;
  char         repbuf[93];
  char         *sbp;

  /* Unmarshall message body */
  rbp = req_data;
  unmarshall_LONG (rbp, reqinfo->uid);
  unmarshall_LONG (rbp, reqinfo->gid);
  get_client_actual_id (thip);

  unmarshall_HYPER (rbp, fileid);
  if (fileid > 0)
    reqinfo->fileid = fileid;
  unmarshall_HYPER (rbp, filesize);
  if (unmarshall_STRINGN (rbp, csumtype, 3))
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, csumvalue, CA_MAXCKSUMLEN + 1))
    RETURN (EINVAL);
  unmarshall_TIME_T (rbp, new_mtime);
  unmarshall_HYPER (rbp, last_stagertime);
  last_stagertime *= 1E-6;   /* we get usecs, convert back to secs */

  /* Check if namespace is in 'readonly' mode */
  if (rdonly)
    RETURN (EROFS);

  /* Construct log message */
  sprintf (reqinfo->logbuf,
           "FileSize=%llu ChecksumType=\"%s\" ChecksumValue=\"%s\" "
           "NewModTime=%lld StagerLastOpenTime=%.6f",
           filesize, csumtype, csumvalue,
           (long long int)new_mtime, last_stagertime);

  /* Check that the checksum type and value are valid. For now only adler32 (AD)
   * and pre-adler32 (PA) are supported.
   */
  if (csumtype[0] != '\0' && strcmp(csumtype, "AD") && strcmp(csumtype, "PA"))
    RETURN (EINVAL);
  if (csumvalue[0] != '\0') {
    strtoul (csumvalue, &dp, 16);
    if (*dp != '\0') {
      RETURN (EINVAL);
    }
  }

  /* The call to close files is a privileged one, so here we check that the
   * user issuing the call is authorized to do so
   */
  if (Cupv_check (reqinfo->uid, reqinfo->gid, reqinfo->clienthost,
                  localhost, P_ADMIN))
    RETURN (serrno);

  /* Start transaction */
  (void) Cns_start_tr (&thip->dbfd);

  /* Get/lock basename entry */
  if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &fmd_entry, 1, &rec_addr))
    RETURN (serrno);

  /* We deliberately don't check the parent directory components for search
   * permissions as the previous call to CUPV has already verified that we are
   * an ADMIN so the check is superfluous.
   */

  if (fmd_entry.filemode & S_IFDIR) /* Operation not permitted on directories */
    RETURN (EISDIR);
  
  /* Check for concurrent modifications */
  if (Cns_is_concurrent_open(&thip->dbfd, &fmd_entry, last_stagertime, reqinfo->logbuf))
    RETURN (serrno);

  /* Check for end-to-end checksum mismatch */
  if ((strcmp (fmd_entry.csumtype, "PA") == 0) &&
      (strcmp (csumtype, "AD") == 0)) {
    if (strcmp (fmd_entry.csumvalue, csumvalue) != 0) {
      nslogit("MSG=\"Predefined file checksum mismatch\" REQID=%s "
              "NSHOSTNAME=%s NSFILEID=%llu NewChecksum=\"0x%s\" "
              "ExpectedChecksum=\"0x%s\"",
              reqinfo->requuid, nshostname, fmd_entry.fileid, csumvalue,
              fmd_entry.csumvalue);
      RETURN (SECHECKSUM);
    }
  }

  /* Update the file metadata entry */
  fmd_entry.filesize = filesize;
  fmd_entry.ctime = fmd_entry.mtime = new_mtime;
  strcpy (fmd_entry.csumtype, csumtype);
  strcpy (fmd_entry.csumvalue, csumvalue);

  /* Delete the segments associated to the file */
  if (Cns_delete_segs (thip, &fmd_entry, 0) != 0)
    if (serrno != SEENTRYNFND)
      RETURN (serrno);
  if (fmd_entry.status == 'm')
    fmd_entry.status = '-';

  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &fmd_entry))
    RETURN (serrno);

  /* Marshall return value, the attributes of the file */
  sbp = repbuf;
  marshall_HYPER (sbp, fmd_entry.fileid);
  marshall_WORD (sbp, fmd_entry.filemode);
  marshall_LONG (sbp, fmd_entry.nlink);
  marshall_LONG (sbp, fmd_entry.uid);
  marshall_LONG (sbp, fmd_entry.gid);
  marshall_HYPER (sbp, fmd_entry.filesize);
  marshall_TIME_T (sbp, fmd_entry.atime);
  marshall_TIME_T (sbp, fmd_entry.mtime);
  marshall_TIME_T (sbp, fmd_entry.ctime);
  marshall_WORD (sbp, fmd_entry.fileclass);
  marshall_BYTE (sbp, fmd_entry.status);
  marshall_STRING (sbp, fmd_entry.csumtype);
  marshall_STRING (sbp, fmd_entry.csumvalue);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}
