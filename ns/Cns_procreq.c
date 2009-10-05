/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#if defined(_WIN32)
#define R_OK 4
#define W_OK 2
#define X_OK 1
#define F_OK 0
#define S_ISGID 0002000
#define S_ISVTX 0001000
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include <uuid/uuid.h>
#include "marshall.h"
#include "Cgrp.h"
#include "Cns.h"
#include "Cns_server.h"
#include "Cpwd.h"
#include "Cupv_api.h"
#include "patchlevel.h"
#include "rfcntl.h"
#include "serrno.h"
#include "u64subr.h"

/* forware declarations */
int sendrep(int rpfd, int rep_type, ...);
int nslogit(char *func, char *msg, ...);
int Cns_update_unique_gid(struct Cns_dbfd *dbfd, unsigned int unique_id);
int Cns_update_unique_uid(struct Cns_dbfd *dbfd, unsigned int unique_id);

extern int being_shutdown;
extern char localhost[CA_MAXHOSTNAMELEN+1];
extern int rdonly;

void get_cwd_path (thip, cwd, cwdpath)
     struct Cns_srv_thread_info *thip;
     u_signed64 cwd;
     char *cwdpath;
{
  char path[CA_MAXPATHLEN+1];
  char *p = path;
  cwdpath[0] = path[0] = '\0';
  if (cwd == 2) {
    p = "/";
  } else if (cwd > 0) {
    Cns_getpath_by_fileid (&thip->dbfd, cwd, &p);
  }
  if (path[0] != '\0')
    sprintf (cwdpath, "(cwd: %s)", p);
}

int get_client_actual_id (thip, uid, gid, user)
     struct Cns_srv_thread_info *thip;
     uid_t *uid;
     gid_t *gid;
     char **user;
{
  struct passwd *pw;

  if (thip->secure) {
    *uid = thip->Csec_uid;
    *gid = thip->Csec_gid;
    *user = thip->Csec_auth_id;
  } else {
    if ((pw = Cgetpwuid (*uid)) == NULL)
      *user = "UNKNOWN";
    else
      *user = pw->pw_name;
  }
  return (0);
}

/* Cns_logreq - log a request */

/* Split the message into lines so they don't exceed LOGBUFSZ-1 characters
 * A backslash is appended to a line to be continued
 * A continuation line is prefixed by '+ '
 */
void Cns_logreq(func, logbuf)
     char *func;
     char *logbuf;
{
  int n1, n2;
  char *p;
  char savechrs1[2];
  char savechrs2[2];

  n1 = LOGBUFSZ - strlen (func) - 36;
  n2 = strlen (logbuf);
  p = logbuf;
  while (n2 > n1) {
    savechrs1[0] = *(p + n1);
    savechrs1[1] = *(p + n1 + 1);
    *(p + n1) = '\\';
    *(p + n1 + 1) = '\0';
    nslogit (func, NS098, p);
    if (p != logbuf) {
      *p = savechrs2[0];
      *(p + 1) = savechrs2[1];
    }
    p += n1 - 2;
    savechrs2[0] = *p;
    savechrs2[1] = *(p + 1);
    *p = '+';
    *(p + 1) = ' ';
    *(p + 2) = savechrs1[0];
    *(p + 3) = savechrs1[1];
    n2 -= n1;
  }
  nslogit (func, NS098, p);
  if (p != logbuf) {
    *p = savechrs2[0];
    *(p + 1) = savechrs2[1];
  }
}

int marshall_DIRX (sbpp, magic, fmd_entry)
     char **sbpp;
     int magic;
     struct Cns_file_metadata *fmd_entry;
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

int marshall_DIRXR (sbpp, magic, fmd_entry)
     char **sbpp;
     int magic;
     struct Cns_file_metadata *fmd_entry;
{
  char *sbp = *sbpp;

  marshall_HYPER (sbp, fmd_entry->fileid);
  marshall_STRING (sbp, fmd_entry->guid);
  marshall_WORD (sbp, fmd_entry->filemode);
  marshall_HYPER (sbp, fmd_entry->filesize);
  marshall_STRING (sbp, fmd_entry->name);
  *sbpp = sbp;
  return (0);
}

int marshall_DIRXT (sbpp, magic, fmd_entry, smd_entry)
     char **sbpp;
     int magic;
     struct Cns_file_metadata *fmd_entry;
     struct Cns_seg_metadata *smd_entry;
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

int Cns_srv_aborttrans(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char func[19];
  gid_t gid;
  char *rbp;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_aborttrans");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "aborttrans", user, uid, gid, clienthost);

  (void) Cns_abort_tr (&thip->dbfd);
  RETURN (0);
}

/* Cns_srv_access - check accessibility of a file/directory */

int Cns_srv_access(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  int amode;
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  char func[16];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  mode_t mode;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_access");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "access", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);
  unmarshall_LONG (rbp, amode);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "access %o %s %s", amode, path, cwdpath);
  Cns_logreq (func, logbuf);

  if (amode & ~(R_OK | W_OK | X_OK | F_OK))
    RETURNQ (EINVAL);

  /* check parent directory components for search permission and
     get basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost, NULL,
                     NULL, &fmd_entry, NULL, CNS_MUST_EXIST))
    RETURNQ (serrno);

  /* check permissions for basename */

  if (amode == F_OK)
    RETURNQ (0);
  mode = (amode & (R_OK|W_OK|X_OK)) << 6;
  if (Cns_chkentryperm (&fmd_entry, mode, uid, gid, clienthost))
    RETURNQ (EACCES);
  RETURNQ (0);
}

/*      Cns_srv_chclass - change class on directory */

int Cns_srv_chclass(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char class_name[CA_MAXCLASNAMELEN+1];
  int classid;
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  char func[16];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  struct Cns_class_metadata new_class_entry;
  Cns_dbrec_addr new_rec_addrc;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_chclass");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "chclass", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_LONG (rbp, classid);
  if (unmarshall_STRINGN (rbp, class_name, CA_MAXCLASNAMELEN+1))
    RETURN (EINVAL);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "chclass %s %d %s %s", path, classid, class_name, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check parent directory components for search permission and
     get/lock basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost, NULL,
                     NULL, &fmd_entry, &rec_addr, CNS_MUST_EXIST))
    RETURN (serrno);

  /* is the class valid? */

  if (classid > 0) {
    if (Cns_get_class_by_id (&thip->dbfd, classid, &new_class_entry,
                             0, &new_rec_addrc)) {
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
    if (Cns_get_class_by_name (&thip->dbfd, class_name, &new_class_entry,
                               1, &new_rec_addrc)) {
      if (serrno == ENOENT) {
        sendrep (thip->s, MSG_ERR, "No such class\n");
        RETURN (EINVAL);
      } else {
        RETURN (serrno);
      }
    }
  }

  /* check if the user is authorized to chclass this entry */

  if (uid != fmd_entry.uid &&
      Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
    RETURN (EPERM);
  if (((fmd_entry.filemode & S_IFDIR) == 0) &&
      (0 != Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))) {
    RETURN (ENOTDIR);
  }

  /* update entries */

  if (fmd_entry.fileclass != new_class_entry.classid) {
    fmd_entry.fileclass = new_class_entry.classid;
    fmd_entry.ctime = time (0);
    if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &fmd_entry))
      RETURN (serrno);
  }
  RETURN (0);
}

/*      Cns_srv_chdir - change current working directory */

int Cns_srv_chdir(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  struct Cns_file_metadata direntry;
  char func[16];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  char repbuf[8];
  char *sbp;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_chdir");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "chdir", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "chdir %s %s", path, cwdpath);
  Cns_logreq (func, logbuf);

  /* check parent directory components for search permission and
     get basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost, NULL,
                     NULL, &direntry, NULL, CNS_MUST_EXIST))
    RETURN (serrno);

  if ((direntry.filemode & S_IFDIR) == 0)
    RETURN (ENOTDIR);
  if (Cns_chkentryperm (&direntry, S_IEXEC, uid, gid, clienthost))
    RETURN (EACCES);

  /* return directory fileid */

  sbp = repbuf;
  marshall_HYPER (sbp, direntry.fileid);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/*      Cns_srv_chmod - change file/directory permissions */

int Cns_srv_chmod(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  char func[16];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  mode_t mode;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_chmod");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "chmod", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_LONG (rbp, mode);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "chmod %o %s %s", mode, path, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check parent directory components for search permission and
     get/lock basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost, NULL,
                     NULL, &fmd_entry, &rec_addr, CNS_MUST_EXIST))
    RETURN (serrno);

  /* check if the user is authorized to chmod this entry */

  if (uid != fmd_entry.uid &&
      Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
    RETURN (EPERM);
  if ((fmd_entry.filemode & S_IFDIR) == 0 && uid != 0)
    mode &= ~S_ISVTX;
  if (gid != fmd_entry.gid && uid != 0)
    mode &= ~S_ISGID;

  /* update entry */

  fmd_entry.filemode = (fmd_entry.filemode & S_IFMT) | (mode & ~S_IFMT);
  if (*fmd_entry.acl)
    Cns_acl_chmod (&fmd_entry);
  fmd_entry.ctime = time (0);
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &fmd_entry))
    RETURN (serrno);
  RETURN (0);
}

/*      Cns_srv_chown - change owner and group of a file or a directory */

int Cns_srv_chown(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  int found;
  char func[16];
  gid_t gid;
  struct group *gr;
  char logbuf[LOGBUFSZ];
  char **membername;
  int need_p_admin = 0;
  int need_p_expt_admin = 0;
  gid_t new_gid;
  uid_t new_uid;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  struct passwd *pw;
  char *rbp;
  Cns_dbrec_addr rec_addr;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_chown");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "chown", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_LONG (rbp, new_uid);
  unmarshall_LONG (rbp, new_gid);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "chown %d:%d %s %s", new_uid, new_gid, path, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check parent directory components for search permission and
     get/lock basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost, NULL,
                     NULL, &fmd_entry, &rec_addr, CNS_MUST_EXIST))
    RETURN (serrno);

  /* check if the user is authorized to change ownership this entry */
  if (fmd_entry.uid != new_uid && new_uid != -1) {
    if (gid != fmd_entry.gid)
      need_p_admin = 1;
    else if ((pw = Cgetpwuid (new_uid)) == NULL)
      need_p_admin = 1;
    else if (pw->pw_gid == gid) /* new owner belongs to same group */
      need_p_expt_admin = 1;
    else
      need_p_admin = 1;
  }
  if (fmd_entry.gid != new_gid && new_gid != -1) {
    if (uid != fmd_entry.uid) {
      need_p_admin = 1;
    } else if ((pw = Cgetpwuid (uid)) == NULL) {
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
    if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
      RETURN (EPERM);
  } else if (need_p_expt_admin) {
    if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN) &&
        Cupv_check (uid, gid, clienthost, localhost, P_GRP_ADMIN))
      RETURN (EPERM);
  }

  /* update entry */

  if (new_uid != -1)
    fmd_entry.uid = new_uid;
  if (new_gid != -1)
    fmd_entry.gid = new_gid;
  if (*fmd_entry.acl)
    Cns_acl_chown (&fmd_entry);
  fmd_entry.ctime = time (0);
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &fmd_entry))
    RETURN (serrno);
  RETURN (0);
}

/*      Cns_internal_dropsegs - internal method dropping all segments for a given file */

int Cns_internal_dropsegs(func, thip, filentry)
     char* func;
     struct Cns_srv_thread_info *thip;
     struct Cns_file_metadata *filentry;
{
  int bof = 1;
  int c;
  DBLISTPTR dblistptr;
  struct Cns_seg_metadata smd_entry;
  Cns_dbrec_addr rec_addrs; /* segment record address */
  char logbuf[LOGBUFSZ];
  char tmpbuf[21];
  char tmpbuf2[21];

  while ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry->fileid,
                                   &smd_entry, 1, &rec_addrs, 0, &dblistptr)) == 0) {
    sprintf (logbuf, "unlinking segment: %s %d %d %s %d %c %s %d %d %02x%02x%02x%02x \"%s\" %lx",
             u64tostr (smd_entry.s_fileid, tmpbuf, 0), smd_entry.copyno,
             smd_entry.fsec, u64tostr (smd_entry.segsize, tmpbuf2, 0),
             smd_entry.compression, smd_entry.s_status, smd_entry.vid,
             smd_entry.side, smd_entry.fseq, smd_entry.blockid[0],
             smd_entry.blockid[1], smd_entry.blockid[2], smd_entry.blockid[3],
             smd_entry.checksum_name, smd_entry.checksum);
    Cns_logreq (func, logbuf);
    if (Cns_delete_smd_entry (&thip->dbfd, &rec_addrs))
      return (serrno);
    bof = 0;
  }
  (void) Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry->fileid,
                              &smd_entry, 1, &rec_addrs, 1, &dblistptr); /* free res */
  if (c < 0)
    return (serrno);
  return (0);
}

/*      Cns_srv_creat - create a file entry */

int Cns_srv_creat(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char func[16];
  gid_t gid;
  char guid[CA_MAXGUIDLEN+1];
  char logbuf[LOGBUFSZ];
  mode_t mask;
  mode_t mode;
  struct Cns_file_metadata parent_dir;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addr;  /* file record address */
  Cns_dbrec_addr rec_addrp; /* parent record address */
  char repbuf[8];
  char *sbp;
  char tmpbuf[21];
  uid_t uid;
  char *user;
  uuid_t uuid;

  strcpy (func, "Cns_srv_creat");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "creat", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_WORD (rbp, mask);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_LONG (rbp, mode);
  if (magic >= CNS_MAGIC2) {
    if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN+1))
      RETURN (EINVAL);
    if (uuid_parse (guid, uuid) < 0)
      RETURN (EINVAL);
  } else
    *guid = '\0';
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "creat %s %s %o %o %s", path, guid, mode, mask, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check parent directory components for write/search permission and
     get/lock basename entry if it exists */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                     &parent_dir, &rec_addrp, &filentry, &rec_addr, 0))
    RETURN (serrno);

  if (*filentry.name == '/') /* Cns_creat / */
    RETURN (EISDIR);

  if (filentry.fileid) { /* file exists */
    if (filentry.filemode & S_IFDIR)
      RETURN (EISDIR);
    if (strcmp (filentry.guid, guid))
      RETURN (EEXIST);

    /* check write permission in basename entry */

    if (Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost))
      RETURN (EACCES);

    /* delete file segments if any */

    if (Cns_internal_dropsegs(func, thip, &filentry) != 0)
      RETURN (serrno);

    /* update basename entry */

    if (*guid)
      strcpy (filentry.guid, guid);
    filentry.filesize = 0;
    filentry.mtime = time (0);
    filentry.ctime = filentry.mtime;
    filentry.status = '-';
    if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
      RETURN (serrno);
    nslogit (func, "file %s reset\n", u64tostr (filentry.fileid, tmpbuf, 0));
  } else { /* must create the file */
    if (parent_dir.fileclass <= 0)
      RETURN (EINVAL);
    if (Cns_unique_id (&thip->dbfd, &filentry.fileid) < 0)
      RETURN (serrno);
    /* parent_fileid and name have been set by Cns_parsepath */
    strcpy (filentry.guid, guid);
    filentry.filemode = S_IFREG | ((mode & ~S_IFMT) & ~mask);
    filentry.nlink = 1;
    filentry.uid = uid;
    if (parent_dir.filemode & S_ISGID) {
      filentry.gid = parent_dir.gid;
      if (gid == filentry.gid)
        filentry.filemode |= S_ISGID;
    } else
      filentry.gid = gid;
    filentry.atime = time (0);
    filentry.mtime = filentry.atime;
    filentry.ctime = filentry.atime;
    filentry.fileclass = parent_dir.fileclass;
    filentry.status = '-';
    if (*parent_dir.acl)
      Cns_acl_inherit (&parent_dir, &filentry, mode);

    /* write new file entry */

    if (Cns_insert_fmd_entry (&thip->dbfd, &filentry))
      RETURN (serrno);

    /* update parent directory entry */

    parent_dir.nlink++;
    parent_dir.mtime = time (0);
    parent_dir.ctime = parent_dir.mtime;
    if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
      RETURN (serrno);
    nslogit (func, "file %s created\n", u64tostr (filentry.fileid, tmpbuf, 0));
  }
  sbp = repbuf;
  marshall_HYPER (sbp, filentry.fileid);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/* Cns_srv_delcomment - delete a comment associated with a file/directory */

int Cns_srv_delcomment(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char func[19];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addru;
  uid_t uid;
  struct Cns_user_metadata umd_entry;
  char *user;

  strcpy (func, "Cns_srv_delcomment");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "delcomment", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "delcomment %s %s", path, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check parent directory components for search permission and
     get basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost, NULL,
                     NULL, &filentry, NULL, CNS_MUST_EXIST))
    RETURN (serrno);

  /* check if the user is authorized to delete the comment on this entry */

  if (uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost))
    RETURN (EACCES);

  /* delete the comment if it exists */

  if (Cns_get_umd_by_fileid (&thip->dbfd, filentry.fileid, &umd_entry, 1,
                             &rec_addru))
    RETURN (serrno);
  if (Cns_delete_umd_entry (&thip->dbfd, &rec_addru))
    RETURN (serrno);
  RETURN (0);
}

/*      Cns_srv_delete - logically remove a file entry */

int Cns_srv_delete(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  int bof = 1;
  int c;
  u_signed64 cwd;
  DBLISTPTR dblistptr;
  struct Cns_file_metadata filentry;
  char func[16];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  struct Cns_file_metadata parent_dir;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addr; /* file record address */
  Cns_dbrec_addr rec_addrp; /* parent record address */
  Cns_dbrec_addr rec_addrs; /* segment record address */
  struct Cns_seg_metadata smd_entry;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_delete");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "delete", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "delete %s %s", path, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check parent directory components for write/search permission and
     get/lock basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                     &parent_dir, &rec_addrp, &filentry, &rec_addr, CNS_MUST_EXIST|CNS_NOFOLLOW))
    RETURN (serrno);

  if (*filentry.name == '/') /* Cns_delete / */
    RETURN (EINVAL);

  if (filentry.filemode & S_IFDIR)
    RETURN (EPERM);

  /* if the parent has the sticky bit set,
     the user must own the file or the parent or
     the basename entry must have write permission */

  if (parent_dir.filemode & S_ISVTX &&
      uid != parent_dir.uid && uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost))
    RETURN (EACCES);

  /* mark file segments if any as logically deleted */

  bof = 1;
  while ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid,
                                   &smd_entry, 1, &rec_addrs, 0, &dblistptr)) == 0) {
    smd_entry.s_status = 'D';
    if (Cns_update_smd_entry (&thip->dbfd, &rec_addrs, &smd_entry))
      RETURN (serrno);
    bof = 0;
  }
  (void) Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid,
                              &smd_entry, 1, &rec_addrs, 1, &dblistptr); /* free res */
  if (c < 0)
    RETURN (serrno);

  /* mark file entry as logically deleted */

  filentry.status = 'D';
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
    RETURN (serrno);

  /* update parent directory entry */

  parent_dir.mtime = time (0);
  parent_dir.ctime = parent_dir.mtime;
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_deleteclass - delete a file class definition */

int Cns_srv_deleteclass(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  int bol = 1;
  struct Cns_class_metadata class_entry;
  char class_name[CA_MAXCLASNAMELEN+1];
  int classid;
  DBLISTPTR dblistptr;
  char func[20];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  Cns_dbrec_addr rec_addrt;
  struct Cns_tp_pool tppool_entry;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_deleteclass");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "deleteclass", user, uid, gid, clienthost);
  unmarshall_LONG (rbp, classid);
  if (unmarshall_STRINGN (rbp, class_name, CA_MAXCLASNAMELEN+1))
    RETURN (EINVAL);
  sprintf (logbuf, "deleteclass %d %s", classid, class_name);
  Cns_logreq (func, logbuf);

  if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
    RETURN (serrno);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

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
                                &tppool_entry, 1, &rec_addrt, 0, &dblistptr) == 0) {
    if (Cns_delete_tppool_entry (&thip->dbfd, &rec_addrt))
      RETURN (serrno);
    bol = 0;
  }
  (void) Cns_get_tppool_by_cid (&thip->dbfd, bol, class_entry.classid,
                                &tppool_entry, 1, &rec_addrt, 1, &dblistptr); /* free res */
  if (Cns_delete_class_entry (&thip->dbfd, &rec_addr))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_du - summarize file space usage */

int compute_du4dir (thip, direntry, Lflag, uid, gid, clienthost, nbbytes, nbentries)
     struct Cns_srv_thread_info *thip;
     struct Cns_file_metadata *direntry;
     int Lflag;
     uid_t uid;
     gid_t gid;
     const char *clienthost;
     u_signed64 *nbbytes;
     u_signed64 *nbentries;
{
  int bod = 1;
  int c;
  DBLISTPTR dblistptr;
  struct dirlist {
    u_signed64 fileid;
    struct dirlist *next;
  };
  struct dirlist *dlc;  /* pointer to current directory in the list */
  struct dirlist *dlf = NULL; /* pointer to first directory in the list */
  struct dirlist *dll;  /* pointer to last directory in the list */
  struct Cns_file_metadata fmd_entry;

  if (Cns_chkentryperm (direntry, S_IREAD|S_IEXEC, uid, gid, clienthost))
    return (EACCES);
  while ((c = Cns_get_fmd_by_pfid (&thip->dbfd, bod, direntry->fileid,
                                   &fmd_entry, 1, 0, &dblistptr)) == 0) { /* loop on directory entries */
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
    } else { /* regular file */
      *nbbytes += fmd_entry.filesize;
      *nbentries += 1;
    }
    bod = 0;
  }
  (void) Cns_get_fmd_by_pfid (&thip->dbfd, bod, direntry->fileid,
                              &fmd_entry, 1, 1, &dblistptr);
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

int Cns_srv_du(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  int c;
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  char func[16];
  gid_t gid;
  int Lflag;
  char logbuf[LOGBUFSZ];
  u_signed64 nbbytes = 0;
  u_signed64 nbentries = 0;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  char repbuf[16];
  char *sbp;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_du");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "du", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);
  unmarshall_WORD (rbp, Lflag);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "du %s %s", path, cwdpath);
  Cns_logreq (func, logbuf);

  if (! cwd && *path == 0)
    RETURNQ (ENOENT);
  if (! cwd && *path != '/')
    RETURNQ (EINVAL);

  /* check parent directory components for search permission and
     get basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                     NULL, NULL, &fmd_entry, NULL, CNS_MUST_EXIST))
    RETURNQ (serrno);

  if (fmd_entry.filemode & S_IFDIR) {
    if ((c = compute_du4dir (thip, &fmd_entry, Lflag, uid, gid,
                             clienthost, &nbbytes, &nbentries)))
      RETURNQ (c);
  } else { /* regular file */
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

int Cns_srv_endsess(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char func[16];
  gid_t gid;
  char *rbp;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_endsess");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "endsess", user, uid, gid, clienthost);
  RETURN (0);
}

/* Cns_srv_endtrans - end transaction mode */

int Cns_srv_endtrans(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char func[17];
  gid_t gid;
  char *rbp;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_endtrans");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "endtrans", user, uid, gid, clienthost);

  (void) Cns_end_tr (&thip->dbfd);
  RETURN (0);
}

/* Cns_srv_enterclass - define a new file class */

int Cns_srv_enterclass(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  struct Cns_class_metadata class_entry;
  char func[19];
  gid_t gid;
  int i;
  char logbuf[LOGBUFSZ];
  int nbtppools;
  char *rbp;
  struct Cns_tp_pool tppool_entry;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_enterclass");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "enterclass", user, uid, gid, clienthost);
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
  sprintf (logbuf, "enterclass %d %s", class_entry.classid,
           class_entry.name);
  Cns_logreq (func, logbuf);

  if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
    RETURN (serrno);

  /* start transaction */

  if (class_entry.classid <= 0 || *class_entry.name == '\0')
    RETURN (EINVAL);
  if (class_entry.max_filesize < class_entry.min_filesize)
    RETURN (EINVAL);
  (void) Cns_start_tr (thip->s, &thip->dbfd);

  if (Cns_insert_class_entry (&thip->dbfd, &class_entry))
    RETURN (serrno);

  /* receive/store tppool entries */

  tppool_entry.classid = class_entry.classid;
  for (i = 0; i < nbtppools; i++) {
    if (unmarshall_STRINGN (rbp, tppool_entry.tape_pool, CA_MAXPOOLNAMELEN+1))
      RETURN (EINVAL);
    if (Cns_insert_tppool_entry (&thip->dbfd, &tppool_entry))
      RETURN (serrno);
  }
  RETURN (0);
}

/*      Cns_srv_getacl - get the Access Control List for a file/directory */

int Cns_srv_getacl(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  char func[16];
  gid_t gid;
  char *iacl;
  char logbuf[LOGBUFSZ];
  int nentries = 0;
  char *p;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  char repbuf[REPBUFSZ];
  char *sbp;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_getacl");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "getacl", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "getacl %s %s", path, cwdpath);
  Cns_logreq (func, logbuf);

  /* check parent directory components for search permission and
     get basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost, NULL,
                     NULL, &fmd_entry, NULL, CNS_MUST_EXIST))
    RETURNQ (serrno);

  sbp = repbuf;
  marshall_WORD (sbp, nentries);  /* will be updated */
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
  marshall_WORD (p, nentries);  /* update nentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_getcomment - get the comment associated with a file/directory */

int Cns_srv_getcomment(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char func[19];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  char repbuf[CA_MAXCOMMENTLEN+1];
  char *sbp;
  uid_t uid;
  struct Cns_user_metadata umd_entry;
  char *user;

  strcpy (func, "Cns_srv_getcomment");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "getcomment", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "getcomment %s %s", path, cwdpath);
  Cns_logreq (func, logbuf);

  /* check parent directory components for search permission and
     get basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost, NULL,
                     NULL, &filentry, NULL, CNS_MUST_EXIST))
    RETURNQ (serrno);

  /* check if the user is authorized to get the comment for this entry */

  if (uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IREAD, uid, gid, clienthost))
    RETURNQ (EACCES);

  /* get the comment if it exists */

  if (Cns_get_umd_by_fileid (&thip->dbfd, filentry.fileid, &umd_entry, 0,
                             NULL))
    RETURNQ (serrno);

  sbp = repbuf;
  marshall_STRING (sbp, umd_entry.comments);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_getlinks - get the link entries associated with a given file */

int Cns_srv_getlinks(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  int bol = 1;
  int c;
  u_signed64 cwd;
  DBLISTPTR dblistptr;
  struct Cns_file_metadata fmd_entry;
  char func[17];
  gid_t gid;
  char guid[CA_MAXGUIDLEN+1];
  struct Cns_symlinks lnk_entry;
  char logbuf[LOGBUFSZ];
  int n;
  char *p;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  char repbuf[REPBUFSZ];
  char *sbp = repbuf;
  char tmp_path[CA_MAXPATHLEN+1];
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_getlinks");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "getlinks", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);
  if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN + 1) < 0)
    RETURNQ (EINVAL);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "getlinks %s %s %s", path, guid, cwdpath);
  Cns_logreq (func, logbuf);

  if (*path) {
    /* check parent directory components for search permission and
       get basename entry */

    if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                       NULL, NULL, &fmd_entry, NULL, CNS_MUST_EXIST))
      RETURNQ (serrno);
    if (*guid && strcmp (guid, fmd_entry.guid))
      RETURNQ (EINVAL);
  } else {
    if (! *guid)
      RETURNQ (ENOENT);

    /* get basename entry */

    if (Cns_get_fmd_by_guid (&thip->dbfd, guid, &fmd_entry, 0, NULL))
      RETURNQ (serrno);

    /* do not check parent directory components for search permission
     * as symlinks can anyway point directly at a file
     */
  }
  if ((fmd_entry.filemode & S_IFMT) == S_IFLNK) {
    if (Cns_get_lnk_by_fileid (&thip->dbfd, fmd_entry.fileid,
                               &lnk_entry, 0, NULL))
      RETURNQ (serrno);
  } else {
    if (*path != '/') { /* need to get path */
      p = tmp_path;
      if (Cns_getpath_by_fileid (&thip->dbfd, fmd_entry.fileid, &p))
        RETURNQ (serrno);
      strcpy (lnk_entry.linkname, p);
    } else
      strcpy (lnk_entry.linkname, path);
  }
  marshall_STRING (sbp, lnk_entry.linkname);
  while ((c = Cns_list_lnk_entry (&thip->dbfd, bol, lnk_entry.linkname,
                                  &lnk_entry, 0, &dblistptr)) == 0) {
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
  (void) Cns_list_lnk_entry (&thip->dbfd, bol, lnk_entry.linkname,
                             &lnk_entry, 1, &dblistptr);
  if (c < 0)
    RETURNQ (serrno);
  if (sbp > repbuf)
    sendrep (thip->s, MSG_LINKS, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_getpath - resolve a file id to a path */

int Cns_srv_getpath(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cur_fileid;
  char func[16];
  gid_t gid;
  char *p;
  char path[CA_MAXPATHLEN+1];
  char *rbp;
  char repbuf[CA_MAXPATHLEN+1];
  char *sbp;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_getpath");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "getpath", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cur_fileid);

  if (cur_fileid == 2)
    p = "/";
  else {
    p = path;
    if (Cns_getpath_by_fileid (&thip->dbfd, cur_fileid, &p))
      RETURNQ (serrno);
  }
  sbp = repbuf;
  marshall_STRING (sbp, p);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_getsegattrs - get file segments attributes */

int Cns_srv_getsegattrs(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  int bof = 1;
  int c;
  u_signed64 cwd;
  DBLISTPTR dblistptr;
  u_signed64 fileid;
  struct Cns_file_metadata filentry;
  char func[20];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  int nbseg = 0;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *q;
  char *rbp;
  char repbuf[REPBUFSZ];
  char *sbp;
  struct Cns_seg_metadata smd_entry;
  char tmpbuf[21];
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_getsegattrs");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "getsegattrs", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "getsegattrs %s %s %s",
           u64tostr (fileid, tmpbuf, 0), path, cwdpath);
  Cns_logreq (func, logbuf);

  if (fileid) {
    /* get basename entry */

    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid,
                               &filentry, 0, NULL))
      RETURN (serrno);

    /* check parent directory components for search permission */

    if (Cns_chkbackperm (&thip->dbfd, filentry.parent_fileid,
                         S_IEXEC, uid, gid, clienthost))
      RETURN (serrno);
  } else {
    /* check parent directory components for search permission and
       get basename entry */

    if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                       NULL, NULL, &filentry, NULL, CNS_MUST_EXIST))
      RETURN (serrno);
  }

  /* check if the entry is a regular file */

  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);

  /* get/send file segment entries */

  sbp = repbuf;
  marshall_WORD (sbp, nbseg); /* will be updated */
  while ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid,
                                   &smd_entry, 0, NULL, 0, &dblistptr)) == 0) {
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
  (void) Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid,
                              &smd_entry, 0, NULL, 1, &dblistptr); /* free res */
  if (c < 0)
    RETURN (serrno);

  q = repbuf;
  marshall_WORD (q, nbseg);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/*      Cns_srv_lchown - change owner and group of a file or a directory */

int Cns_srv_lchown(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  int found;
  char func[16];
  gid_t gid;
  struct group *gr;
  char logbuf[LOGBUFSZ];
  char **membername;
  int need_p_admin = 0;
  int need_p_expt_admin = 0;
  gid_t new_gid;
  uid_t new_uid;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  struct passwd *pw;
  char *rbp;
  Cns_dbrec_addr rec_addr;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_lchown");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "lchown", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_LONG (rbp, new_uid);
  unmarshall_LONG (rbp, new_gid);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "lchown %d:%d %s %s", new_uid, new_gid, path, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check parent directory components for search permission and
     get/lock basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost, NULL,
                     NULL, &fmd_entry, &rec_addr, CNS_NOFOLLOW|CNS_MUST_EXIST))
    RETURN (serrno);

  /* check if the user is authorized to change ownership this entry */

  if (fmd_entry.uid != new_uid && new_uid != -1) {
    if (gid != fmd_entry.gid)
      need_p_admin = 1;
    else if ((pw = Cgetpwuid (new_uid)) == NULL)
      need_p_admin = 1;
    else if (pw->pw_gid == gid) /* new owner belongs to same group */
      need_p_expt_admin = 1;
    else
      need_p_admin = 1;
  }
  if (fmd_entry.gid != new_gid && new_gid != -1) {
    if (uid != fmd_entry.uid) {
      need_p_admin = 1;
    } else if ((pw = Cgetpwuid (uid)) == NULL) {
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
    if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
      RETURN (EPERM);
  } else if (need_p_expt_admin) {
    if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN) &&
        Cupv_check (uid, gid, clienthost, localhost, P_GRP_ADMIN))
      RETURN (EPERM);
  }

  /* update entry */

  if (new_uid != -1)
    fmd_entry.uid = new_uid;
  if (new_gid != -1)
    fmd_entry.gid = new_gid;
  if (*fmd_entry.acl)
    Cns_acl_chmod (&fmd_entry);
  fmd_entry.ctime = time (0);
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &fmd_entry))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_listclass - list file classes */

int Cns_srv_listclass(magic, req_data, clienthost, thip, class_entry, endlist, dblistptr)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
     struct Cns_class_metadata *class_entry;
     int endlist;
     DBLISTPTR *dblistptr;
{
  int bol; /* beginning of class list flag */
  int bot; /* beginning of tape pools list flag */
  int c;
  int eol = 0; /* end of list flag */
  char func[18];
  gid_t gid;
  int listentsz; /* size of client machine Cns_fileclass structure */
  int maxsize;
  int nbentries = 0;
  int nbtppools;
  char outbuf[LISTBUFSZ+4];
  char *p;
  char *q;
  char *rbp;
  char *sav_sbp;
  char *sbp;
  DBLISTPTR tplistptr;
  struct Cns_tp_pool tppool_entry;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_listclass");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "listclass", user, uid, gid, clienthost);
  unmarshall_WORD (rbp, listentsz);
  unmarshall_WORD (rbp, bol);

  bol = 0;
  if (! class_entry->classid)
    bol = 1; /* do not rely on client */

  /* return as many entries as possible to the client */

  maxsize = LISTBUFSZ;
  sbp = outbuf;
  marshall_WORD (sbp, nbentries);  /* will be updated */

  if (bol || endlist)
    c = Cns_list_class_entry (&thip->dbfd, bol, class_entry,
                              endlist, dblistptr);
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

    /* get/send tppool entries */

    bot = 1;
    nbtppools = 0;
    q = sbp;
    marshall_LONG (sbp, nbtppools); /* will be updated */
    maxsize -= listentsz;
    while ((c = Cns_get_tppool_by_cid (&thip->dbfd, bot,
                                       class_entry->classid, &tppool_entry, 0, NULL, 0, &tplistptr)) == 0) {
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
                                  &tppool_entry, 0, NULL, 1, &tplistptr); /* free res */
    if (c < 0)
      RETURN (serrno);

    marshall_LONG (q, nbtppools);
    nbentries++;
    bol = 0;
    c = Cns_list_class_entry (&thip->dbfd, bol, class_entry,
                              endlist, dblistptr);
  }
  if (c < 0)
    RETURN (serrno);
  if (c == 1)
    eol = 1;
 reply:
  marshall_WORD (sbp, eol);
  p = outbuf;
  marshall_WORD (p, nbentries);  /* update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
  RETURN (0);
}

int Cns_srv_listlinks(magic, req_data, clienthost, thip, lnk_entry, endlist, dblistptr)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
     struct Cns_symlinks *lnk_entry;
     int endlist;
     DBLISTPTR *dblistptr;
{
  int bol; /* beginning of list flag */
  int c;
  u_signed64 cwd;
  int eol = 0; /* end of list flag */
  struct Cns_file_metadata fmd_entry;
  char func[18];
  gid_t gid;
  char guid[CA_MAXGUIDLEN+1];
  int listentsz; /* size of client machine Cns_linkinfo structure */
  char logbuf[LOGBUFSZ];
  int maxsize;
  int nbentries = 0;
  char outbuf[LISTBUFSZ+4];
  char *p;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  char *sbp;
  char tmp_path[CA_MAXPATHLEN+1];
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_listlinks");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "listlinks", user, uid, gid, clienthost);
  unmarshall_WORD (rbp, listentsz);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN + 1) < 0)
    RETURN (EINVAL);
  unmarshall_WORD (rbp, bol);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "listlinks %s %s %s", path, guid, cwdpath);
  Cns_logreq (func, logbuf);

  bol = 0;
  if (! lnk_entry->fileid)
    bol = 1; /* do not rely on client */
  
  if (bol) {
    if (*path) {
      /* check parent directory components for search permission and
         get basename entry */

      if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                         NULL, NULL, &fmd_entry, NULL, CNS_MUST_EXIST|CNS_NOFOLLOW))
        RETURN (serrno);
      if (*guid && strcmp (guid, fmd_entry.guid))
        RETURN (EINVAL);
    } else {
      if (! *guid)
        RETURN (ENOENT);

      /* get basename entry */

      if (Cns_get_fmd_by_guid (&thip->dbfd, guid, &fmd_entry, 0, NULL))
        RETURN (serrno);

      /* do not check parent directory components for search permission
       * as symlinks can anyway point directly at a file
       */
    }

    if ((fmd_entry.filemode & S_IFMT) == S_IFLNK) {
      if (Cns_get_lnk_by_fileid (&thip->dbfd, fmd_entry.fileid,
                                 lnk_entry, 0, NULL))
        RETURN (serrno);
    } else {
      if (*path != '/') { /* need to get path */
        p = tmp_path;
        if (Cns_getpath_by_fileid (&thip->dbfd, fmd_entry.fileid, &p))
          RETURN (serrno);
        strcpy (lnk_entry->linkname, p);
      } else
        strcpy (lnk_entry->linkname, path);
    }
  }

  /* return as many entries as possible to the client */

  maxsize = LISTBUFSZ;
  sbp = outbuf;
  marshall_WORD (sbp, nbentries);  /* will be updated */

  if (bol) {
    marshall_STRING (sbp, lnk_entry->linkname);
    maxsize -= listentsz;
    nbentries++;
  }
  if (bol || endlist)
    c = Cns_list_lnk_entry (&thip->dbfd, bol, lnk_entry->linkname,
                            lnk_entry, endlist, dblistptr);
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
    c = Cns_list_lnk_entry (&thip->dbfd, bol, lnk_entry->linkname,
                            lnk_entry, endlist, dblistptr);
  }
  if (c < 0)
    RETURN (serrno);
  if (c == 1)
    eol = 1;

  marshall_WORD (sbp, eol);
  p = outbuf;
  marshall_WORD (p, nbentries);  /* update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
  RETURN (0);
}

int Cns_srv_lastfseq(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  struct Cns_seg_metadata smd_entry;
  char  func[19];
  char  vid[CA_MAXVIDLEN+1];
  char  logbuf[LOGBUFSZ];
  char  repbuf[REPBUFSZ];
  char  *rbp;
  char  *sbp;
  char  *user;
  int   side;
  int   c;
  gid_t gid;
  uid_t uid;

  strcpy(func, "Cns_srv_lastfseq");

  /* Extract and log common request attributes */
  rbp = req_data;
  unmarshall_LONG(rbp, uid);
  unmarshall_LONG(rbp, gid);
  get_client_actual_id(thip, &uid, &gid, &user);
  nslogit(func, NS092, "lastfseq", user, uid, gid, clienthost);

  /* Extract volume id */
  if (unmarshall_STRINGN(rbp, vid, CA_MAXVIDLEN + 1)) {
    RETURN (EINVAL);
  }
  unmarshall_LONG(rbp, side);
  sprintf(logbuf, "lastfseq %s %d", vid, side);
  Cns_logreq(func, logbuf);

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

int Cns_srv_bulkexist(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char  logbuf[LOGBUFSZ];
  char  func[19];
  char  *repbuf;
  char  *rbp;
  char  *sbp;
  char  *user;
  gid_t gid;
  uid_t uid;
  u_signed64 *fileIds;
  int nbFileIds, i, c, count = 0;

  strcpy(func, "Cns_srv_bulkexist");

  /* Extract and log common request attributes */
  rbp = req_data;
  unmarshall_LONG(rbp, uid);
  unmarshall_LONG(rbp, gid);
  get_client_actual_id(thip, &uid, &gid, &user);
  nslogit(func, NS092, "bulkexist", user, uid, gid, clienthost);

  /* Extract fileIds */
  unmarshall_LONG(rbp, nbFileIds);

  sprintf(logbuf, "bulkexist %d", nbFileIds);
  if (nbFileIds > 3000) {
    Cns_logreq(func, logbuf);
    RETURN (EINVAL);
  }
  fileIds = (u_signed64*) malloc(nbFileIds * HYPERSIZE);
  for (i = 0; i < nbFileIds; i++) {
    unmarshall_HYPER(rbp, fileIds[i]);
  }

  /* Check for long database operations e.g. backups */
  c = Cns_count_long_ops(&thip->dbfd, &count, 1);
  if (c < 0) {
    Cns_logreq(func, logbuf);
    free(fileIds);
    RETURN (serrno);
  } else if (count == 0) {
    /* Check file existence */
    c = Cns_check_files_exist(&thip->dbfd, fileIds, &nbFileIds);
    if (c < 0) {
      Cns_logreq(func, logbuf);
      free(fileIds);
      RETURN (serrno);
    }
  } else {
    sprintf(logbuf, "bulkexist %d (disabled/longops)", nbFileIds);
    nbFileIds = 0;
  }
  Cns_logreq(func, logbuf);

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

int Cns_srv_tapesum(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char  func[19];
  char  vid[CA_MAXVIDLEN+1];
  char  logbuf[LOGBUFSZ];
  char  repbuf[REPBUFSZ];
  u_signed64 count = 0;
  u_signed64 size = 0;
  u_signed64 maxfileid = 0;
  char  *rbp;
  char  *sbp;
  char  *user;
  int   c;
  int   filter;
  gid_t gid;
  uid_t uid;

  strcpy(func, "Cns_srv_tapesum");

  /* Extract and log common request attributes */
  rbp = req_data;
  unmarshall_LONG(rbp, uid);
  unmarshall_LONG(rbp, gid);
  get_client_actual_id(thip, &uid, &gid, &user);
  nslogit(func, NS092, "tapesum", user, uid, gid, clienthost);

  /* Extract volume id */
  if (unmarshall_STRINGN(rbp, vid, CA_MAXVIDLEN + 1)) {
    RETURN (EINVAL);
  }
  unmarshall_LONG(rbp, filter);
  sprintf(logbuf, "tapesum %s %d", vid, filter);
  Cns_logreq(func, logbuf);

  /* Get tape summary information */
  c = Cns_get_tapesum_by_vid(&thip->dbfd, vid, filter, &count, &size, &maxfileid);
  if (c < 0) {
    RETURN (serrno);
  }

  /* Marshall response */
  sbp = repbuf;
  marshall_HYPER(sbp, count);
  marshall_HYPER(sbp, size);
  if (magic >= CNS_MAGIC5) {
    marshall_HYPER(sbp, maxfileid);
  }

  /* Send response */
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

int Cns_srv_listtape(magic, req_data, clienthost, thip, fmd_entry, smd_entry, endlist, dblistptr)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
     struct Cns_file_metadata *fmd_entry;
     struct Cns_seg_metadata *smd_entry;
     int endlist;
     DBLISTPTR *dblistptr;
{
  int bov; /* beginning of volume flag */
  int c;
  char dirbuf[DIRBUFSZ+4];
  int direntsz; /* size of client machine dirent structure excluding d_name */
  int eov = 0; /* end of volume flag */
  char func[17];
  int fseq = 0;   /* file sequence number to filter on */
  gid_t gid;
  char logbuf[LOGBUFSZ];
  int maxsize;
  int nbentries = 0;
  char *p;
  char *rbp;
  char *sbp;
  uid_t uid;
  char *user;
  char vid[CA_MAXVIDLEN+1];

  strcpy (func, "Cns_srv_listtape");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "listtape", user, uid, gid, clienthost);
  unmarshall_WORD (rbp, direntsz);
  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1))
    RETURN (EINVAL);
  unmarshall_WORD (rbp, bov);
  if (magic >= CNS_MAGIC5) {
    unmarshall_LONG (rbp, fseq);
  }
  sprintf (logbuf, "listtape %s %d %d", vid, bov, fseq);
  Cns_logreq (func, logbuf);

  bov = 0;
  if (! smd_entry->s_fileid)
    bov = 1; /* do not rely on client */
  
  /* return as many entries as possible to the client */

  maxsize = DIRBUFSZ - direntsz;
  sbp = dirbuf;
  marshall_WORD (sbp, nbentries);  /* will be updated */

  if (! bov && ! endlist) {
    marshall_DIRXT (&sbp, magic, fmd_entry, smd_entry);
    nbentries++;
    maxsize -= ((direntsz + strlen (fmd_entry->name) + 8) / 8) * 8;
  }
  while ((c = Cns_get_smd_by_vid (&thip->dbfd, bov, vid, fseq, smd_entry,
                                  endlist, dblistptr)) == 0) {
    if (Cns_get_fmd_by_fileid (&thip->dbfd, smd_entry->s_fileid,
                               fmd_entry, 0, NULL) < 0)
      RETURN (serrno);
    if ((int) strlen (fmd_entry->name) > maxsize) break;
    marshall_DIRXT (&sbp, magic, fmd_entry, smd_entry);
    nbentries++;
    bov = 0;
    maxsize -= ((direntsz + strlen (fmd_entry->name) + 8) / 8) * 8;
  }
  if (c < 0)
    RETURN (serrno);
  if (c == 1)
    eov = 1;

  marshall_WORD (sbp, eov);
  p = dirbuf;
  marshall_WORD (p, nbentries);  /* update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - dirbuf, dirbuf);
  RETURN (0);
}

/* Cns_srv_lstat - get information about a symbolic link */

int Cns_srv_lstat(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  u_signed64 fileid;
  struct Cns_file_metadata fmd_entry;
  char func[16];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  char repbuf[57];
  char *sbp;
  char tmpbuf[21];
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_lstat");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "lstat", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "lstat %s %s %s", u64tostr(fileid, tmpbuf, 0), path, cwdpath);
  Cns_logreq (func, logbuf);

  if (fileid) {
    /* get basename entry */

    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid,
                               &fmd_entry, 0, NULL))
      RETURNQ (serrno);

    /* check parent directory components for search permission */

    if (Cns_chkbackperm (&thip->dbfd, fmd_entry.parent_fileid,
                         S_IEXEC, uid, gid, clienthost))
      RETURNQ (serrno);
  } else {
    /* check parent directory components for search permission and
       get basename entry */

    if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                       NULL, NULL, &fmd_entry, NULL, CNS_NOFOLLOW|CNS_MUST_EXIST))
      RETURNQ (serrno);
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

/*      Cns_srv_mkdir - create a directory entry */

int Cns_srv_mkdir(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  struct Cns_file_metadata direntry;
  char func[16];
  gid_t gid;
  char guid[CA_MAXGUIDLEN+1];
  char logbuf[LOGBUFSZ];
  mode_t mask;
  mode_t mode;
  struct Cns_file_metadata parent_dir;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addrp;
  uid_t uid;
  char *user;
  uuid_t uuid;

  strcpy (func, "Cns_srv_mkdir");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "mkdir", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_WORD (rbp, mask);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_LONG (rbp, mode);
  if (magic >= CNS_MAGIC2) {
    if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN+1))
      RETURN (EINVAL);
    if (uuid_parse (guid, uuid) < 0)
      RETURN (EINVAL);
  } else
    *guid = '\0';
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "mkdir %s %s %o %o %s", path, guid, mode, mask, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check parent directory components for write/search permission and
     get basename entry if it exists */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                     &parent_dir, &rec_addrp, &direntry, NULL, CNS_NOFOLLOW))
    RETURN (serrno);

  if (*direntry.name == '/') /* Cns_mkdir / */
    RETURN (EEXIST);

  /* check if basename entry exists already */

  if (direntry.fileid)
    RETURN (EEXIST);

  /* build new directory entry */

  if (Cns_unique_id (&thip->dbfd, &direntry.fileid) < 0)
    RETURN (serrno);
  /* parent_fileid and name have been set by Cns_parsepath */
  strcpy (direntry.guid, guid);
  direntry.filemode = S_IFDIR | ((mode & ~S_IFMT) & ~mask);
  direntry.nlink = 0;
  direntry.uid = uid;
  if (parent_dir.filemode & S_ISGID) {
    direntry.gid = parent_dir.gid;
    if (gid == direntry.gid)
      direntry.filemode |= S_ISGID;
  } else
    direntry.gid = gid;
  direntry.atime = time (0);
  direntry.mtime = direntry.atime;
  direntry.ctime = direntry.atime;
  direntry.fileclass = parent_dir.fileclass;
  direntry.status = '-';
  if (*parent_dir.acl)
    Cns_acl_inherit (&parent_dir, &direntry, mode);

  /* write new directory entry */

  if (Cns_insert_fmd_entry (&thip->dbfd, &direntry))
    RETURN (serrno);

  /* update parent directory entry */

  parent_dir.nlink++;
  parent_dir.mtime = time (0);
  parent_dir.ctime = parent_dir.mtime;
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_modifyclass - modify an existing fileclass definition */

int Cns_srv_modifyclass(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  int bol = 1;
  struct Cns_class_metadata class_entry;
  gid_t class_group;
  char class_name[CA_MAXCLASNAMELEN+1];
  uid_t class_user;
  int classid;
  DBLISTPTR dblistptr;
  int flags;
  char func[20];
  gid_t gid;
  int i;
  char logbuf[LOGBUFSZ];
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
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_modifyclass");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "modifyclass", user, uid, gid, clienthost);
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
  sprintf (logbuf, "modifyclass %d %s", classid, class_name);
  Cns_logreq (func, logbuf);

  if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
    RETURN (serrno);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* get and lock entry */

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

  /* update entry */

  if (*new_class_name)
    strcpy (class_entry.name, new_class_name);
  if (class_user != -1)
    class_entry.uid = class_user;
  if (class_group != -1)
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

    /* delete the entries which are not needed anymore */

    while (Cns_get_tppool_by_cid (&thip->dbfd, bol, class_entry.classid,
                                  &tppool_entry, 1, &rec_addrt, 0, &dblistptr) == 0) {
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
                                  &tppool_entry, 1, &rec_addrt, 1, &dblistptr); /* free res */

    /* add the new entries if any */

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

/*      Cns_srv_opendir - open a directory entry */

int Cns_srv_opendir(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  struct Cns_file_metadata direntry;
  char func[16];
  gid_t gid;
  char guid[CA_MAXGUIDLEN+1];
  char logbuf[LOGBUFSZ];
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  char repbuf[8];
  char *sbp;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_opendir");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "opendir", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  if (magic >= CNS_MAGIC2) {
    if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN+1))
      RETURN (EINVAL);
  } else
    *guid = '\0';
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "opendir %s %s %s", path, guid, cwdpath);
  Cns_logreq (func, logbuf);

  if (*guid) {
    /* get basename entry */

    if (Cns_get_fmd_by_guid (&thip->dbfd, guid, &direntry, 0, NULL))
      RETURN (serrno);

    /* do not check parent directory components for search permission
     * as symlinks can anyway point directly at a file
     */
  } else {
    if (! cwd && *path == 0)
      RETURN (ENOENT);
    if (! cwd && *path != '/')
      RETURN (EINVAL);

    /* check parent directory components for search permission and
       get basename entry */

    if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                       NULL, NULL, &direntry, NULL, CNS_MUST_EXIST))
      RETURN (serrno);
  }
  if ((direntry.filemode & S_IFDIR) == 0)
    RETURN (ENOTDIR);
  if (Cns_chkentryperm (&direntry, S_IREAD|S_IEXEC, uid, gid, clienthost))
    RETURN (EACCES);

  /* return directory fileid */

  sbp = repbuf;
  marshall_HYPER (sbp, direntry.fileid);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/*      Cns_srv_ping - check server alive and return version number */

int Cns_srv_ping(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char func[16];
  gid_t gid;
  char info[256];
  char *rbp;
  char repbuf[REPBUFSZ];
  char *sbp;
  uid_t uid;
  char *user;

  strcpy(func, "Cns_srv_ping");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id(thip, &uid, &gid, &user);
  nslogit(func, NS092, "ping", user, uid, gid, clienthost);

  sprintf (info, "%d.%d.%d-%d", MAJORVERSION, MINORVERSION, MAJORRELEASE, MINORRELEASE);
  sbp = repbuf;
  marshall_STRING (sbp, info);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/* Cns_srv_queryclass - query about a file class */

int Cns_srv_queryclass(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  int bol = 1;
  int c;
  struct Cns_class_metadata class_entry;
  char class_name[CA_MAXCLASNAMELEN+1];
  int classid;
  DBLISTPTR dblistptr;
  char func[19];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  int nbtppools = 0;
  char *q;
  char *rbp;
  char repbuf[LISTBUFSZ];
  char *sbp;
  struct Cns_tp_pool tppool_entry;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_queryclass");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "queryclass", user, uid, gid, clienthost);
  unmarshall_LONG (rbp, classid);
  if (unmarshall_STRINGN (rbp, class_name, CA_MAXCLASNAMELEN+1))
    RETURN (EINVAL);
  sprintf (logbuf, "queryclass %d %s", classid, class_name);
  Cns_logreq (func, logbuf);

  memset((void *) &class_entry, 0, sizeof(struct Cns_class_metadata));
  if (classid > 0) {
    if (Cns_get_class_by_id (&thip->dbfd, classid, &class_entry,
                             0, NULL))
      RETURN (serrno);
    if (*class_name && strcmp (class_name, class_entry.name))
      RETURN (EINVAL);
  } else {
    if (Cns_get_class_by_name (&thip->dbfd, class_name, &class_entry,
                               0, NULL))
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

  /* get/send tppool entries */

  q = sbp;
  marshall_LONG (sbp, nbtppools); /* will be updated */
  while ((c = Cns_get_tppool_by_cid (&thip->dbfd, bol, class_entry.classid,
                                     &tppool_entry, 0, NULL, 0, &dblistptr)) == 0) {
    marshall_STRING (sbp, tppool_entry.tape_pool);
    nbtppools++;
    bol = 0;
  }
  (void) Cns_get_tppool_by_cid (&thip->dbfd, bol, class_entry.classid,
                                &tppool_entry, 0, NULL, 1, &dblistptr); /* free res */
  if (c < 0)
    RETURN (serrno);

  marshall_LONG (q, nbtppools);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURN (0);
}

/*      Cns_srv_readdir - read directory entries */

int Cns_srv_readdir(magic, req_data, clienthost, thip, fmd_entry, smd_entry, umd_entry, endlist, dblistptr, smdlistptr, beginp)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
     struct Cns_file_metadata *fmd_entry;
     struct Cns_seg_metadata *smd_entry;
     struct Cns_user_metadata *umd_entry;
     int endlist;
     DBLISTPTR *dblistptr;
     DBLISTPTR *smdlistptr;
     int *beginp;
{
  int bod; /* beginning of directory flag */
  int bof; /* beginning of file flag */
  int c;
  int cml; /* comment length */
  char dirbuf[DIRBUFSZ+4];
  struct Cns_file_metadata direntry;
  int direntsz; /* size of client machine dirent structure excluding d_name */
  u_signed64 dir_fileid;
  int eod = 0; /* end of directory flag */
  int fnl; /* filename length */
  char func[16];
  int getattr;
  gid_t gid;
  int maxsize;
  int nbentries = 0;
  char *p;
  char *rbp;
  Cns_dbrec_addr rec_addr;
  char *sbp;
  char se[CA_MAXHOSTNAMELEN+1];
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_readdir");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, endlist ? "closedir" : "readdir", user, uid, gid, clienthost);
  unmarshall_WORD (rbp, getattr);
  unmarshall_WORD (rbp, direntsz);
  unmarshall_HYPER (rbp, dir_fileid);
  unmarshall_WORD (rbp, bod);
  bod = *beginp; /* do not rely on client */
  *beginp = 0;
  if (getattr == 5 && unmarshall_STRINGN (rbp, se, CA_MAXHOSTNAMELEN+1))
    RETURN (EINVAL);

  /* return as many entries as possible to the client */
  if (getattr == 1 || getattr == 4)
    if (DIRXSIZE > direntsz)
      direntsz = DIRXSIZE;
  maxsize = DIRBUFSZ - direntsz;
  sbp = dirbuf;
  marshall_WORD (sbp, nbentries);  /* will be updated */

  if (endlist && getattr == 2)
    (void) Cns_get_smd_by_pfid (&thip->dbfd, 0, fmd_entry->fileid,
                                smd_entry, 0, NULL, 1, smdlistptr);
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
      while (1) { /* loop on segments */
        marshall_DIRXT (&sbp, magic, fmd_entry, smd_entry);
        nbentries++;
        maxsize -= ((direntsz + fnl + 8) / 8) * 8;
        if ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof,
                                      fmd_entry->fileid, smd_entry, 0, NULL,
                                      0, smdlistptr))) break;
        if (fnl >= maxsize)
          goto reply;
      }
      (void) Cns_get_smd_by_pfid (&thip->dbfd, bof,
                                  fmd_entry->fileid, smd_entry, 0, NULL, 1, smdlistptr);
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

  while ((c = Cns_get_fmd_by_pfid (&thip->dbfd, bod, dir_fileid,
                                   fmd_entry, getattr, endlist, dblistptr)) == 0) { /* loop on directory entries */
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
      while (1) { /* loop on segments */
        if ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof,
                                      fmd_entry->fileid, smd_entry, 0, NULL,
                                      0, smdlistptr))) break;
        if (fnl >= maxsize)
          goto reply;
        marshall_DIRXT (&sbp, magic, fmd_entry, smd_entry);
        nbentries++;
        bof = 0;
        maxsize -= ((direntsz + fnl + 8) / 8) * 8;
      }
      (void) Cns_get_smd_by_pfid (&thip->dbfd, bof,
                                  fmd_entry->fileid, smd_entry, 0, NULL, 1, smdlistptr);
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

    /* start transaction */

    (void) Cns_start_tr (thip->s, &thip->dbfd);

    /* update directory access time */

    if (Cns_get_fmd_by_fileid (&thip->dbfd, dir_fileid, &direntry, 1, &rec_addr))
      RETURN (serrno);
    direntry.atime = time (0);
    if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &direntry))
      RETURN (serrno);
  }

 reply:
  marshall_WORD (sbp, eod);
  p = dirbuf;
  marshall_WORD (p, nbentries);  /* update nbentries in reply */
  sendrep (thip->s, MSG_DATA, sbp - dirbuf, dirbuf);
  RETURN (0);
}

/* Cns_srv_readlink - read value of symbolic link */

int Cns_srv_readlink(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char func[17];
  gid_t gid;
  struct Cns_symlinks lnk_entry;
  char logbuf[LOGBUFSZ];
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  char repbuf[CA_MAXPATHLEN+1];
  char *sbp;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_readlink");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "readlink", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "readlink %s %s", path, cwdpath);
  Cns_logreq (func, logbuf);

  /* check parent directory components for search permission and
     get basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost, NULL,
                     NULL, &filentry, NULL, CNS_MUST_EXIST|CNS_NOFOLLOW))
    RETURNQ (serrno);

  /* check if the user is authorized to get link value for this entry */

  if (uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IREAD, uid, gid, clienthost))
    RETURNQ (EACCES);

  if ((filentry.filemode & S_IFLNK) != S_IFLNK)
    RETURNQ (EINVAL);

  /* get link value */

  if (Cns_get_lnk_by_fileid (&thip->dbfd, filentry.fileid, &lnk_entry, 0,
                             NULL))
    RETURNQ (serrno);

  sbp = repbuf;
  marshall_STRING (sbp, lnk_entry.linkname);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/*      Cns_srv_rename - rename a file or a directory */

int Cns_srv_rename(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  u_signed64 fileid;
  char func[16];
  gid_t gid;
  struct Cns_symlinks lnk_entry;
  char logbuf[LOGBUFSZ];
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
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addrl; /* symlink record address */
  Cns_dbrec_addr rec_addru; /* comment record address */
  struct Cns_file_metadata tmp_fmd_entry;
  uid_t uid;
  struct Cns_user_metadata umd_entry;
  char *user;

  strcpy (func, "Cns_srv_rename");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "rename", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, oldpath, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  if (unmarshall_STRINGN (rbp, newpath, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "rename %s %s %s", oldpath, newpath, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check 'old' parent directory components for write/search permission
     and get/lock basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, oldpath, uid, gid, clienthost,
                     &old_parent_dir, &old_rec_addrp, &old_fmd_entry, &old_rec_addr,
                     CNS_MUST_EXIST|CNS_NOFOLLOW))
    RETURN (serrno);

  /* check 'new' parent directory components for write/search permission
     and get/lock basename entry if it exists */

  if (Cns_parsepath (&thip->dbfd, cwd, newpath, uid, gid, clienthost,
                     &new_parent_dir, &new_rec_addrp, &new_fmd_entry, &new_rec_addr,
                     CNS_NOFOLLOW))
    RETURN (serrno);

  if (old_fmd_entry.fileid == new_fmd_entry.fileid)
    RETURN (0);
  if (old_fmd_entry.fileid == cwd)
    RETURN (EINVAL);

  if (*old_fmd_entry.name == '/' || *new_fmd_entry.name == '/') /* nsrename / */
    RETURN (EINVAL);

  /* if renaming a directory, 'new' must not be a descendant of 'old' */

  if (old_fmd_entry.filemode & S_IFDIR) {
    fileid = new_fmd_entry.parent_fileid;
    while (fileid) {
      if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid,
                                 &tmp_fmd_entry, 0, NULL))
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

    /* if the existing 'new' entry is a directory, the directory
       must be empty */

    if (new_fmd_entry.filemode & S_IFDIR && new_fmd_entry.nlink)
      RETURN (EEXIST);

    /* if parent of 'new' has the sticky bit set,
       the user must own 'new' or the parent of 'new' or
       the basename entry must have write permission */

    if (new_parent_dir.filemode & S_ISVTX &&
        uid != new_parent_dir.uid && uid != new_fmd_entry.uid &&
        Cns_chkentryperm (&new_fmd_entry, S_IWRITE, uid, gid, clienthost))
      RETURN (EACCES);
  }

  /* if 'old' is a directory, its basename entry must have write permission */

  if (old_fmd_entry.filemode & S_IFDIR)
    if (Cns_chkentryperm (&old_fmd_entry, S_IWRITE, uid, gid, clienthost))
      RETURN (EACCES);

  /* if parent of 'old' has the sticky bit set,
     the user must own 'old' or the parent of 'old' or
     the basename entry must have write permission */

  if (old_parent_dir.filemode & S_ISVTX &&
      uid != old_parent_dir.uid && uid != old_fmd_entry.uid &&
      Cns_chkentryperm (&old_fmd_entry, S_IWRITE, uid, gid, clienthost))
    RETURN (EACCES);

  if (new_exists) { /* must remove it */

    /* delete file segments if any */

    if (Cns_internal_dropsegs(func, thip, &new_fmd_entry) != 0)
      RETURN (serrno);

    /* if the existing 'new' entry is a symlink, delete it */

    if ((new_fmd_entry.filemode & S_IFLNK) == S_IFLNK) {
      if (Cns_get_lnk_by_fileid (&thip->dbfd, new_fmd_entry.fileid,
                                 &lnk_entry, 1, &rec_addrl))
        RETURN (serrno);
      if (Cns_delete_lnk_entry (&thip->dbfd, &rec_addrl))
        RETURN (serrno);
    }

    /* delete the comment if it exists */

    if (Cns_get_umd_by_fileid (&thip->dbfd, new_fmd_entry.fileid,
                               &umd_entry, 1, &rec_addru) == 0) {
      if (Cns_delete_umd_entry (&thip->dbfd, &rec_addru))
        RETURN (serrno);
    } else if (serrno != ENOENT)
      RETURN (serrno);

    if (Cns_delete_fmd_entry (&thip->dbfd, &new_rec_addr))
      RETURN (serrno);
  }

  /* update directory nlink value */

  if (old_parent_dir.fileid != new_parent_dir.fileid) {

    /* rename across different directories */

    old_parent_dir.nlink--;
    if (!new_exists) {
      new_parent_dir.nlink++;
    }
    new_parent_dir.mtime = time (0);
    new_parent_dir.ctime = new_parent_dir.mtime;
    if (Cns_update_fmd_entry (&thip->dbfd, &new_rec_addrp, &new_parent_dir))
      RETURN (serrno);
  } else if (new_exists) {

    /* rename within the same directory on an existing file */

    old_parent_dir.nlink--;
  }

  /* update 'old' basename entry */

  old_fmd_entry.parent_fileid = new_parent_dir.fileid;
  strcpy (old_fmd_entry.name, new_fmd_entry.name);
  old_fmd_entry.ctime = time (0);
  if (Cns_update_fmd_entry (&thip->dbfd, &old_rec_addr, &old_fmd_entry))
    RETURN (serrno);

  /* update parent directory entry */

  old_parent_dir.mtime = time (0);
  old_parent_dir.ctime = old_parent_dir.mtime;
  if (Cns_update_fmd_entry (&thip->dbfd, &old_rec_addrp, &old_parent_dir))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_updateseg_status - updates the segment status */

int Cns_srv_updateseg_status(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char *rbp;
  char *user;
  char func[25];
  char logbuf[LOGBUFSZ];
  char newstatus;
  char oldstatus;
  char tmpbuf[21];
  char vid[CA_MAXVIDLEN+1];
  Cns_dbrec_addr rec_addr;
  Cns_dbrec_addr rec_addrs;
  gid_t gid;
  int copyno;
  int fsec;
  int fseq;
  int side;
  struct Cns_file_metadata filentry;
  struct Cns_seg_metadata old_smd_entry;
  u_signed64 fileid;
  uid_t uid;

  strcpy (func, "Cns_srv_updateseg_status");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "updateseg_status", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, fileid);
  unmarshall_WORD (rbp, copyno);
  unmarshall_WORD (rbp, fsec);
  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1))
    RETURN (EINVAL);
  unmarshall_WORD (rbp, side);
  unmarshall_LONG (rbp, fseq);
  unmarshall_BYTE (rbp, oldstatus);
  unmarshall_BYTE (rbp, newstatus);
  sprintf (logbuf, "updateseg_status %s %d %d %s '%c' -> '%c'",
           u64tostr (fileid, tmpbuf, 0), copyno, fsec, vid, oldstatus, newstatus);
  Cns_logreq (func, logbuf);

  /* check if the user is authorized to set segment status */

  if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
    RETURN (serrno);

  /* check for valid status */

  if (((newstatus != '-') && (newstatus != 'D')) || (oldstatus == newstatus))
    RETURN (EINVAL);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* get/lock basename entry */

  if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &filentry, 1, &rec_addr))
    RETURN (serrno);

  /* check if the entry is a regular file */

  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);

  /* get/lock segment metadata entry to be updated */

  if (Cns_get_smd_by_fullid (&thip->dbfd, fileid, copyno, fsec,
                             &old_smd_entry, 1, &rec_addrs))
    RETURN (serrno);

  /* verify that the segment metadata return is what we expected */

  if (strcmp (old_smd_entry.vid, vid) ||
      (old_smd_entry.side != side) ||
      (old_smd_entry.fseq != fseq) ||
      (old_smd_entry.s_status != oldstatus))
    RETURN (SEENTRYNFND);

  /* update file segment entry */

  old_smd_entry.s_status = newstatus;
  if (Cns_update_smd_entry (&thip->dbfd, &rec_addrs, &old_smd_entry))
    RETURN (serrno);

  RETURN (0);
}

/* Cns_srv_updateseg_checksum - updates file segment checksum
   when previous value is NULL */

int Cns_srv_updateseg_checksum(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  int copyno;
  u_signed64 fileid;
  struct Cns_file_metadata filentry;
  int fsec;
  int fseq;
  char func[30];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  struct Cns_seg_metadata old_smd_entry;
  char *rbp;
  Cns_dbrec_addr rec_addr;
  Cns_dbrec_addr rec_addrs;
  int side;
  struct Cns_seg_metadata smd_entry;
  char tmpbuf[21];
  char tmpbuf2[21];
  uid_t uid;
  char *user;
  char vid[CA_MAXVIDLEN+1];
  int checksum_ok;

  strcpy (func, "Cns_srv_updateseg_checksum");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "updateseg_checksum", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, fileid);
  unmarshall_WORD (rbp, copyno);
  unmarshall_WORD (rbp, fsec);
  sprintf (logbuf, "updateseg_checksum %s %d %d",
           u64tostr (fileid, tmpbuf, 0), copyno, fsec);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* get/lock basename entry */

  if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &filentry, 1, &rec_addr))
    RETURN (serrno);

  /* check if the entry is a regular file */

  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);

  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1))
    RETURN (EINVAL);
  unmarshall_WORD (rbp, side);
  unmarshall_LONG (rbp, fseq);

  /* get/lock segment metadata entry to be updated */

  if (Cns_get_smd_by_fullid (&thip->dbfd, fileid, copyno, fsec,
                             &old_smd_entry, 1, &rec_addrs))
    RETURN (serrno);

  if (strcmp (old_smd_entry.vid, vid) || old_smd_entry.side != side ||
      old_smd_entry.fseq != fseq)
    RETURN (SEENTRYNFND);

  sprintf (logbuf, "updateseg_checksum old segment: %s %d %d %s %d %c %s %d %d %02x%02x%02x%02x \"%s\" %lx",
           u64tostr (old_smd_entry.s_fileid, tmpbuf, 0), old_smd_entry.copyno,
           old_smd_entry.fsec, u64tostr (old_smd_entry.segsize, tmpbuf2, 0),
           old_smd_entry.compression, old_smd_entry.s_status, old_smd_entry.vid,
           old_smd_entry.side, old_smd_entry.fseq, old_smd_entry.blockid[0],
           old_smd_entry.blockid[1], old_smd_entry.blockid[2], old_smd_entry.blockid[3],
           old_smd_entry.checksum_name, old_smd_entry.checksum);
  Cns_logreq (func, logbuf);

  /* Checking that the segment has no checksum */

  if (!(old_smd_entry.checksum_name == NULL
        || old_smd_entry.checksum_name[0] == '\0')) {
    sprintf (logbuf, "updateseg_checksum old checksum \"%s\" %lx non NULL, Cannot overwrite",
             old_smd_entry.checksum_name,
             old_smd_entry.checksum);
    Cns_logreq (func, logbuf);
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

  if (smd_entry.checksum_name == NULL
      || strlen(smd_entry.checksum_name) == 0) {
    checksum_ok = 0;
  } else {
    checksum_ok = 1;
  }

  if (magic >= CNS_MAGIC4) {
    /* Checking that we can't have a NULL checksum name when a
       checksum is specified */
    if (!checksum_ok
        && smd_entry.checksum != 0) {
      sprintf (logbuf, "updateseg_checksum NULL checksum name with non zero value, overriding");
      Cns_logreq (func, logbuf);
      smd_entry.checksum = 0;
    }
  }

  sprintf (logbuf, "updateseg_checksum new segment: %s %d %d %s %d %c %s %d %d %02x%02x%02x%02x \"%s\" %lx",
           u64tostr (smd_entry.s_fileid, tmpbuf, 0), smd_entry.copyno,
           smd_entry.fsec, u64tostr (smd_entry.segsize, tmpbuf2, 0),
           smd_entry.compression, smd_entry.s_status, smd_entry.vid,
           smd_entry.side, smd_entry.fseq, smd_entry.blockid[0],
           smd_entry.blockid[1], smd_entry.blockid[2], smd_entry.blockid[3],
           smd_entry.checksum_name, smd_entry.checksum);
  Cns_logreq (func, logbuf);

  /* update file segment entry */

  if (Cns_update_smd_entry (&thip->dbfd, &rec_addrs, &smd_entry))
    RETURN (serrno);

  RETURN (0);
}

/* Cns_srv_replaceseg - replace file segment */

int Cns_srv_replaceseg(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  int copyno;
  u_signed64 fileid;
  struct Cns_file_metadata filentry;
  int fsec;
  int fseq;
  char func[19];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  struct Cns_seg_metadata old_smd_entry;
  char *rbp;
  Cns_dbrec_addr rec_addr;
  Cns_dbrec_addr rec_addrs;
  int side;
  struct Cns_seg_metadata smd_entry;
  char tmpbuf[21];
  char tmpbuf2[21];
  uid_t uid;
  char *user;
  char vid[CA_MAXVIDLEN+1];
  int checksum_ok;
  time_t last_mod_time = 0;

  strcpy (func, "Cns_srv_replaceseg");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "replaceseg", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, fileid);
  if (magic >= CNS_MAGIC5) {
    unmarshall_TIME_T (rbp, last_mod_time);
  }
  unmarshall_WORD (rbp, copyno);
  unmarshall_WORD (rbp, fsec);
  sprintf (logbuf, "replaceseg %s %lld %d %d",
           u64tostr (fileid, tmpbuf, 0),
           (long long int)last_mod_time, copyno, fsec);
  Cns_logreq (func, logbuf);

  /* check if the user is authorized to replace segment attributes */

  if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
    RETURN (serrno);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* get/lock basename entry */

  if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &filentry, 1, &rec_addr))
    RETURN (serrno);

  /* check if the entry is a regular file */

  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);

  /* check that the file in nameserver is not newer than what
     is given. This can happen in case of multiple stagers
     concurrently modifying a file.
     In such a case, raise the appropriate error and ignore the request. */
  if ((filentry.mtime > last_mod_time) && (last_mod_time > 0)) {
    RETURN (ENSFILECHG);
  }

  if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1))
    RETURN (EINVAL);
  unmarshall_WORD (rbp, side);
  unmarshall_LONG (rbp, fseq);

  /* get/lock segment metadata entry to be updated */

  if (Cns_get_smd_by_fullid (&thip->dbfd, fileid, copyno, fsec,
                             &old_smd_entry, 1, &rec_addrs))
    RETURN (serrno);

  if (strcmp (old_smd_entry.vid, vid) || old_smd_entry.side != side ||
      old_smd_entry.fseq != fseq)
    RETURN (SEENTRYNFND);

  sprintf (logbuf, "replaceseg old segment: %s %d %d %s %d %c %s %d %d %02x%02x%02x%02x \"%s\" %lx",
           u64tostr (old_smd_entry.s_fileid, tmpbuf, 0), old_smd_entry.copyno,
           old_smd_entry.fsec, u64tostr (old_smd_entry.segsize, tmpbuf2, 0),
           old_smd_entry.compression, old_smd_entry.s_status, old_smd_entry.vid,
           old_smd_entry.side, old_smd_entry.fseq, old_smd_entry.blockid[0],
           old_smd_entry.blockid[1], old_smd_entry.blockid[2], old_smd_entry.blockid[3],
           old_smd_entry.checksum_name, old_smd_entry.checksum);
  Cns_logreq (func, logbuf);

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

  if (smd_entry.checksum_name == NULL
      || strlen(smd_entry.checksum_name) == 0) {
    checksum_ok = 0;
  } else {
    checksum_ok = 1;
  }

  if (magic >= CNS_MAGIC4) {

    /* Checking that we can't have a NULL checksum name when a
       checksum is specified */
    if (!checksum_ok
        && smd_entry.checksum != 0) {
      sprintf (logbuf, "replaceseg: NULL checksum name with non zero value, overriding");
      Cns_logreq (func, logbuf);
      smd_entry.checksum = 0;
    }
  }

  sprintf (logbuf, "replaceseg new segment: %s %d %d %s %d %c %s %d %d %02x%02x%02x%02x \"%s\" %lx",
           u64tostr (smd_entry.s_fileid, tmpbuf, 0), smd_entry.copyno,
           smd_entry.fsec, u64tostr (smd_entry.segsize, tmpbuf2, 0),
           smd_entry.compression, smd_entry.s_status, smd_entry.vid,
           smd_entry.side, smd_entry.fseq, smd_entry.blockid[0],
           smd_entry.blockid[1], smd_entry.blockid[2], smd_entry.blockid[3],
           smd_entry.checksum_name, smd_entry.checksum);
  Cns_logreq (func, logbuf);

  /* update file segment entry */

  if (Cns_update_smd_entry (&thip->dbfd, &rec_addrs, &smd_entry))
    RETURN (serrno);

  RETURN (0);
}

/* Cns_srv_replacetapecopy - replace a tapecopy
 *
 * This function replaces a tapecopy by another one.
 * For compatibility reasons, it is also able to recieve
 * >1 segs for replacement, although the new stager policy
 * is not to segment file anymore.
 * ! It deletes the old entries in the DB (no!! update to status 'D') !
 */
int Cns_srv_replacetapecopy(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 fileid = 0;
  int rc,checksum_ok, nboldsegs, nbseg ,copyno, bof, i;
  DBLISTPTR dblistptr; /* in fact an int */
  int CA_MAXSEGS = 20; /* maximum number of segments of a file */
  gid_t gid;
  uid_t uid;
  char *user;
  time_t last_mod_time = 0;

  struct Cns_seg_metadata new_smd_entry[CA_MAXSEGS];
  struct Cns_seg_metadata old_smd_entry[CA_MAXSEGS];
  struct Cns_file_metadata filentry;

  Cns_dbrec_addr backup_rec_addr[CA_MAXSEGS]; /* db keys of old segs */
  Cns_dbrec_addr rec_addr;   /* db key for file */

  char *rbp;
  char func[24];
  char logbuf[LOGBUFSZ];
  char newvid[CA_MAXVIDLEN+1];
  char oldvid[CA_MAXVIDLEN+1];

  /* the header stuff */

  strcpy (func, "Cns_srv_replacetapecopy");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "replacetapecopy", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, fileid);
  if (magic >= CNS_MAGIC5) {
    unmarshall_TIME_T (rbp, last_mod_time);
  }

  if (unmarshall_STRINGN (rbp, newvid, CA_MAXVIDLEN+1))
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, oldvid, CA_MAXVIDLEN+1))
    RETURN (EINVAL);

  sprintf (logbuf, "replacetapecopy %lld %s->%s %lld", fileid, oldvid, newvid,
           (long long int)last_mod_time);
  Cns_logreq (func, logbuf);

  /* check if the user is authorized to replacetapecopy */

  if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
    RETURN (serrno);

  /* start transaction */
  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* get/lock basename entry */
  if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &filentry, 1, &rec_addr))
    RETURN (serrno);

  /* check if the entry is a regular file */
  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);

  /* check that the file in nameserver is not newer than what
     is given. This can happen in case of multiple stagers
     concurrently modifying a file.
     In such a case, raise the appropriate error and ignore the request. */
  if ((filentry.mtime > last_mod_time) && (last_mod_time > 0)) {
    RETURN (ENSFILECHG);
  }

  /* get the old segs for a file */
  copyno = -1;
  bof = 1; /* first time: open cursor */
  nboldsegs = 0;

  while ((rc = Cns_get_smd_by_pfid (&thip->dbfd, bof,
                                    filentry.fileid,
                                    &old_smd_entry[nboldsegs],
                                    0,
                                    &backup_rec_addr[nboldsegs],
                                    0,
                                    &dblistptr)) == 0) {
    /* we want only segments, which are on the oldvid */
    if ( strcmp( old_smd_entry[nboldsegs].vid, oldvid ) == 0 ){
      /* Store the copyno for first right segment.
         we have to know it for the new segments */
      if (!nboldsegs){
        copyno = old_smd_entry[nboldsegs].copyno;
        break;
      }
    }

    bof = 0; /* no creation of cursor for next call */
  }

  if ( copyno == -1 ){
    sprintf (logbuf, "replacetapecopy cannot find old copyno");
    Cns_logreq (func, logbuf);
    RETURN (ENSNOSEG);
  }

  /* after we have the copyno, we get the old segs by this no */
  bof = 1;
  dblistptr = 0;
  while ((rc = Cns_get_smd_by_copyno (&thip->dbfd, bof,
                                      filentry.fileid,
                                      copyno,
                                      &old_smd_entry[nboldsegs],
                                      1,
                                      &backup_rec_addr[nboldsegs],
                                      0,
                                      &dblistptr )) == 0) {
    nboldsegs++;
    bof = 0;

    /* SHOULD NEVER HAPPEN !*/
    if (nboldsegs > CA_MAXSEGS ){
      sprintf (logbuf,"replacetapecopy too many segments for file %lld",
               fileid);
      Cns_logreq (func, logbuf);
      RETURN (EINVAL);
    }
  }

  if (rc < 0){
    RETURN (serrno);
  }

  if ( !nboldsegs ) {
    sprintf (logbuf, "replacetapecopy cannot find old segs for copyno %d", copyno);
    Cns_logreq (func, logbuf);
    RETURN (ENSNOSEG);
  }

  unmarshall_WORD(rbp, nbseg);
  sprintf (logbuf, "replacetapecopy replacing %d old segments by %d from stream, the tapecopyno is %d",
           nboldsegs, nbseg, copyno);
  Cns_logreq (func, logbuf);

  /* get the new segs from stream */
  for (i = 0; i < nbseg; i++) {
    memset ((char *) &new_smd_entry[i], 0, sizeof(struct Cns_seg_metadata));
    /* same fileid for all segs */
    new_smd_entry[i].s_fileid = filentry.fileid;

    unmarshall_WORD (rbp, new_smd_entry[i].copyno);
    unmarshall_WORD (rbp, new_smd_entry[i].fsec);
    unmarshall_HYPER (rbp, new_smd_entry[i].segsize);
    unmarshall_LONG (rbp, new_smd_entry[i].compression);
    unmarshall_BYTE (rbp, new_smd_entry[i].s_status);

    if (unmarshall_STRINGN (rbp, new_smd_entry[i].vid, CA_MAXVIDLEN+1))
      RETURN (EINVAL);
    if (magic >= CNS_MAGIC2)
      unmarshall_WORD (rbp, new_smd_entry[i].side);
    unmarshall_LONG (rbp, new_smd_entry[i].fseq);
    unmarshall_OPAQUE (rbp, new_smd_entry[i].blockid, 4);

    /* we want to have the same copyno as our old segs */
    new_smd_entry[i].copyno = copyno;

    if (magic >= CNS_MAGIC4) {
      unmarshall_STRINGN (rbp, new_smd_entry[i].checksum_name, CA_MAXCKSUMNAMELEN);
      new_smd_entry[i].checksum_name[CA_MAXCKSUMNAMELEN] = '\0';
      unmarshall_LONG (rbp, new_smd_entry[i].checksum);
    } else {
      new_smd_entry[i].checksum_name[0] = '\0';
      new_smd_entry[i].checksum = 0;
    }

    if (new_smd_entry[i].checksum_name == NULL ||
        strlen(new_smd_entry[i].checksum_name) == 0) {
      checksum_ok = 0;
    } else {
      checksum_ok = 1;
    }

    if (magic >= CNS_MAGIC4) {
      /* Checking that we can't have a NULL checksum name when a
         checksum is specified */
      if (!checksum_ok && new_smd_entry[i].checksum != 0) {
        sprintf (logbuf, "replacetapecopy NULL checksum name with non zero value, overriding");
        Cns_logreq (func, logbuf);
        new_smd_entry[i].checksum = 0;
      }
    }

    sprintf (logbuf, "replacetapecopy %lld %d %d %lld %d %c %s %d %02x%02x%02x%02x %s:%lx",
             new_smd_entry[i].s_fileid, new_smd_entry[i].copyno,
             new_smd_entry[i].fsec, new_smd_entry[i].segsize,
             new_smd_entry[i].compression, new_smd_entry[i].s_status, new_smd_entry[i].vid,
             new_smd_entry[i].fseq, new_smd_entry[i].blockid[0], new_smd_entry[i].blockid[1],
             new_smd_entry[i].blockid[2], new_smd_entry[i].blockid[3],
             new_smd_entry[i].checksum_name, new_smd_entry[i].checksum);
    Cns_logreq (func, logbuf);
  }

  /* remove old segs */
  for (i = 0; i < nboldsegs; i++){
    if (Cns_delete_smd_entry (&thip->dbfd, &backup_rec_addr[i])) {
      sprintf (logbuf,"replacetapecopy %s",sstrerror(serrno));
      Cns_logreq (func, logbuf);
      RETURN (serrno);
    }
  }

  /* insert new segs */
  for (i=0; i< nbseg; i++){
    /* insert new file segment entry */
    if (Cns_insert_smd_entry (&thip->dbfd,&new_smd_entry[i])){
      sprintf (logbuf,"replacetapecopy %s",sstrerror(serrno));
      Cns_logreq (func, logbuf);
      RETURN (serrno);
    }
  }

  RETURN (0);
}

/*      Cns_srv_rmdir - remove a directory entry */

int Cns_srv_rmdir(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  struct Cns_file_metadata direntry;
  char func[16];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  struct Cns_file_metadata parent_dir;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  Cns_dbrec_addr rec_addrp;
  Cns_dbrec_addr rec_addru;
  uid_t uid;
  struct Cns_user_metadata umd_entry;
  char *user;

  strcpy (func, "Cns_srv_rmdir");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "rmdir", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "rmdir %s %s", path, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check parent directory components for write/search permission and
     get/lock basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                     &parent_dir, &rec_addrp, &direntry, &rec_addr, CNS_MUST_EXIST|CNS_NOFOLLOW))
    RETURN (serrno);

  if (*direntry.name == '/') /* Cns_rmdir / */
    RETURN (EINVAL);

  if ((direntry.filemode & S_IFDIR) == 0)
    RETURN (ENOTDIR);
  if (direntry.fileid == cwd)
    RETURN (EINVAL); /* cannot remove current working directory */
  if (direntry.nlink)
    RETURN (EEXIST);

  /* if the parent has the sticky bit set,
     the user must own the directory or the parent or
     the basename entry must have write permission */

  if (parent_dir.filemode & S_ISVTX &&
      uid != parent_dir.uid && uid != direntry.uid &&
      Cns_chkentryperm (&direntry, S_IWRITE, uid, gid, clienthost))
    RETURN (EACCES);

  /* delete the comment if it exists */

  if (Cns_get_umd_by_fileid (&thip->dbfd, direntry.fileid, &umd_entry, 1,
                             &rec_addru) == 0) {
    if (Cns_delete_umd_entry (&thip->dbfd, &rec_addru))
      RETURN (serrno);
  } else if (serrno != ENOENT)
    RETURN (serrno);

  /* delete directory entry */

  if (Cns_delete_fmd_entry (&thip->dbfd, &rec_addr))
    RETURN (serrno);

  /* update parent directory entry */

  parent_dir.nlink--;
  parent_dir.mtime = time (0);
  parent_dir.ctime = parent_dir.mtime;
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
    RETURN (serrno);
  RETURN (0);
}

/*      Cns_srv_setacl - set the Access Control List for a file/directory */

int Cns_srv_setacl(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  struct Cns_acl acl[CA_MAXACLENTRIES];
  struct Cns_acl *aclp;
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  char func[16];
  gid_t gid;
  int i;
  char *iacl;
  char logbuf[LOGBUFSZ];
  int nentries;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_setacl");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "setacl", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "setacl %s %s", path, cwdpath);
  Cns_logreq (func, logbuf);

  unmarshall_WORD (rbp, nentries);
  if (nentries > CA_MAXACLENTRIES)
    RETURN (EINVAL);
  for (i = 0, aclp = acl; i < nentries; i++, aclp++) {
    unmarshall_BYTE (rbp, aclp->a_type);
    unmarshall_LONG (rbp, aclp->a_id);
    unmarshall_BYTE (rbp, aclp->a_perm);
  }

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check parent directory components for search permission and
     get/lock basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost, NULL,
                     NULL, &fmd_entry, &rec_addr, CNS_MUST_EXIST))
    RETURN (serrno);

  /* check if the user is authorized to setacl for this entry */

  if (uid != fmd_entry.uid &&
      Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
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
  if (nentries == 3) {         /* no extended ACL, just update filemode */
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

int Cns_srv_setatime(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  u_signed64 fileid;
  struct Cns_file_metadata filentry;
  char func[17];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  char tmpbuf[21];
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_setatime");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "setatime", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "setatime %s %s %s", u64tostr (fileid, tmpbuf, 0), path, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  if (fileid) {
    /* get/lock basename entry */

    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid,
                               &filentry, 1, &rec_addr))
      RETURN (serrno);

    /* check parent directory components for search permission */

    if (Cns_chkbackperm (&thip->dbfd, filentry.parent_fileid,
                         S_IEXEC, uid, gid, clienthost))
      RETURN (serrno);
  } else {
    /* check parent directory components for search permission and
       get/lock basename entry */

    if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                       NULL, NULL, &filentry, &rec_addr, CNS_MUST_EXIST))
      RETURN (serrno);
  }

  /* check if the entry is a regular file and
     if the user is authorized to set access time for this entry */

  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);
  if (uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IREAD, uid, gid, clienthost))
    RETURN (EACCES);

  /* update entry */

  filentry.atime = time (0);

  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_setcomment - add/replace a comment associated with a file/directory */

int Cns_srv_setcomment(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char comment[CA_MAXCOMMENTLEN+1];
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char func[19];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  struct Cns_user_metadata old_umd_entry;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addru;
  uid_t uid;
  struct Cns_user_metadata umd_entry;
  char *user;

  strcpy (func, "Cns_srv_setcomment");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "setcomment", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  if (unmarshall_STRINGN (rbp, comment, CA_MAXCOMMENTLEN+1))
    RETURN (EINVAL);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "setcomment %s %s", path, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check parent directory components for search permission and
     get basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost, NULL,
                     NULL, &filentry, NULL, CNS_MUST_EXIST))
    RETURN (serrno);

  /* check if the user is authorized to add/replace the comment on this entry */

  if (uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost))
    RETURN (EACCES);

  if (*comment) { /* add the comment or replace the comment if it exists */
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
  } else { /* delete the comment if it exists */
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

int Cns_srv_setfsize(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  u_signed64 fileid;
  struct Cns_file_metadata filentry;
  u_signed64 filesize;
  char func[17];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  char tmpbuf[21];
  char tmpbuf2[21];
  uid_t uid;
  char *user;
  time_t last_mod_time = 0;
  time_t new_mod_time = 0;

  strcpy (func, "Cns_srv_setfsize");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "setfsize", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_HYPER (rbp, filesize);
  if (magic >= CNS_MAGIC2) {
    unmarshall_TIME_T (rbp, new_mod_time);
    unmarshall_TIME_T (rbp, last_mod_time);
  }
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "setfsize %s %lld %lld %s %s %s", u64tostr (fileid, tmpbuf, 0),
           (long long int)last_mod_time,
           (long long int)new_mod_time,
           path, u64tostr (filesize, tmpbuf2, 0), cwdpath);
  Cns_logreq (func, logbuf);

  if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
    RETURN (serrno);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  if (fileid) {
    /* get/lock basename entry */

    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid,
                               &filentry, 1, &rec_addr))
      RETURN (serrno);

    /* check parent directory components for search permission */

    if (Cns_chkbackperm (&thip->dbfd, filentry.parent_fileid,
                         S_IEXEC, uid, gid, clienthost))
      RETURN (serrno);
  } else {
    /* check parent directory components for search permission and
       get/lock basename entry */

    if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                       NULL, NULL, &filentry, &rec_addr, CNS_MUST_EXIST))
      RETURN (serrno);
  }

  /* check if the entry is a regular file and
     if the user is authorized to set modification time for this entry */

  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);
  if (uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost))
    RETURN (EACCES);

  /* check that the file in nameserver is not newer than what
     is given. This can happen in case of multiple stagers
     concurrently modifying a file.
     In such a case, raise the appropriate error and ignore the request. */
  if ((filentry.mtime > last_mod_time) && (last_mod_time > 0)) {
    RETURN (ENSFILECHG);
  }

  /* update entry */

  filentry.filesize = filesize;
  if (magic >= CNS_MAGIC2) {
    filentry.mtime = new_mod_time;
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

int Cns_srv_setfsizecs(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  u_signed64 fileid;
  struct Cns_file_metadata filentry;
  u_signed64 filesize;
  char func[19];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  char tmpbuf[21];
  char tmpbuf2[21];
  uid_t uid;
  char *user;
  char csumtype[3];
  char csumvalue[CA_MAXCKSUMLEN+1];
  time_t last_mod_time = 0;
  time_t new_mod_time = 0;

  strcpy (func, "Cns_srv_setfsizecs");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "setfsizecs", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_HYPER (rbp, filesize);
  if (unmarshall_STRINGN (rbp, csumtype, 3))
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, csumvalue, CA_MAXCKSUMLEN+1))
    RETURN (EINVAL);
  if (magic >= CNS_MAGIC2) {
    unmarshall_TIME_T (rbp, new_mod_time);
    unmarshall_TIME_T (rbp, last_mod_time);
  }
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "setfsizecs %s %lld %lld %s %s %s %s %s",
           u64tostr (fileid, tmpbuf, 0), (long long int)last_mod_time,
           (long long int)new_mod_time, path, u64tostr (filesize, tmpbuf2, 0),
           csumvalue, csumtype, cwdpath);
  Cns_logreq (func, logbuf);
  if (*csumtype &&
      (strcmp (csumtype, "AD") && strcmp (csumtype, "PA")))
    RETURN (EINVAL);

  if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
    RETURN (serrno);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  if (fileid) {
    /* get/lock basename entry */

    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid,
                               &filentry, 1, &rec_addr))
      RETURN (serrno);

    /* check parent directory components for search permission */

    if (Cns_chkbackperm (&thip->dbfd, filentry.parent_fileid,
                         S_IEXEC, uid, gid, clienthost))
      RETURN (serrno);
  } else {
    /* check parent directory components for search permission and
       get/lock basename entry */

    if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                       NULL, NULL, &filentry, &rec_addr, CNS_MUST_EXIST))
      RETURN (serrno);
  }

  /* check if the entry is a regular file and
     if the user is authorized to set modification time for this entry */

  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);
  if (uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost))
    RETURN (EACCES);

  /* check that the file in nameserver is not newer than what
     is given. This can happen in case of multiple stagers
     concurrently modifying a file.
     In such a case, raise the appropriate error and ignore the request. */
  if ((filentry.mtime > last_mod_time) && (last_mod_time > 0)) {
    RETURN (ENSFILECHG);
  }

  if ((strcmp(filentry.csumtype, "PA") == 0 && strcmp(csumtype, "AD") == 0)) {
    /* we have predefined checksums then should check them with new ones */
    if(strcmp(filentry.csumvalue,csumvalue)!=0) {
      sprintf (logbuf, "setfsizecs: predefined checksum error 0x%s != 0x%s", filentry.csumvalue, csumvalue);
      Cns_logreq (func, logbuf);
      RETURN (SECHECKSUM);
    }
  }

  /* update entry */

  filentry.filesize = filesize;
  if (magic >= CNS_MAGIC2) {
    filentry.mtime = new_mod_time;
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

/* Cns_srv_setfsizeg - set file size and last modification time */

int Cns_srv_setfsizeg(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char csumtype[3];
  char csumvalue[CA_MAXCKSUMLEN+1];
  struct Cns_file_metadata filentry;
  u_signed64 filesize;
  char func[18];
  gid_t gid;
  char guid[CA_MAXGUIDLEN+1];
  char logbuf[LOGBUFSZ];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  char tmpbuf[21];
  uid_t uid;
  char *user;
  time_t last_mod_time = 0;
  time_t new_mod_time = 0;

  strcpy (func, "Cns_srv_setfsizeg");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "setfsizeg", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN+1))
    RETURN (EINVAL);
  unmarshall_HYPER (rbp, filesize);
  if (unmarshall_STRINGN (rbp, csumtype, 3))
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, csumvalue, CA_MAXCKSUMLEN+1))
    RETURN (EINVAL);
  if (magic >= CNS_MAGIC2) {
    unmarshall_TIME_T (rbp, new_mod_time);
    unmarshall_TIME_T (rbp, last_mod_time);
  }
  sprintf (logbuf, "setfsizeg %s %s %lld %lld", guid,
           u64tostr (filesize, tmpbuf, 0),
           (long long int)last_mod_time, (long long int)new_mod_time);
  Cns_logreq (func, logbuf);
  if (*csumtype &&
      strcmp (csumtype, "AD"))
    RETURN (EINVAL);

  if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
    RETURN (serrno);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* get/lock basename entry */

  if (Cns_get_fmd_by_guid (&thip->dbfd, guid, &filentry, 1, &rec_addr))
    RETURN (serrno);

  /* check parent directory components for search permission */

  if (Cns_chkbackperm (&thip->dbfd, filentry.parent_fileid,
                       S_IEXEC, uid, gid, clienthost))
    RETURN (serrno);

  /* check if the entry is a regular file and
     if the user is authorized to set modification time for this entry */

  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);
  if (uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost))
    RETURN (EACCES);

  /* check that the file in nameserver is not newer than what
     is given. This can happen in case of multiple stagers
     concurrently modifying a file.
     In such a case, raise the appropriate error and ignore the request. */
  if ((filentry.mtime > last_mod_time) && (last_mod_time > 0)) {
    RETURN (ENSFILECHG);
  }

  /* update entry */

  filentry.filesize = filesize;
  if (magic >= CNS_MAGIC2) {
    filentry.mtime = new_mod_time;
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

/* Cns_srv_setsegattrs - set file segment attributes */

int Cns_srv_setsegattrs(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  int copyno = 0;
  u_signed64 cwd;
  u_signed64 fileid;
  struct Cns_file_metadata filentry;
  int fsec;
  char func[20];
  gid_t gid;
  int i;
  char logbuf[LOGBUFSZ];
  int nbseg;
  struct Cns_seg_metadata old_smd_entry;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  Cns_dbrec_addr rec_addrs;
  struct Cns_seg_metadata smd_entry;
  char tmpbuf[21];
  char tmpbuf2[21];
  char tmpbuf3[50];
  uid_t uid;
  char *user;
  time_t last_mod_time = 0;

  strcpy (func, "Cns_srv_setsegattrs");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "setsegattrs", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  if (magic >= CNS_MAGIC5) {
    unmarshall_TIME_T (rbp, last_mod_time);
  }
  unmarshall_WORD (rbp, nbseg);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "setsegattrs %s %lld %s %s",
           u64tostr (fileid, tmpbuf, 0),
           (long long int)last_mod_time, path, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  if (fileid) {
    /* get/lock basename entry */

    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid,
                               &filentry, 1, &rec_addr))
      RETURN (serrno);

    /* check parent directory components for search permission */

    if (Cns_chkbackperm (&thip->dbfd, filentry.parent_fileid,
                         S_IEXEC, uid, gid, clienthost))
      RETURN (serrno);
  } else {
    /* check parent directory components for search permission and
       get/lock basename entry */

    if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                       NULL, NULL, &filentry, &rec_addr, CNS_MUST_EXIST))
      RETURN (serrno);
  }

  /* check if the entry is a regular file */
  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);

  /* check that the file in nameserver is not newer than what
     is given. This can happen in case of multiple stagers
     concurrently modifying a file.
     In such a case, raise the appropriate error and ignore the request. */
  if ((filentry.mtime > last_mod_time) && (last_mod_time > 0)) {
    RETURN (ENSFILECHG);
  }

  for (i = 0; i < nbseg; i++) {
    memset ((char *) &smd_entry, 0, sizeof(smd_entry));
    smd_entry.s_fileid = filentry.fileid;
    unmarshall_WORD (rbp, smd_entry.copyno);
    unmarshall_WORD (rbp, smd_entry.fsec);
    unmarshall_HYPER (rbp, smd_entry.segsize);
    unmarshall_LONG (rbp, smd_entry.compression);
    unmarshall_BYTE (rbp, smd_entry.s_status);
    if (unmarshall_STRINGN (rbp, smd_entry.vid, CA_MAXVIDLEN+1))
      RETURN (EINVAL);
    if (magic >= CNS_MAGIC2)
      unmarshall_WORD (rbp, smd_entry.side);
    unmarshall_LONG (rbp, smd_entry.fseq);
    unmarshall_OPAQUE (rbp, smd_entry.blockid, 4);
    if (magic >= CNS_MAGIC4) {
      unmarshall_STRINGN (rbp, smd_entry.checksum_name, CA_MAXCKSUMNAMELEN);
      smd_entry.checksum_name[CA_MAXCKSUMNAMELEN] = '\0';
      unmarshall_LONG (rbp, smd_entry.checksum);
    } else {
      smd_entry.checksum_name[0] = '\0';
      smd_entry.checksum = 0;
    }

    /* Automatically set the copy number if not provided */

    if (smd_entry.copyno == 0) {
      if (copyno == 0) {
        if (Cns_get_max_copyno (&thip->dbfd,
                                smd_entry.s_fileid, &copyno) &&
            serrno != ENOENT)
          RETURN (serrno);
        copyno++;
      }
      smd_entry.copyno = copyno;
    }
    sprintf (logbuf, "setsegattrs %s %d %d %s %d %c %s %d %02x%02x%02x%02x %s:%lx",
             u64tostr (smd_entry.s_fileid, tmpbuf, 0), smd_entry.copyno,
             smd_entry.fsec, u64tostr (smd_entry.segsize, tmpbuf2, 0),
             smd_entry.compression, smd_entry.s_status, smd_entry.vid,
             smd_entry.fseq, smd_entry.blockid[0], smd_entry.blockid[1],
             smd_entry.blockid[2], smd_entry.blockid[3],
             smd_entry.checksum_name, smd_entry.checksum);
    Cns_logreq (func, logbuf);

    if (magic >= CNS_MAGIC4) {
      /* Checking that we can't have a NULL checksum name when a
         checksum is specified */
      if ((smd_entry.checksum_name == NULL
           || strlen(smd_entry.checksum_name) == 0)
          && smd_entry.checksum != 0) {
        sprintf (logbuf, "setsegattrs: invalid checksum name with non zero value");
        RETURN (EINVAL);
      }
    }

    if ((magic >= CNS_MAGIC4) && (nbseg == 1)) {
      /* Checking for a checksum in the file metadata table if we have
         only one segment for a file. We have different names for
         checksum type in Cns_seg_metadata table and Cns_file_metadata.
         adler32==AD */
      if (((strcmp(smd_entry.checksum_name, "adler32") == 0) &&
           (strcmp(filentry.csumtype, "AD") == 0))) {
        /* we have adler32 checksum type */
        sprintf(tmpbuf3, "%lx", smd_entry.checksum);  /* convert number to a string */
        if (strncmp(tmpbuf3, filentry.csumvalue, CA_MAXCKSUMLEN)) {
          /* checksum mismatch! error! */
          sprintf (logbuf, "setsegattrs: checksum mismatch for the castor file and the segment 0x%s != 0x%lx", filentry.csumvalue, smd_entry.checksum);
          Cns_logreq (func, logbuf);
          RETURN (SECHECKSUM);
        }
      }
    }

    /* insert/update file segment entry */

    if (Cns_insert_smd_entry (&thip->dbfd, &smd_entry)) {
      if (serrno != EEXIST ||
          Cns_get_smd_by_fullid (&thip->dbfd,
                                 smd_entry.s_fileid, smd_entry.copyno,
                                 smd_entry.fsec, &old_smd_entry, 1, &rec_addrs) ||
          Cns_update_smd_entry (&thip->dbfd, &rec_addrs,
                                &smd_entry))
        RETURN (serrno);
    }
  }

  /* delete old segments if they were more numerous */

  fsec = nbseg + 1;
  while (Cns_get_smd_by_fullid (&thip->dbfd, smd_entry.s_fileid, copyno,
                                fsec, &old_smd_entry, 1, &rec_addrs) == 0) {
    if (Cns_delete_smd_entry (&thip->dbfd, &rec_addrs))
      RETURN (serrno);
    fsec++;
  }

  if (filentry.status != 'm') {
    filentry.status = 'm';
    if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
      RETURN (serrno);
  }
  RETURN (0);
}

/*      Cns_srv_dropsegs - drops all segments of a file */

int Cns_srv_dropsegs(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char func[17];
  uid_t uid;
  gid_t gid;
  u_signed64 fileid;
  struct Cns_file_metadata parent_dir;
  char path[CA_MAXPATHLEN+1];
  char *rbp;
  Cns_dbrec_addr rec_addr; /* file record address */
  char *user;

  strcpy (func, "Cns_srv_dropsegs");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "setsegattrs", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);

  /* start transaction */
  (void) Cns_start_tr (thip->s, &thip->dbfd);

  if (fileid) {
    /* get/lock basename entry */
    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid,
                               &filentry, 1, &rec_addr))
      RETURN (serrno);
    /* check parent directory components for search permission */
    if (Cns_chkbackperm (&thip->dbfd, filentry.parent_fileid,
                         S_IEXEC, uid, gid, clienthost))
      RETURN (serrno);
  } else {
    /* check parent directory components for search permission and
       get/lock basename entry */
    if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                       NULL, NULL, &filentry, &rec_addr, CNS_MUST_EXIST))
      RETURN (serrno);
  }
  /* check if the entry is a regular file */
  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);
  if ((filentry.filemode & S_IFLNK) == S_IFLNK) {
    RETURN (ENSISLINK);
    /* if the parent has the sticky bit set,
       the user must own the file or the parent or
       the basename entry must have write permission */
    if (parent_dir.filemode & S_ISVTX &&
        uid != parent_dir.uid && uid != filentry.uid &&
        Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost))
      RETURN (EACCES);
  }

  /* delete file segments if any */
  if (Cns_internal_dropsegs(func, thip, &filentry) != 0)
    RETURN (serrno);

  RETURN (0);
}

/* Cns_srv_startsess - start session */

int Cns_srv_startsess(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char comment[CA_MAXCOMMENTLEN+1];
  char func[18];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char *rbp;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_startsess");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "startsess", user, uid, gid, clienthost);
  if (unmarshall_STRINGN (rbp, comment, CA_MAXCOMMENTLEN+1))
    RETURN (EINVAL);
  sprintf (logbuf, "startsess (%s)", comment);
  Cns_logreq (func, logbuf);
  RETURN (0);
}

/* Cns_srv_starttrans - start transaction mode */

int Cns_srv_starttrans(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char comment[CA_MAXCOMMENTLEN+1];
  char func[19];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char *rbp;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_starttrans");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "starttrans", user, uid, gid, clienthost);
  if (magic >= CNS_MAGIC2) {
    if (unmarshall_STRINGN (rbp, comment, CA_MAXCOMMENTLEN+1))
      RETURN (EINVAL);
    sprintf (logbuf, "starttrans (%s)", comment);
    Cns_logreq (func, logbuf);
  }

  (void) Cns_start_tr (thip->s, &thip->dbfd);
  thip->dbfd.tr_mode++;
  RETURN (0);
}

/* Cns_srv_stat - get information about a file or a directory */

int Cns_srv_stat(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  u_signed64 fileid;
  struct Cns_file_metadata fmd_entry;
  char func[16];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  char repbuf[57];
  char *sbp;
  char tmpbuf[21];
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_stat");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "stat", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "stat %s %s %s", u64tostr(fileid, tmpbuf, 0), path, cwdpath);
  Cns_logreq (func, logbuf);

  if (fileid) {
    /* get basename entry */

    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid,
                               &fmd_entry, 0, NULL))
      RETURNQ (serrno);

    /* check parent directory components for search permission */

    if (Cns_chkbackperm (&thip->dbfd, fmd_entry.parent_fileid,
                         S_IEXEC, uid, gid, clienthost))
      RETURNQ (serrno);
  } else {
    /* check parent directory components for search permission and
       get basename entry */

    if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                       NULL, NULL, &fmd_entry, NULL, CNS_MUST_EXIST))
      RETURNQ (serrno);
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

int Cns_srv_statcs(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  u_signed64 fileid;
  struct Cns_file_metadata fmd_entry;
  char func[16];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  char repbuf[93];
  char *sbp;
  char tmpbuf[21];
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_statcs");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "statcs", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cwd);
  unmarshall_HYPER (rbp, fileid);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "statcs %s %s %s", u64tostr(fileid, tmpbuf, 0), path, cwdpath);
  Cns_logreq (func, logbuf);

  if (fileid) {
    /* get basename entry */

    if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid,
                               &fmd_entry, 0, NULL))
      RETURNQ (serrno);

    /* check parent directory components for search permission */

    if (Cns_chkbackperm (&thip->dbfd, fmd_entry.parent_fileid,
                         S_IEXEC, uid, gid, clienthost))
      RETURNQ (serrno);
  } else {
    /* check parent directory components for search permission and
       get basename entry */

    if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                       NULL, NULL, &fmd_entry, NULL, CNS_MUST_EXIST))
      RETURNQ (serrno);
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

int Cns_srv_statg(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  struct Cns_file_metadata fmd_entry;
  char func[16];
  gid_t gid;
  char guid[CA_MAXGUIDLEN+1];
  char logbuf[LOGBUFSZ];
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  char repbuf[130];
  char *sbp;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_statg");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "statg", user, uid, gid, clienthost);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURNQ (SENAMETOOLONG);
  if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN+1))
    RETURNQ (EINVAL);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "statg %s %s %s", path, guid, cwdpath);
  Cns_logreq (func, logbuf);

  if (*path) {
    /* check parent directory components for search permission and
       get basename entry */

    if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                       NULL, NULL, &fmd_entry, NULL, CNS_MUST_EXIST))
      RETURNQ (serrno);
    if (*guid && strcmp (guid, fmd_entry.guid))
      RETURNQ (EINVAL);
  } else {
    if (! *guid)
      RETURNQ (ENOENT);
    /* get basename entry */

    if (Cns_get_fmd_by_guid (&thip->dbfd, guid, &fmd_entry, 0, NULL))
      RETURNQ (serrno);

    /* do not check parent directory components for search permission
     * as symlinks can anyway point directly at a file
     */
  }
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

/*      Cns_srv_symlink - make a symbolic link to a file or a directory */

int Cns_srv_symlink(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  char func[16];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  struct Cns_file_metadata fmd_entry;
  char linkname[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  struct Cns_symlinks lnk_entry;
  struct Cns_file_metadata parent_dir;
  char *rbp;
  Cns_dbrec_addr rec_addrp;
  char target[CA_MAXPATHLEN+1];
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_symlink");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "symlink", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, target, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  if (unmarshall_STRINGN (rbp, linkname, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "symlink %s %s %s", target, linkname, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check parent directory components for write/search permission and
     get basename entry if it exists */

  if (Cns_parsepath (&thip->dbfd, cwd, linkname, uid, gid, clienthost,
                     &parent_dir, &rec_addrp, &fmd_entry, NULL, CNS_NOFOLLOW))
    RETURN (serrno);

  /* check if 'linkname' basename entry exists already */

  if (fmd_entry.fileid)
    RETURN (EEXIST);

  /* build new entry */

  if (Cns_unique_id (&thip->dbfd, &fmd_entry.fileid) < 0)
    RETURN (serrno);
  /* parent_fileid and name have been set by Cns_parsepath */
  fmd_entry.filemode = S_IFLNK | 0777;
  fmd_entry.nlink = 1;
  fmd_entry.uid = uid;
  if (parent_dir.filemode & S_ISGID)
    fmd_entry.gid = parent_dir.gid;
  else
    fmd_entry.gid = gid;
  fmd_entry.atime = time (0);
  fmd_entry.mtime = fmd_entry.atime;
  fmd_entry.ctime = fmd_entry.atime;
  fmd_entry.fileclass = 0;
  fmd_entry.status = '-';

  memset ((char *) &lnk_entry, 0, sizeof(lnk_entry));
  lnk_entry.fileid = fmd_entry.fileid;
  strcpy (lnk_entry.linkname, target);

  /* write new entry */

  if (Cns_insert_fmd_entry (&thip->dbfd, &fmd_entry))
    RETURN (serrno);
  if (Cns_insert_lnk_entry (&thip->dbfd, &lnk_entry))
    RETURN (serrno);

  /* update parent directory entry */

  parent_dir.nlink++;
  parent_dir.mtime = time (0);
  parent_dir.ctime = parent_dir.mtime;
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
    RETURN (serrno);
  RETURN (0);
}

/*      Cns_srv_undelete - logically restore a file entry */

int Cns_srv_undelete(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  int bof = 1;
  int c;
  u_signed64 cwd;
  DBLISTPTR dblistptr;
  struct Cns_file_metadata filentry;
  char func[17];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  struct Cns_file_metadata parent_dir;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addr;  /* file record address */
  Cns_dbrec_addr rec_addrp; /* parent record address */
  Cns_dbrec_addr rec_addrs; /* segment record address */
  struct Cns_seg_metadata smd_entry;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_undelete");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "undelete", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "undelete %s %s", path, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check parent directory components for write/search permission and
     get/lock basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                     &parent_dir, &rec_addrp, &filentry, &rec_addr, CNS_MUST_EXIST|CNS_NOFOLLOW))
    RETURN (serrno);

  if (*filentry.name == '/') /* Cns_undelete / */
    RETURN (EINVAL);

  if (filentry.filemode & S_IFDIR)
    RETURN (EPERM);

  /* if the parent has the sticky bit set,
     the user must own the file or the parent or
     the basename entry must have write permission */

  if (parent_dir.filemode & S_ISVTX &&
      uid != parent_dir.uid && uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost))
    RETURN (EACCES);

  /* remove the mark "logically deleted" on the file segments if any */

  while ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid,
                                   &smd_entry, 1, &rec_addrs, 0, &dblistptr)) == 0) {
    smd_entry.s_status = '-';
    if (Cns_update_smd_entry (&thip->dbfd, &rec_addrs, &smd_entry))
      RETURN (serrno);
    bof = 0;
  }
  (void) Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid,
                              &smd_entry, 1, &rec_addrs, 1, &dblistptr); /* free res */
  if (c < 0)
    RETURN (serrno);

  /* remove the mark "logically deleted" */

  if (bof)
    filentry.status = '-';
  else
    filentry.status = 'm';
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
    RETURN (serrno);

  /* update parent directory entry */

  parent_dir.mtime = time (0);
  parent_dir.ctime = parent_dir.mtime;
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
    RETURN (serrno);
  RETURN (0);
}

/*      Cns_srv_unlink - remove a file entry */

int Cns_srv_unlink(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char func[16];
  gid_t gid;
  struct Cns_symlinks lnk_entry;
  char logbuf[LOGBUFSZ];
  struct Cns_file_metadata parent_dir;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addr;  /* file record address */
  Cns_dbrec_addr rec_addrl; /* link record address */
  Cns_dbrec_addr rec_addrp; /* parent record address */
  Cns_dbrec_addr rec_addru; /* comment record address */
  uid_t uid;
  struct Cns_user_metadata umd_entry;
  char *user;

  strcpy (func, "Cns_srv_unlink");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "unlink", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "unlink %s %s", path, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check parent directory components for write/search permission and
     get/lock basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
                     &parent_dir, &rec_addrp, &filentry, &rec_addr, CNS_MUST_EXIST|CNS_NOFOLLOW))
    RETURN (serrno);

  if (*filentry.name == '/') /* Cns_unlink / */
    RETURN (EINVAL);

  if (filentry.filemode & S_IFDIR)
    RETURN (EPERM);

  /* if the parent has the sticky bit set,
     the user must own the file or the parent or
     the basename entry must have write permission */

  if (parent_dir.filemode & S_ISVTX &&
      uid != parent_dir.uid && uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost))
    RETURN (EACCES);

  if ((filentry.filemode & S_IFLNK) == S_IFLNK) {
    if (Cns_get_lnk_by_fileid (&thip->dbfd, filentry.fileid,
                               &lnk_entry, 1, &rec_addrl))
      RETURN (serrno);
    if (Cns_delete_lnk_entry (&thip->dbfd, &rec_addrl))
      RETURN (serrno);
  } else {

    /* delete file segments if any */

    if (Cns_internal_dropsegs(func, thip, &filentry) != 0)
      RETURN (serrno);

    /* delete the comment if it exists */

    if (Cns_get_umd_by_fileid (&thip->dbfd, filentry.fileid, &umd_entry, 1,
                               &rec_addru) == 0) {
      if (Cns_delete_umd_entry (&thip->dbfd, &rec_addru))
        RETURN (serrno);
    } else if (serrno != ENOENT)
      RETURN (serrno);
  }

  /* delete file entry */

  if (Cns_delete_fmd_entry (&thip->dbfd, &rec_addr))
    RETURN (serrno);

  /* update parent directory entry */

  parent_dir.nlink--;
  parent_dir.mtime = time (0);
  parent_dir.ctime = parent_dir.mtime;
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_utime - set last access and modification times */

int Cns_srv_utime(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  time_t actime;
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char func[16];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  time_t modtime;
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  uid_t uid;
  char *user;
  int user_specified_time;

  strcpy (func, "Cns_srv_utime");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "utime", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  unmarshall_LONG (rbp, user_specified_time);
  if (user_specified_time) {
    unmarshall_TIME_T (rbp, actime);
    unmarshall_TIME_T (rbp, modtime);
  }
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "utime %s %d %s", path, user_specified_time, cwdpath);
  Cns_logreq (func, logbuf);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check parent directory components for search permission and
     get/lock basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost, NULL,
                     NULL, &filentry, &rec_addr, CNS_MUST_EXIST))
    RETURN (serrno);

  /* check if the user is authorized to set access/modification time
     for this entry */

  if (user_specified_time) {
    if (uid != filentry.uid &&
        Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
      RETURN (EPERM);
  } else {
    if (uid != filentry.uid &&
        Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost))
      RETURN (EACCES);
  }

  /* update entry */

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

int Cns_srv_updatefile_checksum(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  u_signed64 cwd;
  struct Cns_file_metadata filentry;
  char func[28];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char path[CA_MAXPATHLEN+1];
  char cwdpath[CA_MAXPATHLEN+10];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  uid_t uid;
  char *user;
  char csumtype[3];
  char csumvalue[CA_MAXCKSUMLEN+1];
  int notAdmin = 1;

  strcpy (func, "Cns_srv_updatefile_checksum");
  rbp = req_data;
  unmarshall_LONG (rbp, uid);
  unmarshall_LONG (rbp, gid);
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "updatefile_checksum", user, uid, gid, clienthost);
  if (rdonly)
    RETURN (EROFS);
  unmarshall_HYPER (rbp, cwd);
  if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
    RETURN (SENAMETOOLONG);
  if (unmarshall_STRINGN (rbp, csumtype, 3))
    RETURN (EINVAL);
  if (unmarshall_STRINGN (rbp, csumvalue, CA_MAXCKSUMLEN+1))
    RETURN (EINVAL);
  get_cwd_path (thip, cwd, cwdpath);
  sprintf (logbuf, "updatefile_checksum %s %s %s %s", path, csumvalue, csumtype, cwdpath);
  Cns_logreq (func, logbuf);
  if (*csumtype &&
      strcmp (csumtype, "PA") &&
      strcmp (csumtype, "AD"))
    RETURN (EINVAL);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  /* check parent directory components for search permission and
     get/lock basename entry */

  if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost, NULL,
                     NULL, &filentry, &rec_addr, CNS_MUST_EXIST))
    RETURN (serrno);

  /* check if the entry is a regular file */
  if (filentry.filemode & S_IFDIR)
    RETURN (EISDIR);

  /* check if the user is authorized to set access/modification checksum for this entry
     the file must be 0 size or the user must be ADMIN
     the checksum must be a known one (i.e. adler) or empty (case of a reset, user must be ADMIN then) */
  notAdmin = Cupv_check (uid, gid, clienthost, localhost, P_ADMIN);
  if (filentry.filesize > 0 && notAdmin) {
    RETURN (EPERM);
  }
  if (notAdmin && uid != filentry.uid &&
      Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost)) {
    RETURN (EACCES);
  }

  /* update entry only if not admin */
  if (notAdmin) {
    filentry.mtime = time(0);
    filentry.ctime = filentry.mtime;
  }

  strcpy (filentry.csumtype, csumtype);
  if (*csumtype == '\0') {
    strcpy (filentry.csumvalue, ""); /* reset value for empty types */
  } else {
    strcpy (filentry.csumvalue, csumvalue);
  }
  if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
    RETURN (serrno);
  RETURN (0);
}

/* Routines for identity mapping */

extern char lcgdmmapfile[];

int Cns_vo_from_dn(const char *dn, char *vo)
{
  char buf[1024];
  char func[16];
  FILE *fopen(), *mf;
  char *p;
  char *q;

  strcpy (func, "Cns_vo_from_dn");
  if (! dn)
    return (EFAULT);
  if (! *lcgdmmapfile)
    strcpy (lcgdmmapfile, LCGDM_MAPFILE);
  if ((mf = fopen (lcgdmmapfile, "r")) == NULL) {
    nslogit (func, NS023, lcgdmmapfile);
    return (SENOCONFIG);
  }
  while (fgets (buf, sizeof(buf), mf)) {
    buf[strlen (buf)-1] = '\0';
    p = buf;
    while (*p == ' ' || *p == '\t') /* skip leading blanks */
      p++;
    if (*p == '\0') continue; /* empty line */
    if (*p == '#') continue; /* comment */
    if (*p == '"') {
      q = p + 1;
      if ((p = strrchr (q, '"')) == NULL) continue;
    } else {
      q = p;
      while (*p !=  ' ' && *p != '\t' && *p != '\0')
        p++;
      if (*p == '\0') continue; /* no vo */
    }
    *p = '\0';
    if (strcmp (dn, q)) continue; /* DN does not match */
    p++;
    while (*p == ' ' || *p == '\t') /* skip blanks between dn and vo */
      p++;
    q = p;
    while (*p !=  ' ' && *p != '\t' && *p != '\0' && *p != ',')
      p++;
    *p = '\0';
    strcpy (vo, q);
    fclose (mf);
    return (0);
  }
  fclose (mf);
  return (SENOMAPFND);
}

/* Cns_srv_entergrpmap - define a new group entry in Virtual Id table */

int Cns_srv_entergrpmap(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char func[20];
  gid_t gid;
  struct Cns_groupinfo group_entry;
  char logbuf[LOGBUFSZ];
  char *rbp;
  gid_t reqgid;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_entergrpmap");
  rbp = req_data;
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "entergrpmap", user, uid, gid, clienthost);
  unmarshall_LONG (rbp, reqgid);
  memset ((char *) &group_entry, 0, sizeof(group_entry));
  if (unmarshall_STRINGN (rbp, group_entry.groupname, sizeof(group_entry.groupname)))
    RETURN (EINVAL);
  sprintf (logbuf, "entergrpmap %d %s", reqgid, group_entry.groupname);
  Cns_logreq (func, logbuf);

  if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
    RETURN (serrno);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  if (reqgid == -1) {
    if (Cns_unique_gid (&thip->dbfd, &group_entry.gid) < 0)
      RETURN (serrno);
  } else {
    if (reqgid == 0 || reqgid > CA_MAXGID)
      RETURN (EINVAL);
    group_entry.gid = reqgid;
  }
  if (Cns_insert_group_entry (&thip->dbfd, &group_entry))
    RETURN (serrno);
  if (Cns_update_unique_gid (&thip->dbfd, group_entry.gid))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_enterusrmap - define a new user entry in Virtual Id table */

int Cns_srv_enterusrmap(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char func[20];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char *rbp;
  uid_t requid;
  uid_t uid;
  char *user;
  struct Cns_userinfo user_entry;

  strcpy (func, "Cns_srv_enterusrmap");
  rbp = req_data;
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "enterusrmap", user, uid, gid, clienthost);
  unmarshall_LONG (rbp, requid);
  memset ((char *) &user_entry, 0, sizeof(user_entry));
  if (unmarshall_STRINGN (rbp, user_entry.username, sizeof(user_entry.username)))
    RETURN (EINVAL);
  sprintf (logbuf, "enterusrmap %d %s", requid, user_entry.username);
  Cns_logreq (func, logbuf);

  if (Cupv_check (uid, uid, clienthost, localhost, P_ADMIN))
    RETURN (serrno);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  if (requid == -1) {
    if (Cns_unique_uid (&thip->dbfd, &user_entry.userid) < 0)
      RETURN (serrno);
  } else {
    if (requid == 0 || requid > CA_MAXUID)
      RETURN (EINVAL);
    user_entry.userid = requid;
  }
  if (Cns_insert_user_entry (&thip->dbfd, &user_entry))
    RETURN (serrno);
  if (Cns_update_unique_uid (&thip->dbfd, user_entry.userid))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_getidmap - get uid/gids associated with a given dn/roles */

int getonegid(dbfd, groupname, gid)
     struct Cns_dbfd *dbfd;
     char *groupname;
     gid_t *gid;
{
  int c;
  struct Cns_groupinfo group_entry;

  if ((c = Cns_get_grpinfo_by_name (dbfd, groupname,
                                    &group_entry, 0, NULL)) && serrno != ENOENT)
    return (serrno);
  if (c) { /* must create an entry */
    if (Cns_unique_gid (dbfd, &group_entry.gid) < 0)
      return (serrno);
    strcpy (group_entry.groupname, groupname);
    if (Cns_insert_group_entry (dbfd, &group_entry) < 0 &&
        serrno != EEXIST)
      return (serrno);
    if (Cns_get_grpinfo_by_name (dbfd, groupname,
                                 &group_entry, 0, NULL))
      return (serrno);
  }
  *gid = group_entry.gid;
  return (0);
}

int getidmap(dbfd, username, nbgroups, groupnames, userid, gids)
     struct Cns_dbfd *dbfd;
     char *username;
     int nbgroups;
     char **groupnames;
     uid_t *userid;
     gid_t *gids;
{
  int c;
  char *groupname;
  int i;
  char *p;
  struct Cns_userinfo user_entry;
  char vo[256];

  if (! username || ! userid || ! gids)
    return (EFAULT);
  if (nbgroups < 0)
    return (EINVAL);
  if ((c = Cns_get_usrinfo_by_name (dbfd, username, &user_entry, 0, NULL)) &&
      serrno != ENOENT)
    return (serrno);
  if (c) { /* must create an entry */
    if (Cns_unique_uid (dbfd, &user_entry.userid) < 0)
      return (serrno);
    strcpy (user_entry.username, username);
    if (Cns_insert_user_entry (dbfd, &user_entry) < 0 &&
        serrno != EEXIST)
      return (serrno);
    if (Cns_get_usrinfo_by_name (dbfd, username, &user_entry, 0, NULL))
      return (serrno);
  }
  *userid = user_entry.userid;

  if (groupnames == NULL) { /* not using VOMS */
    if ((c = Cns_vo_from_dn (username, vo)))
      return (c);
    return (getonegid (dbfd, vo, &gids[0]));
  }
  for (i = 0; i < nbgroups; i++) {
    groupname = groupnames[i];
    if (*groupname == '/')
      groupname++; /* skipping leading slash */
    if ((p = strstr (groupname, "/Role=NULL")))
      *p = '\0';
    else if ((p = strstr (groupname, "/Capability=NULL")))
      *p = '\0';
    if ((c = getonegid (dbfd, groupname, &gids[i])))
      return (c);
  }
  return (0);
}

int Cns_srv_getidmap(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  int c;
  char func[17];
  gid_t gid;
  gid_t *gids;
  char **groupnames = NULL;
  int i;
  char logbuf[LOGBUFSZ];
  int nbgroups;
  char *p;
  char *q;
  char *rbp;
  char repbuf[REPBUFSZ];
  char *sbp;
  uid_t uid;
  char *user;
  uid_t userid;
  char username[256];

  strcpy (func, "Cns_srv_getidmap");
  rbp = req_data;
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "getidmap", user, uid, gid, clienthost);
  if (unmarshall_STRINGN (rbp, username, sizeof(username)) ||
      *username == '\0')
    RETURN (EINVAL);
  sprintf (logbuf, "getidmap %s", username);
  Cns_logreq (func, logbuf);
  unmarshall_LONG (rbp, nbgroups);
  if (nbgroups < 0)
    RETURN (EINVAL);
  if (nbgroups) { /* using VOMS */
    if ((groupnames = malloc (nbgroups * sizeof(char *))) == NULL)
      RETURN (ENOMEM);
    if ((q = malloc (nbgroups * 256)) == NULL) {
      free (groupnames);
      RETURN (ENOMEM);
    }
    p = q;
    for (i = 0; i < nbgroups; i++) {
      groupnames[i] = p;
      if (unmarshall_STRINGN (rbp, p, 256)) {
        free (q);
        free (groupnames);
        RETURN (EINVAL);
      }
      p += 256;
    }
  } else {
    nbgroups = 1;
  }
  if ((gids = malloc (nbgroups * sizeof(gid_t))) == NULL) {
    free (q);
    free (groupnames);
    RETURN (ENOMEM);
  }
  c = getidmap (&thip->dbfd, username, nbgroups, groupnames, &userid, gids);
  free (q);
  free (groupnames);
  if (c == 0) {
    sbp = repbuf;
    marshall_LONG (sbp, userid);
    for (i = 0; i < nbgroups; i++) {
      marshall_LONG (sbp, gids[i]);
    }
    sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  }
  free (gids);
  RETURN (c);
}

/* Cns_srv_getgrpbygid - get group name associated with a given gid */

int Cns_srv_getgrpbygid(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char func[20];
  gid_t gid;
  struct Cns_groupinfo group_entry;
  char logbuf[LOGBUFSZ];
  char *rbp;
  char repbuf[256];
  gid_t reqgid;
  char *sbp;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_getgrpbygid");
  rbp = req_data;
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "getgrpbygid", user, uid, gid, clienthost);
  unmarshall_LONG (rbp, reqgid);
  sprintf (logbuf, "getgrpbygid %d", reqgid);
  Cns_logreq (func, logbuf);

  if (Cns_get_grpinfo_by_gid (&thip->dbfd, reqgid, &group_entry, 0, NULL) < 0) {
    if (serrno == ENOENT) {
      sendrep (thip->s, MSG_ERR, "No such gid\n");
      RETURNQ (EINVAL);
    } else {
      RETURNQ (serrno);
    }
  }
  sbp = repbuf;
  marshall_STRING (sbp, group_entry.groupname);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_getgrpbynam - get gid associated with a given group name */

int Cns_srv_getgrpbynam(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char func[20];
  gid_t gid;
  struct Cns_groupinfo group_entry;
  char groupname[256];
  char logbuf[LOGBUFSZ];
  char *rbp;
  char repbuf[4];
  char *sbp;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_getgrpbynam");
  rbp = req_data;
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "getgrpbynam", user, uid, gid, clienthost);
  if (unmarshall_STRINGN (rbp, groupname, sizeof(groupname)))
    RETURNQ (EINVAL);
  sprintf (logbuf, "getgrpbynam %s", groupname);
  Cns_logreq (func, logbuf);

  if (Cns_get_grpinfo_by_name (&thip->dbfd, groupname, &group_entry, 0, NULL) < 0) {
    if (serrno == ENOENT) {
      sendrep (thip->s, MSG_ERR, "No such group\n");
      RETURNQ (EINVAL);
    } else {
      RETURNQ (serrno);
    }
  }
  sbp = repbuf;
  marshall_LONG (sbp, group_entry.gid);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_getusrbynam - get uid associated with a given user name */

int Cns_srv_getusrbynam(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char func[20];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char *rbp;
  char repbuf[4];
  char *sbp;
  uid_t uid;
  char *user;
  struct Cns_userinfo user_entry;
  char username[256];

  strcpy (func, "Cns_srv_getusrbynam");
  rbp = req_data;
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "getusrbynam", user, uid, gid, clienthost);
  if (unmarshall_STRINGN (rbp, username, sizeof(username)))
    RETURNQ (EINVAL);
  sprintf (logbuf, "getusrbynam %s", username);
  Cns_logreq (func, logbuf);

  if (Cns_get_usrinfo_by_name (&thip->dbfd, username, &user_entry, 0, NULL) < 0) {
    if (serrno == ENOENT) {
      sendrep (thip->s, MSG_ERR, "No such user\n");
      RETURNQ (EINVAL);
    } else {
      RETURNQ (serrno);
    }
  }
  sbp = repbuf;
  marshall_LONG (sbp, user_entry.userid);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_getusrbyuid - get user name associated with a given uid */

int Cns_srv_getusrbyuid(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char func[20];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char *rbp;
  char repbuf[256];
  gid_t requid;
  char *sbp;
  uid_t uid;
  char *user;
  struct Cns_userinfo user_entry;

  strcpy (func, "Cns_srv_getusrbyuid");
  rbp = req_data;
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "getusrbyuid", user, uid, gid, clienthost);
  unmarshall_LONG (rbp, requid);
  sprintf (logbuf, "getusrbyuid %d", requid);
  Cns_logreq (func, logbuf);

  if (Cns_get_usrinfo_by_uid (&thip->dbfd, requid, &user_entry, 0, NULL) < 0) {
    if (serrno == ENOENT) {
      sendrep (thip->s, MSG_ERR, "No such uid\n");
      RETURNQ (EINVAL);
    } else {
      RETURNQ (serrno);
    }
  }
  sbp = repbuf;
  marshall_STRING (sbp, user_entry.username);
  sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
  RETURNQ (0);
}

/* Cns_srv_modgrpmap - modify group name associated with a given gid */

int Cns_srv_modgrpmap(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char func[18];
  gid_t gid;
  struct Cns_groupinfo group_entry;
  char groupname[256];
  char logbuf[LOGBUFSZ];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  gid_t reqgid;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_modgrpmap");
  rbp = req_data;
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "modgrpmap", user, uid, gid, clienthost);
  unmarshall_LONG (rbp, reqgid);
  if (unmarshall_STRINGN (rbp, groupname, sizeof(groupname)) ||
      *groupname == '\0')
    RETURN (EINVAL);
  sprintf (logbuf, "modgrpmap %d %s", reqgid, groupname);
  Cns_logreq (func, logbuf);

  if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
    RETURN (serrno);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  if (Cns_get_grpinfo_by_gid (&thip->dbfd, reqgid, &group_entry, 1, &rec_addr) < 0) {
    if (serrno == ENOENT) {
      sendrep (thip->s, MSG_ERR, "No such gid\n");
      RETURN (EINVAL);
    } else {
      RETURN (serrno);
    }
  }
  strcpy (group_entry.groupname, groupname);
  if (Cns_update_group_entry (&thip->dbfd, &rec_addr, &group_entry))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_modusrmap - modify user name associated with a given uid */

int Cns_srv_modusrmap(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char func[18];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  gid_t requid;
  uid_t uid;
  char *user;
  struct Cns_userinfo user_entry;
  char username[256];

  strcpy (func, "Cns_srv_modusrmap");
  rbp = req_data;
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "modusrmap", user, uid, gid, clienthost);
  unmarshall_LONG (rbp, requid);
  if (unmarshall_STRINGN (rbp, username, sizeof(username)) ||
      *username == '\0')
    RETURN (EINVAL);
  sprintf (logbuf, "modusrmap %d %s", requid, username);
  Cns_logreq (func, logbuf);

  if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
    RETURN (serrno);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  if (Cns_get_usrinfo_by_uid (&thip->dbfd, requid, &user_entry, 1, &rec_addr) < 0) {
    if (serrno == ENOENT) {
      sendrep (thip->s, MSG_ERR, "No such uid\n");
      RETURN (EINVAL);
    } else {
      RETURN (serrno);
    }
  }
  strcpy (user_entry.username, username);
  if (Cns_update_user_entry (&thip->dbfd, &rec_addr, &user_entry))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_rmgrpmap - suppress group entry corresponding to a given gid/name */

int Cns_srv_rmgrpmap(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char func[18];
  gid_t gid;
  struct Cns_groupinfo group_entry;
  char groupname[256];
  char logbuf[LOGBUFSZ];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  gid_t reqgid;
  uid_t uid;
  char *user;

  strcpy (func, "Cns_srv_rmgrpmap");
  rbp = req_data;
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "rmgrpmap", user, uid, gid, clienthost);
  unmarshall_LONG (rbp, reqgid);
  if (unmarshall_STRINGN (rbp, groupname, sizeof(groupname)))
    RETURN (EINVAL);
  sprintf (logbuf, "rmgrpmap %d %s", reqgid, groupname);
  Cns_logreq (func, logbuf);

  if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
    RETURN (serrno);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  if (reqgid > 0) {
    if (Cns_get_grpinfo_by_gid (&thip->dbfd, reqgid, &group_entry,
                                1, &rec_addr) < 0) {
      if (serrno == ENOENT) {
        sendrep (thip->s, MSG_ERR, "No such gid\n");
        RETURN (EINVAL);
      } else {
        RETURN (serrno);
      }
    }
    if (*groupname && strcmp (groupname, group_entry.groupname))
      RETURN (EINVAL);
  } else {
    if (Cns_get_grpinfo_by_name (&thip->dbfd, groupname, &group_entry,
                                 1, &rec_addr) < 0) {
      if (serrno == ENOENT) {
        sendrep (thip->s, MSG_ERR, "No such group\n");
        RETURN (EINVAL);
      } else {
        RETURN (serrno);
      }
    }
  }
  if (Cns_delete_group_entry (&thip->dbfd, &rec_addr))
    RETURN (serrno);
  RETURN (0);
}

/* Cns_srv_rmusrmap - suppress user entry corresponding to a given uid/name */

int Cns_srv_rmusrmap(magic, req_data, clienthost, thip)
     int magic;
     char *req_data;
     const char *clienthost;
     struct Cns_srv_thread_info *thip;
{
  char func[18];
  gid_t gid;
  char logbuf[LOGBUFSZ];
  char *rbp;
  Cns_dbrec_addr rec_addr;
  gid_t requid;
  uid_t uid;
  char *user;
  struct Cns_userinfo user_entry;
  char username[256];

  strcpy (func, "Cns_srv_rmusrmap");
  rbp = req_data;
  get_client_actual_id (thip, &uid, &gid, &user);
  nslogit (func, NS092, "rmusrmap", user, uid, gid, clienthost);
  unmarshall_LONG (rbp, requid);
  if (unmarshall_STRINGN (rbp, username, sizeof(username)))
    RETURN (EINVAL);
  sprintf (logbuf, "rmusrmap %d %s", requid, username);
  Cns_logreq (func, logbuf);

  if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
    RETURN (serrno);

  /* start transaction */

  (void) Cns_start_tr (thip->s, &thip->dbfd);

  if (requid > 0) {
    if (Cns_get_usrinfo_by_uid (&thip->dbfd, requid, &user_entry,
                                1, &rec_addr) < 0) {
      if (serrno == ENOENT) {
        sendrep (thip->s, MSG_ERR, "No such uid\n");
        RETURN (EINVAL);
      } else {
        RETURN (serrno);
      }
    }
    if (*username && strcmp (username, user_entry.username))
      RETURN (EINVAL);
  } else {
    if (Cns_get_usrinfo_by_name (&thip->dbfd, username, &user_entry,
                                 1, &rec_addr) < 0) {
      if (serrno == ENOENT) {
        sendrep (thip->s, MSG_ERR, "No such user\n");
        RETURN (EINVAL);
      } else {
        RETURN (serrno);
      }
    }
  }
  if (Cns_delete_user_entry (&thip->dbfd, &rec_addr))
    RETURN (serrno);
  RETURN (0);
}
