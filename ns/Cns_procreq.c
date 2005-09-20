/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)Cns_procreq.c,v 1.61 2004/03/03 08:51:31 CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */
 
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
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
#include "marshall.h"
#include "Cgrp.h"
#include "Cns.h"
#include "Cns_server.h"
#include "Cpwd.h"
#include "Cupv_api.h"
#include "rfcntl.h"
#include "serrno.h"
#include "u64subr.h"
extern int being_shutdown;
extern char localhost[CA_MAXHOSTNAMELEN+1];

#define RESETID(UID,GID) resetid(&UID, &GID, thip);
 
void resetid(uid_t *u, gid_t *g, struct Cns_srv_thread_info *thip) {
#ifdef CSEC
  if (thip->Csec_service_type < 0) {
    *u = thip->Csec_uid;
    *g = thip->Csec_gid;
  }
#endif
}

/*	Cns_logreq - log a request */

/*	Split the message into lines so they don't exceed LOGBUFSZ-1 characters
 *	A backslash is appended to a line to be continued
 *	A continuation line is prefixed by '+ '
 */
void
Cns_logreq(func, logbuf)
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

marshall_DIRX (sbpp, fmd_entry)
char **sbpp;
struct Cns_file_metadata *fmd_entry;
{
	char *sbp = *sbpp;

	marshall_HYPER (sbp, fmd_entry->fileid);
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
	marshall_STRING (sbp, fmd_entry->name);
	*sbpp = sbp;
	return (0);
}

marshall_DIRXT (sbpp, magic, fmd_entry, smd_entry)
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

/*	Cns_srv_access - check accessibility of a file/directory */

Cns_srv_access(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	int amode;
	char basename[CA_MAXNAMELEN+1];
	u_signed64 cwd;
	struct Cns_file_metadata fmd_entry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+13];
	mode_t mode;
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	uid_t uid;

	strcpy (func, "Cns_srv_access");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "access", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	unmarshall_LONG (rbp, amode);
	sprintf (logbuf, "access %o %s", amode, path);
	Cns_logreq (func, logbuf);

	RESETID(uid, gid);

	if (amode & ~(R_OK | W_OK | X_OK | F_OK))
		RETURN (EINVAL);

	if (Cns_splitname (cwd, path, basename))
		RETURN (serrno);

	if (*basename == '/') {	/* Cns_access / */
		parent_dir.fileid = 0;
	} else { /* check parent directory components for search permission */

		if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IEXEC, uid, gid,
		    clienthost, &parent_dir, NULL))
			RETURN (serrno);
	}

	/* get basename entry */

	if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
	    &fmd_entry, 0, NULL))
		RETURN (serrno);

	/* check permissions for basename */

	if (amode == F_OK)
		RETURN (0);
	mode = (amode & (R_OK|W_OK|X_OK)) << 6;
	if (Cns_chkentryperm (&fmd_entry, mode, uid, gid, clienthost))
		RETURN (EACCES);
	RETURN (0);
}

/*      Cns_srv_chclass - change class on directory */

Cns_srv_chclass(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	char class_name[CA_MAXCLASNAMELEN+1];
	int classid;
	u_signed64 cwd;
	struct Cns_file_metadata fmd_entry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+CA_MAXCLASNAMELEN+16];
	struct Cns_class_metadata new_class_entry;
	Cns_dbrec_addr new_rec_addrc;
	struct Cns_class_metadata old_class_entry;
	Cns_dbrec_addr old_rec_addrc;
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addr;
	uid_t uid;

	strcpy (func, "Cns_srv_chclass");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "chclass", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	unmarshall_LONG (rbp, classid);
	if (unmarshall_STRINGN (rbp, class_name, CA_MAXCLASNAMELEN+1))
		RETURN (EINVAL);
	sprintf (logbuf, "chclass %s %d %s", path, classid, class_name);
	Cns_logreq (func, logbuf);

	RESETID(uid, gid);

	if (Cns_splitname (cwd, path, basename))
		RETURN (serrno);

	if (*basename == '/') {	/* Cns_chclass / */
		parent_dir.fileid = 0;
	} else { /* check parent directory components for search permission */

		if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IEXEC, uid, gid,
		    clienthost, &parent_dir, NULL))
			RETURN (serrno);
	}

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* is the class valid? */

	if (classid > 0) {
		if (Cns_get_class_by_id (&thip->dbfd, classid, &new_class_entry,
		    1, &new_rec_addrc))
			if (serrno == ENOENT) {
				sendrep (thip->s, MSG_ERR, "No such class\n");
				RETURN (EINVAL);
			} else
				RETURN (serrno);
		if (*class_name && strcmp (class_name, new_class_entry.name))
			RETURN (EINVAL);
	} else {
		if (Cns_get_class_by_name (&thip->dbfd, class_name, &new_class_entry,
		    1, &new_rec_addrc))
			if (serrno == ENOENT) {
				sendrep (thip->s, MSG_ERR, "No such class\n");
				RETURN (EINVAL);
			} else
				RETURN (serrno);
	}

	/* get/lock basename entry */

	if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
	    &fmd_entry, 1, &rec_addr))
		RETURN (serrno);

	/* check if the user is authorized to chclass this entry */

	if (uid != fmd_entry.uid &&
	    Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (EPERM);
	if ((fmd_entry.filemode & S_IFDIR) == 0)
		RETURN (ENOTDIR);

	/* update entries */

	if (fmd_entry.fileclass != new_class_entry.classid) {
		if (fmd_entry.fileclass > 0) {
			if (Cns_get_class_by_id (&thip->dbfd, fmd_entry.fileclass,
			    &old_class_entry, 1, &old_rec_addrc))
				RETURN (serrno);
			old_class_entry.nbdirs_using_class--;
			if (Cns_update_class_entry (&thip->dbfd, &old_rec_addrc,
			    &old_class_entry))
				RETURN (serrno);
		}
		fmd_entry.fileclass = new_class_entry.classid;
		fmd_entry.ctime = time (0);
		if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &fmd_entry))
			RETURN (serrno);
		new_class_entry.nbdirs_using_class++;
		if (Cns_update_class_entry (&thip->dbfd, &new_rec_addrc,
		    &new_class_entry))
			RETURN (serrno);
	}
	RETURN (0);
}

/*      Cns_srv_chdir - change current working directory */

Cns_srv_chdir(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	u_signed64 cwd;
	struct Cns_file_metadata direntry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+12];
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	char repbuf[8];
	char *sbp;
	uid_t uid;

	strcpy (func, "Cns_srv_chdir");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "chdir", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	sprintf (logbuf, "chdir %s", path);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	/* check directory components for search permission */

	if (strcmp (path, "..") == 0) {
		if (Cns_get_fmd_by_fileid (&thip->dbfd, cwd, &direntry, 0, NULL))
			RETURN (serrno);
		if (direntry.parent_fileid) {
			if (Cns_get_fmd_by_fileid (&thip->dbfd,
			    direntry.parent_fileid, &direntry, 0, NULL))
				RETURN (serrno);
			if (Cns_chkentryperm (&direntry, S_IEXEC, uid, gid, clienthost))
				RETURN (EACCES);
		}
	} else if (strcmp (path, ".") == 0) {
		if (Cns_get_fmd_by_fileid (&thip->dbfd, cwd, &direntry, 0, NULL))
			RETURN (serrno);
	} else {
		if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IEXEC, uid, gid,
		    clienthost, &direntry, NULL))
			RETURN (serrno);
	}

	/* return directory fileid */

	sbp = repbuf;
	marshall_HYPER (sbp, direntry.fileid);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURN (0);
}

/*      Cns_srv_chmod - change file/directory permissions */

Cns_srv_chmod(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	u_signed64 cwd;
	struct Cns_file_metadata fmd_entry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+12];
	mode_t mode;
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addr;
	uid_t uid;

	strcpy (func, "Cns_srv_chmod");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "chmod", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	unmarshall_LONG (rbp, mode);
	sprintf (logbuf, "chmod %o %s", mode, path);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	if (Cns_splitname (cwd, path, basename))
		RETURN (serrno);

	if (*basename == '/') {	/* Cns_chmod / */
		parent_dir.fileid = 0;
	} else { /* check parent directory components for search permission */

		if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IEXEC, uid, gid,
		    clienthost, &parent_dir, NULL))
			RETURN (serrno);
	}

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* get/lock basename entry */

	if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
	    &fmd_entry, 1, &rec_addr))
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
	fmd_entry.ctime = time (0);
	if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &fmd_entry))
		RETURN (serrno);
	RETURN (0);
}

/*      Cns_srv_chown - change owner and group of a file or a directory */

Cns_srv_chown(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	u_signed64 cwd;
	struct Cns_file_metadata fmd_entry;
	int found;
	char func[16];
	gid_t gid;
	struct group *gr;
	char logbuf[CA_MAXPATHLEN+19];
	char **membername;
	int need_p_admin = 0;
	int need_p_expt_admin = 0;
	gid_t new_gid;
	uid_t new_uid;
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	struct passwd *pw;
	char *rbp;
	Cns_dbrec_addr rec_addr;
	uid_t uid;

	strcpy (func, "Cns_srv_chown");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "chown", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	unmarshall_LONG (rbp, new_uid);
	unmarshall_LONG (rbp, new_gid);
	sprintf (logbuf, "chown %d:%d %s", new_uid, new_gid, path);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	if (Cns_splitname (cwd, path, basename))
		RETURN (serrno);

	if (*basename == '/') {	/* Cns_chown / */
		parent_dir.fileid = 0;
	} else { /* check parent directory components for search permission */

		if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IEXEC, uid, gid,
		    clienthost, &parent_dir, NULL))
			RETURN (serrno);
	}

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* get/lock basename entry */

	if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
	    &fmd_entry, 1, &rec_addr))
		RETURN (serrno);

	/* check if the user is authorized to change ownership this entry */

	if (fmd_entry.uid != new_uid && new_uid != -1) {
		if (gid != fmd_entry.gid)
			need_p_admin = 1;
		else if ((pw = Cgetpwuid (new_uid)) == NULL)
			need_p_admin = 1;
		else if (pw->pw_gid == gid)	/* new owner belongs to same group */
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
				if (membername = gr->gr_mem) {
					while (*membername) {
						if (strcmp (pw->pw_name, *membername) == 0) {
							found = 1;
							break;
						}
						membername++;
					}
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
	fmd_entry.ctime = time (0);
	if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &fmd_entry))
		RETURN (serrno);
	RETURN (0);
}

/*      Cns_srv_creat - create a file entry */
 
Cns_srv_creat(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	int bof = 1;
	int c;
	u_signed64 cwd;
	DBLISTPTR dblistptr;
	struct Cns_file_metadata filentry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+17];
	mode_t mask;
	mode_t mode;
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addr;	/* file record address */
	Cns_dbrec_addr rec_addrp;	/* parent record address */
	Cns_dbrec_addr rec_addrs;	/* segment record address */
	char repbuf[8];
	char *sbp;
	struct Cns_seg_metadata smd_entry;
	char tmpbuf[21];
	uid_t uid;

	strcpy (func, "Cns_srv_creat");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "creat", uid, gid, clienthost);
	unmarshall_WORD (rbp, mask);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	unmarshall_LONG (rbp, mode);
	sprintf (logbuf, "creat %s %o %o", path, mode, mask);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	if (Cns_splitname (cwd, path, basename))
		RETURN (serrno);

	if (*basename == '/')	/* Cns_creat / */
		RETURN (EISDIR);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* check parent directory components for write/search permission */

	if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IWRITE|S_IEXEC, uid, gid,
	    clienthost, &parent_dir, &rec_addrp))
		RETURN (serrno);

	if (strcmp (basename, ".") == 0 || strcmp (basename, "..") == 0)
		RETURN (EISDIR);

	/* check if the file exists already */

	if ((c = Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
	    &filentry, 1, &rec_addr)) && serrno != ENOENT)
		RETURN (serrno);

	if (c == 0) {	/* file exists */
		if (filentry.filemode & S_IFDIR)
			RETURN (EISDIR);

		/* check write permission in basename entry */

		if (Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost))
			RETURN (EACCES);

		/* delete file segments if any */

		while ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid,
		    &smd_entry, 1, &rec_addrs, 0, &dblistptr)) == 0) {
			if (Cns_delete_smd_entry (&thip->dbfd, &rec_addrs))
				RETURN (serrno);
			bof = 0;
		}
		(void) Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid,
		    &smd_entry, 1, &rec_addrs, 1, &dblistptr);	/* free res */
		if (c < 0)
			RETURN (serrno);

		/* update basename entry */

		filentry.filesize = 0;
		filentry.mtime = time (0);
		filentry.ctime = filentry.mtime;
		filentry.status = '-';
		if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
			RETURN (serrno);
		nslogit (func, "file %s reset\n", u64tostr (filentry.fileid, tmpbuf, 0));
	} else {	/* must create the file */
		if (parent_dir.fileclass <= 0)
			RETURN (EINVAL);
		memset ((char *) &filentry, 0, sizeof(filentry));
		if (Cns_unique_id (&thip->dbfd, &filentry.fileid) < 0)
			RETURN (serrno);
		filentry.parent_fileid = parent_dir.fileid;
		strcpy (filentry.name, basename);
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

/*	Cns_srv_delcomment - delete a comment associated with a file/directory */

Cns_srv_delcomment(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	u_signed64 cwd;
	struct Cns_file_metadata filentry;
	char func[19];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+12];
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addru;
	uid_t uid;
	struct Cns_user_metadata umd_entry;

	strcpy (func, "Cns_srv_delcomment");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "delcomment", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	sprintf (logbuf, "delcomment %s", path);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	if (Cns_splitname (cwd, path, basename))
		RETURN (serrno);

	/* check parent directory components for search permission */

	if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IEXEC, uid, gid,
	    clienthost, &parent_dir, NULL))
		RETURN (serrno);

	/* get basename entry */

	if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
	    &filentry, 0, NULL))
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
 
Cns_srv_delete(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	int bof = 1;
	int c;
	u_signed64 cwd;
	DBLISTPTR dblistptr;
	struct Cns_file_metadata filentry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+8];
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addr;	/* file record address */
	Cns_dbrec_addr rec_addrp;	/* parent record address */
	Cns_dbrec_addr rec_addrs;	/* segment record address */
	struct Cns_seg_metadata smd_entry;
	uid_t uid;

	strcpy (func, "Cns_srv_delete");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "delete", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	sprintf (logbuf, "delete %s", path);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	if (Cns_splitname (cwd, path, basename))
		RETURN (serrno);

	if (*basename == '/')	/* Cns_delete / */
		RETURN (EINVAL);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* check parent directory components for write/search permission */

	if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IWRITE|S_IEXEC, uid, gid,
	    clienthost, &parent_dir, &rec_addrp))
		RETURN (serrno);

	/* get and lock requested file entry */

	if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
	    &filentry, 1, &rec_addr))
		RETURN (serrno);

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

	while ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid,
	    &smd_entry, 1, &rec_addrs, 0, &dblistptr)) == 0) {
		smd_entry.s_status = 'D';
		if (Cns_update_smd_entry (&thip->dbfd, &rec_addrs, &smd_entry))
			RETURN (serrno);
		bof = 0;
	}
	(void) Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid,
	    &smd_entry, 1, &rec_addrs, 1, &dblistptr);	/* free res */
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

/*	Cns_srv_deleteclass - delete a file class definition */

Cns_srv_deleteclass(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	int bol = 1;
	struct Cns_class_metadata class_entry;
	char class_name[CA_MAXCLASNAMELEN+1];
	int classid;
	DBLISTPTR dblistptr;
	char func[20];
	gid_t gid;
	char logbuf[CA_MAXCLASNAMELEN+19];
	char *rbp;
	Cns_dbrec_addr rec_addr;
	Cns_dbrec_addr rec_addrt;
	struct Cns_tp_pool tppool_entry;
	uid_t uid;

	strcpy (func, "Cns_srv_deleteclass");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "deleteclass", uid, gid, clienthost);
	unmarshall_LONG (rbp, classid);
	if (unmarshall_STRINGN (rbp, class_name, CA_MAXCLASNAMELEN+1))
		RETURN (EINVAL);
	sprintf (logbuf, "deleteclass %d %s", classid, class_name);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
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
	if (class_entry.nbdirs_using_class)
		RETURN (EEXIST);
	while (Cns_get_tppool_by_cid (&thip->dbfd, bol, class_entry.classid,
	    &tppool_entry, 1, &rec_addrt, 0, &dblistptr) == 0) {
		if (Cns_delete_tppool_entry (&thip->dbfd, &rec_addrt))
			RETURN (serrno);
		bol = 0;
	}
	(void) Cns_get_tppool_by_cid (&thip->dbfd, bol, class_entry.classid,
	    &tppool_entry, 1, &rec_addrt, 1, &dblistptr);	/* free res */
	if (Cns_delete_class_entry (&thip->dbfd, &rec_addr))
		RETURN (serrno);
	RETURN (0);
}

/*	Cns_srv_enterclass - define a new file class */

Cns_srv_enterclass(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	struct Cns_class_metadata class_entry;
	char func[19];
	gid_t gid;
	int i;
	char logbuf[CA_MAXCLASNAMELEN+19];
	int nbtppools;
	char *rbp;
	struct Cns_tp_pool tppool_entry;
	uid_t uid;

	strcpy (func, "Cns_srv_enterclass");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "enterclass", uid, gid, clienthost);
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
	RESETID(uid, gid);
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

/*	Cns_srv_getcomment - get the comment associated with a file/directory */

Cns_srv_getcomment(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	u_signed64 cwd;
	struct Cns_file_metadata filentry;
	char func[19];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+12];
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	char repbuf[CA_MAXCOMMENTLEN+1];
	char *sbp;
	uid_t uid;
	struct Cns_user_metadata umd_entry;

	strcpy (func, "Cns_srv_getcomment");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "getcomment", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	sprintf (logbuf, "getcomment %s", path);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	if (Cns_splitname (cwd, path, basename))
		RETURN (serrno);

	/* check parent directory components for search permission */

	if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IEXEC, uid, gid,
	    clienthost, &parent_dir, NULL))
		RETURN (serrno);

	/* get basename entry */

	if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
	    &filentry, 0, NULL))
		RETURN (serrno);

	/* check if the user is authorized to get the comment for this entry */

	if (uid != filentry.uid &&
	    Cns_chkentryperm (&filentry, S_IREAD, uid, gid, clienthost))
		RETURN (EACCES);

	/* get the comment if it exists */

	if (Cns_get_umd_by_fileid (&thip->dbfd, filentry.fileid, &umd_entry, 0,
	    NULL))
		RETURN (serrno);

	sbp = repbuf;
	marshall_STRING (sbp, umd_entry.comments);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURN (0);
}

Cns_srv_getpath(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	u_signed64 cur_fileid;
	struct Cns_file_metadata fmd_entry;
	char func[16];
	gid_t gid;
	int n;
	char *p;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	char repbuf[CA_MAXPATHLEN+1];
	char *sbp;
	uid_t uid;

	strcpy (func, "Cns_srv_getpath");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "getpath", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cur_fileid);
	RESETID(uid, gid);
	p = path + CA_MAXPATHLEN;
	*p = '\0';
	if (cur_fileid == 2)
		*(--p) = '/';
	else while (cur_fileid != 2) {
		if (Cns_get_fmd_by_fileid (&thip->dbfd, cur_fileid, &fmd_entry,
		    0, NULL))
			RETURN (serrno);
		n = strlen (fmd_entry.name);
		if ((p -= n) < path + 1)
			RETURN (SENAMETOOLONG);
		memcpy (p, fmd_entry.name, n);
		*(--p) = '/';
		cur_fileid = fmd_entry.parent_fileid;
	}
	sbp = repbuf;
	marshall_STRING (sbp, p);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURN (0);
}

/*	Cns_srv_getsegattrs - get file segments attributes */

Cns_srv_getsegattrs(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	int bof = 1;
	int c;
	int copyno;
	u_signed64 cwd;
	DBLISTPTR dblistptr;
	u_signed64 fileid;
	struct Cns_file_metadata filentry;
	int fsec;
	char func[20];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+34];
	int nbseg = 0;
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *q;
	char *rbp;
	char repbuf[REPBUFSZ];
	char *sbp;
	struct Cns_seg_metadata smd_entry;
	char tmpbuf[21];
	uid_t uid;

	strcpy (func, "Cns_srv_getsegattrs");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "getsegattrs", uid, gid, clienthost);
        unmarshall_HYPER (rbp, cwd);
        unmarshall_HYPER (rbp, fileid);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	sprintf (logbuf, "getsegattrs %s %s",
	    u64tostr (fileid, tmpbuf, 0), path);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
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
		if (Cns_splitname (cwd, path, basename))
			RETURN (serrno);

		/* check parent directory components for search permission */

		if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IEXEC, uid, gid,
		    clienthost, &parent_dir, NULL))
			RETURN (serrno);

		/* get basename entry */

		if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
		    &filentry, 0, NULL))
			RETURN (serrno);
	}

	/* check if the entry is a regular file */

	if (filentry.filemode & S_IFDIR)
		RETURN (EISDIR);

	/* get/send file segment entries */

	sbp = repbuf;
	marshall_WORD (sbp, nbseg);	/* will be updated */
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
	    &smd_entry, 0, NULL, 1, &dblistptr);	/* free res */
	if (c < 0)
		RETURN (serrno);

	q = repbuf;
	marshall_WORD (q, nbseg);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURN (0);
}

Cns_srv_listclass(magic, req_data, clienthost, thip, class_entry, endlist, dblistptr)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
struct Cns_class_metadata *class_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	int bol;	/* beginning of class list flag */
	int bot;	/* beginning of tape pools list flag */
	int c;
	int eol = 0;	/* end of list flag */
	char func[18];
	gid_t gid;
	int listentsz;	/* size of client machine Cns_fileclass structure */
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

	strcpy (func, "Cns_srv_listclass");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "listclass", uid, gid, clienthost);
	unmarshall_WORD (rbp, listentsz);
	unmarshall_WORD (rbp, bol);
	RESETID(uid, gid);
	/* return as many entries as possible to the client */

	maxsize = LISTBUFSZ;
	sbp = outbuf;
	marshall_WORD (sbp, nbentries);		/* will be updated */

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
		marshall_LONG (sbp, nbtppools);	/* will be updated */
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
		    &tppool_entry, 0, NULL, 1, &tplistptr);	/* free res */
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
	marshall_WORD (p, nbentries);		/* update nbentries in reply */
	sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
	RETURN (0);
}

Cns_srv_listtape(magic, req_data, clienthost, thip, fmd_entry, smd_entry, endlist, dblistptr)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
struct Cns_file_metadata *fmd_entry;
struct Cns_seg_metadata *smd_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	int bov;	/* beginning of volume flag */
	int c;
	char dirbuf[DIRBUFSZ+4];
	int direntsz;	/* size of client machine dirent structure excluding d_name */
	int eov = 0;	/* end of volume flag */
	char func[17];
	gid_t gid;
	char logbuf[CA_MAXVIDLEN+12];
	int maxsize;
	int nbentries = 0;
	char *p;
	char *rbp;
	char *sbp;
	uid_t uid;
	char vid[CA_MAXVIDLEN+1];

	strcpy (func, "Cns_srv_listtape");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "listtape", uid, gid, clienthost);
	unmarshall_WORD (rbp, direntsz);
	if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1))
		RETURN (EINVAL);
	unmarshall_WORD (rbp, bov);
	sprintf (logbuf, "listtape %s %d", vid, bov);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	/* return as many entries as possible to the client */

	maxsize = DIRBUFSZ - direntsz;
	sbp = dirbuf;
	marshall_WORD (sbp, nbentries);		/* will be updated */

	if (! bov && ! endlist) {
		marshall_DIRXT (&sbp, magic, fmd_entry, smd_entry);
		nbentries++;
		maxsize -= ((direntsz + strlen (fmd_entry->name) + 8) / 8) * 8;
	}
	while ((c = Cns_get_smd_by_vid (&thip->dbfd, bov, vid, smd_entry,
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
	marshall_WORD (p, nbentries);		/* update nbentries in reply */
	sendrep (thip->s, MSG_DATA, sbp - dirbuf, dirbuf);
	RETURN (0);
}

/*      Cns_srv_mkdir - create a directory entry */
 
Cns_srv_mkdir(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	int c;
	struct Cns_class_metadata class_entry;
	u_signed64 cwd;
	struct Cns_file_metadata direntry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+17];
	mode_t mask;
	mode_t mode;
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addrc;
	Cns_dbrec_addr rec_addrp;
	uid_t uid;

	strcpy (func, "Cns_srv_mkdir");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "mkdir", uid, gid, clienthost);
	unmarshall_WORD (rbp, mask);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	unmarshall_LONG (rbp, mode);
	sprintf (logbuf, "mkdir %s %o %o", path, mode, mask);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	if (Cns_splitname (cwd, path, basename))
		RETURN (serrno);

	if (*basename == '/')	/* Cns_mkdir / */
		RETURN (EEXIST);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* check parent directory components for write/search permission */

	if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IWRITE|S_IEXEC, uid, gid,
	    clienthost, &parent_dir, &rec_addrp))
		RETURN (serrno);

	if (strcmp (basename, ".") == 0 || strcmp (basename, "..") == 0)
		RETURN (EEXIST);

	/* check if basename entry exists already */

	if ((c = Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid,
	    basename, &direntry, 0, NULL)) && serrno != ENOENT)
		RETURN (serrno);
	if (c == 0)
		RETURN (EEXIST);

	/* build new directory entry */

	memset ((char *) &direntry, 0, sizeof(direntry));
	if (Cns_unique_id (&thip->dbfd, &direntry.fileid) < 0)
		RETURN (serrno);
	direntry.parent_fileid = parent_dir.fileid;
	strcpy (direntry.name, basename);
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

	/* write new directory entry */

	if (Cns_insert_fmd_entry (&thip->dbfd, &direntry))
		RETURN (serrno);

	/* update parent directory entry */

	parent_dir.nlink++;
	parent_dir.mtime = time (0);
	parent_dir.ctime = parent_dir.mtime;
	if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
		RETURN (serrno);

	/* update nbdirs_using_class in Cns_class_metadata */

	if (direntry.fileclass > 0) {
		if (Cns_get_class_by_id (&thip->dbfd, direntry.fileclass,
		    &class_entry, 1, &rec_addrc))
			RETURN (serrno);
		class_entry.nbdirs_using_class++;
		if (Cns_update_class_entry (&thip->dbfd, &rec_addrc, &class_entry))
			RETURN (serrno);
	}
	RETURN (0);
}

/*	Cns_srv_modifyclass - modify an existing fileclass definition */

Cns_srv_modifyclass(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
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
	char logbuf[CA_MAXCLASNAMELEN+19];
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

	strcpy (func, "Cns_srv_modifyclass");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "modifyclass", uid, gid, clienthost);
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
	RESETID(uid, gid);
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
		    &tppool_entry, 1, &rec_addrt, 1, &dblistptr);	/* free res */

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

/*      Cns_srv_open - open a file */
 
Cns_srv_open(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	int bof = 1;
	int c;
	u_signed64 cwd;
	DBLISTPTR dblistptr;
	struct Cns_file_metadata filentry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+21];
	mode_t mask;
	mode_t mode;
	int oflag;
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addr;	/* file record address */
	Cns_dbrec_addr rec_addrp;	/* parent record address */
	Cns_dbrec_addr rec_addrs;	/* segment record address */
	char repbuf[8];
	char *sbp;
	struct Cns_seg_metadata smd_entry;
	uid_t uid;

	strcpy (func, "Cns_srv_open");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "open", uid, gid, clienthost);
	unmarshall_WORD (rbp, mask);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	unmarshall_LONG (rbp, oflag);
	oflag = ntohopnflg (oflag);
	unmarshall_LONG (rbp, mode);
	sprintf (logbuf, "open %s %o %o %o", path, oflag, mode, mask);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	if (Cns_splitname (cwd, path, basename))
		RETURN (serrno);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	if (*basename == '/') {	/* Cns_open / */
		parent_dir.fileid = 0;
	} else { /* check parent directory components for (write)/search permission */

		if (Cns_chkdirperm (&thip->dbfd, cwd, path,
		    (oflag & O_CREAT) ? S_IWRITE|S_IEXEC : S_IEXEC, uid, gid,
		    clienthost, &parent_dir, &rec_addrp))
			RETURN (serrno);
	}

	/* check if the file exists already */

	if ((c = Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
	    &filentry, (oflag & O_TRUNC) ? 1 : 0, &rec_addr)) && serrno != ENOENT)
		RETURN (serrno);

	if (c && (oflag & O_CREAT) == 0)
		RETURN (ENOENT);

	if (c == 0) {	/* file exists */
		if (oflag & O_CREAT && oflag & O_EXCL)
			RETURN (EEXIST);
		if (filentry.filemode & S_IFDIR &&
		    (oflag & O_WRONLY || oflag & O_RDWR || oflag & O_TRUNC))
			RETURN (EISDIR);

		/* check permissions in basename entry */

		if (Cns_chkentryperm (&filentry,
		    (oflag & O_WRONLY || oflag & O_RDWR || oflag & O_TRUNC) ? S_IWRITE : S_IREAD,
		    uid, gid, clienthost))
			RETURN (EACCES);

		if (oflag & O_TRUNC) {

			/* delete file segments if any */

			while ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof,
			    filentry.fileid, &smd_entry, 1, &rec_addrs,
			    0, &dblistptr)) == 0) {
				if (Cns_delete_smd_entry (&thip->dbfd, &rec_addrs))
					RETURN (serrno);
				bof = 0;
			}
			(void) Cns_get_smd_by_pfid (&thip->dbfd, bof,
			    filentry.fileid, &smd_entry, 1, &rec_addrs,
			    1, &dblistptr);	/* free res */
			if (c < 0)
				RETURN (serrno);

			/* update basename entry */

			filentry.filesize = 0;
			filentry.mtime = time (0);
			filentry.ctime = filentry.mtime;
			filentry.status = '-';
			if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
				RETURN (serrno);
		}
	} else {	/* must create the file */
		memset ((char *) &filentry, 0, sizeof(filentry));
		if (Cns_unique_id (&thip->dbfd, &filentry.fileid) < 0)
			RETURN (serrno);
		filentry.parent_fileid = parent_dir.fileid;
		strcpy (filentry.name, basename);
		filentry.filemode = S_IFREG | ((mode & ~S_IFMT) & ~mask);
		filentry.filemode &= ~S_ISVTX;
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

		/* write new file entry */

		if (Cns_insert_fmd_entry (&thip->dbfd, &filentry))
			RETURN (serrno);

		/* update parent directory entry */

		parent_dir.nlink++;
		parent_dir.mtime = time (0);
		parent_dir.ctime = parent_dir.mtime;
		if (Cns_update_fmd_entry (&thip->dbfd, &rec_addrp, &parent_dir))
			RETURN (serrno);
	}

	/* return fileid */

	sbp = repbuf;
	marshall_HYPER (sbp, filentry.fileid);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURN (0);
}

/*      Cns_srv_opendir - open a directory entry */

Cns_srv_opendir(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	u_signed64 cwd;
	struct Cns_file_metadata direntry;
	char func[16];
	char logbuf[CA_MAXPATHLEN+9];
	gid_t gid;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	char repbuf[8];
	char *sbp;
	uid_t uid;

	strcpy (func, "Cns_srv_opendir");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "opendir", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	sprintf (logbuf, "opendir %s", path);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	if (! cwd && *path == 0)
		RETURN (ENOENT);
	if (! cwd && *path != '/')
		RETURN (EINVAL);

	if (strcmp (path, ".") == 0) {
		if (Cns_get_fmd_by_fileid (&thip->dbfd, cwd, &direntry, 0, NULL))
			RETURN (serrno);
	} else {
		/* check parent directory components for search permission and
		   check directory basename for read permission */

		if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IREAD|S_IEXEC,
		    uid, gid, clienthost, &direntry, NULL))
			RETURN (serrno);
	}

	/* return directory fileid */

	sbp = repbuf;
	marshall_HYPER (sbp, direntry.fileid);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURN (0);
}

/*	Cns_srv_queryclass - query about a file class */

Cns_srv_queryclass(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
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
	char logbuf[CA_MAXCLASNAMELEN+18];
	int nbtppools = 0;
	char *q;
	char *rbp;
	char repbuf[LISTBUFSZ];
	char *sbp;
	struct Cns_tp_pool tppool_entry;
	uid_t uid;

	strcpy (func, "Cns_srv_queryclass");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "queryclass", uid, gid, clienthost);
	unmarshall_LONG (rbp, classid);
	if (unmarshall_STRINGN (rbp, class_name, CA_MAXCLASNAMELEN+1))
		RETURN (EINVAL);
	sprintf (logbuf, "queryclass %d %s", classid, class_name);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
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
	marshall_LONG (sbp, nbtppools);	/* will be updated */
	while ((c = Cns_get_tppool_by_cid (&thip->dbfd, bol, class_entry.classid,
	    &tppool_entry, 0, NULL, 0, &dblistptr)) == 0) {
		marshall_STRING (sbp, tppool_entry.tape_pool);
		nbtppools++;
		bol = 0;
	}
	(void) Cns_get_tppool_by_cid (&thip->dbfd, bol, class_entry.classid,
	    &tppool_entry, 0, NULL, 1, &dblistptr);	/* free res */
	if (c < 0)
		RETURN (serrno);

	marshall_LONG (q, nbtppools);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURN (0);
}

/*      Cns_srv_readdir - read directory entries */

Cns_srv_readdir(magic, req_data, clienthost, thip, fmd_entry, smd_entry, umd_entry, endlist, dblistptr, smdlistptr)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
struct Cns_file_metadata *fmd_entry;
struct Cns_seg_metadata *smd_entry;
struct Cns_user_metadata *umd_entry;
int endlist;
DBLISTPTR *dblistptr;
DBLISTPTR *smdlistptr;
{
	int bod;	/* beginning of directory flag */
	int bof;	/* beginning of file flag */
	int c;
	int cml;	/* comment length */
	char dirbuf[DIRBUFSZ+4];
	struct Cns_file_metadata direntry;
	int direntsz;	/* size of client machine dirent structure excluding d_name */
	u_signed64 dir_fileid;
	int eod = 0;	/* end of directory flag */
	int fnl;	/* filename length */
	char func[16];
	int getattr;
	gid_t gid;
	int maxsize;
	int nbentries = 0;
	char *p;
	char *rbp;
	Cns_dbrec_addr rec_addr;
	char *sbp;
	uid_t uid;

	strcpy (func, "Cns_srv_readdir");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "readdir", uid, gid, clienthost);
	unmarshall_WORD (rbp, getattr);
	unmarshall_WORD (rbp, direntsz);
	unmarshall_HYPER (rbp, dir_fileid);
	unmarshall_WORD (rbp, bod);
	RESETID(uid, gid);
	/* return as many entries as possible to the client */

	if (getattr == 1 || getattr == 4)
		if (DIRXSIZE > direntsz)
			direntsz = DIRXSIZE;
	maxsize = DIRBUFSZ - direntsz;
	sbp = dirbuf;
	marshall_WORD (sbp, nbentries);		/* will be updated */

	if (endlist && getattr == 2)
		(void) Cns_get_smd_by_pfid (&thip->dbfd, 0, fmd_entry->fileid,
		    smd_entry, 0, NULL, 1, smdlistptr);
	if (! bod && ! endlist) {
		fnl = strlen (fmd_entry->name);
		if (getattr == 0) {		/* readdir */
			marshall_STRING (sbp, fmd_entry->name);
			nbentries++;
			maxsize -= ((direntsz + fnl + 8) / 8) * 8;
		} else if (getattr == 1) {	/* readdirx */
			marshall_DIRX (&sbp, fmd_entry);
			nbentries++;
			maxsize -= ((direntsz + fnl + 8) / 8) * 8;
		} else if (getattr == 2) {	/* readdirxt */
			bof = 0;
			while (1) {	/* loop on segments */
				marshall_DIRXT (&sbp, magic, fmd_entry, smd_entry);
				nbentries++;
				maxsize -= ((direntsz + fnl + 8) / 8) * 8;
				if (c = Cns_get_smd_by_pfid (&thip->dbfd, bof, 
				    fmd_entry->fileid, smd_entry, 0, NULL,
				    0, smdlistptr)) break;
				if (fnl > maxsize)
					goto reply;
			}
			(void) Cns_get_smd_by_pfid (&thip->dbfd, bof,
			    fmd_entry->fileid, smd_entry, 0, NULL, 1, smdlistptr);
			if (c < 0)
				RETURN (serrno);
		} else if (getattr == 3) {	/* readdirc */
			cml = strlen (umd_entry->comments);
			marshall_STRING (sbp, fmd_entry->name);
			marshall_STRING (sbp, umd_entry->comments);
			nbentries++;
			maxsize -= ((direntsz + fnl + cml + 9) / 8) * 8;
		} else {			/* readdirxc */
			cml = strlen (umd_entry->comments);
			marshall_DIRX (&sbp, fmd_entry);
			marshall_STRING (sbp, umd_entry->comments);
			nbentries++;
			maxsize -= ((direntsz + fnl + cml + 9) / 8) * 8;
		}
	}
	while ((c = Cns_get_fmd_by_pfid (&thip->dbfd, bod, dir_fileid,
	    fmd_entry, getattr, endlist, dblistptr)) == 0) {	/* loop on directory entries */
		fnl = strlen (fmd_entry->name);
		if (getattr == 0) {		/* readdir */
			if (fnl > maxsize) break;
			marshall_STRING (sbp, fmd_entry->name);
			nbentries++;
			maxsize -= ((direntsz + fnl + 8) / 8) * 8;
		} else if (getattr == 1) {	/* readdirx */
			if (fnl > maxsize) break;
			marshall_DIRX (&sbp, fmd_entry);
			nbentries++;
			maxsize -= ((direntsz + fnl + 8) / 8) * 8;
		} else if (getattr == 2) {	/* readdirxt */
			bof = 1;
			while (1) {	/* loop on segments */
				if (c = Cns_get_smd_by_pfid (&thip->dbfd, bof,
				    fmd_entry->fileid, smd_entry, 0, NULL,
				    0, smdlistptr)) break;
				if (fnl > maxsize)
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
		} else if (getattr == 3) {	/* readdirc */
			*umd_entry->comments = '\0';
			if (Cns_get_umd_by_fileid (&thip->dbfd, fmd_entry->fileid,
			    umd_entry, 0, NULL) && serrno != ENOENT)
				RETURN (serrno);
			cml = strlen (umd_entry->comments);
			if (fnl + cml > maxsize) break;
			marshall_STRING (sbp, fmd_entry->name);
			marshall_STRING (sbp, umd_entry->comments);
			nbentries++;
			maxsize -= ((direntsz + fnl + cml + 9) / 8) * 8;
		} else {			/* readdirxc */
			*umd_entry->comments = '\0';
			if (Cns_get_umd_by_fileid (&thip->dbfd, fmd_entry->fileid,
			    umd_entry, 0, NULL) && serrno != ENOENT)
				RETURN (serrno);
			cml = strlen (umd_entry->comments);
			if (fnl + cml > maxsize) break;
			marshall_DIRX (&sbp, fmd_entry);
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

		if (Cns_get_fmd_by_fileid (&thip->dbfd, dir_fileid, &direntry,
		    1, &rec_addr))
			RETURN (serrno);
		direntry.atime = time (0);
		if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &direntry))
			RETURN (serrno);
	}
reply:
	marshall_WORD (sbp, eod);
	p = dirbuf;
	marshall_WORD (p, nbentries);		/* update nbentries in reply */
	sendrep (thip->s, MSG_DATA, sbp - dirbuf, dirbuf);
	RETURN (0);
}

/*      Cns_srv_rename - rename a file or a directory */
 
Cns_srv_rename(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	int bof = 1;
	int c;
	u_signed64 cwd;
	DBLISTPTR dblistptr;
	char func[16];
	gid_t gid;
	char logbuf[2*CA_MAXPATHLEN+9];
	int n;
	char new_basename[CA_MAXNAMELEN+1];
	int new_exists = 0;
	struct Cns_file_metadata new_fmd_entry;
	struct Cns_file_metadata new_parent_dir;
	Cns_dbrec_addr new_rec_addr;
	Cns_dbrec_addr new_rec_addrp;
	char newpath[CA_MAXPATHLEN+1];
	char old_basename[CA_MAXNAMELEN+1];
	struct Cns_file_metadata old_fmd_entry;
	struct Cns_file_metadata old_parent_dir;
	Cns_dbrec_addr old_rec_addr;
	Cns_dbrec_addr old_rec_addrp;
	char oldpath[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addrs;	/* segment record address */
	Cns_dbrec_addr rec_addru;	/* comment record address */
	struct Cns_seg_metadata smd_entry;
	uid_t uid;
	struct Cns_user_metadata umd_entry;

	strcpy (func, "Cns_srv_rename");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "rename", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, oldpath, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	if (unmarshall_STRINGN (rbp, newpath, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	sprintf (logbuf, "rename %s %s", oldpath, newpath);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	if (strcmp (oldpath, newpath) == 0)
		RETURN (0);

	if (Cns_splitname (cwd, oldpath, old_basename))
		RETURN (serrno);
	if (Cns_splitname (cwd, newpath, new_basename))
		RETURN (serrno);

	if (*old_basename == '/' || *new_basename == '/')	/* nsrename / */
		RETURN (EINVAL);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* check parents directory components for write/search permission */

	if (Cns_chkdirperm (&thip->dbfd, cwd, oldpath, S_IWRITE|S_IEXEC, uid, gid,
	    clienthost, &old_parent_dir, &old_rec_addrp))
		RETURN (serrno);
	if (Cns_chkdirperm (&thip->dbfd, cwd, newpath, S_IWRITE|S_IEXEC, uid, gid,
	    clienthost, &new_parent_dir, &new_rec_addrp))
		RETURN (serrno);

	/* get and lock 'old' basename entry */

	if (Cns_get_fmd_by_fullid (&thip->dbfd, old_parent_dir.fileid,
	    old_basename, &old_fmd_entry, 1, &old_rec_addr))
		RETURN (serrno);

	/* if renaming a directory, 'new' must not be a descendant of 'old' */

	if (old_fmd_entry.filemode & S_IFDIR) {
		oldpath[strlen (oldpath)] = '/';
		newpath[strlen (newpath)] = '/';
		if (strlen (newpath) > (n = strlen (oldpath)) &&
		    strncmp (oldpath, newpath, n) == 0 &&
		    newpath[n] == '/')
			RETURN (EINVAL);
	}

	/* check if 'new' basename entry exists already */

	if ((c = Cns_get_fmd_by_fullid (&thip->dbfd, new_parent_dir.fileid,
	    new_basename, &new_fmd_entry, 1, &new_rec_addr)) && serrno != ENOENT)
		RETURN (serrno);

	if (c == 0) {	/* 'new' basename entry exists already */
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

	if (new_exists) {	/* must remove it */
		/* delete file segments if any */

		while ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof,
		    new_fmd_entry.fileid, &smd_entry, 1, &rec_addrs, 0, &dblistptr)) == 0) {
			if (Cns_delete_smd_entry (&thip->dbfd, &rec_addrs))
				RETURN (serrno);
			bof = 0;
		}
		(void) Cns_get_smd_by_pfid (&thip->dbfd, bof, new_fmd_entry.fileid,
		    &smd_entry, 1, &rec_addrs, 1, &dblistptr);	/* free res */
		if (c < 0)
			RETURN (serrno);

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
	if (old_parent_dir.fileid != new_parent_dir.fileid) {

		/* update 'old' parent directory entry */

		old_parent_dir.nlink--;

		/* update 'new' parent directory entry */

		new_parent_dir.nlink++;
		new_parent_dir.mtime = time (0);
		new_parent_dir.ctime = new_parent_dir.mtime;
		if (Cns_update_fmd_entry (&thip->dbfd, &new_rec_addrp, &new_parent_dir))
			RETURN (serrno);
	}

	/* update 'old' basename entry */

	old_fmd_entry.parent_fileid = new_parent_dir.fileid;
	strcpy (old_fmd_entry.name, new_basename);
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

/*	Cns_srv_updateseg_checksum - Updates file segment checksum
    when previous value is NULL*/

Cns_srv_updateseg_checksum(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	int copyno;
	u_signed64 fileid;
	struct Cns_file_metadata filentry;
	int fsec;
	int fseq;
	char func[19];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+34];
	struct Cns_seg_metadata old_smd_entry;
	struct Cns_file_metadata parent_dir;
	char *rbp;
	Cns_dbrec_addr rec_addr;
	Cns_dbrec_addr rec_addrs;
	u_signed64 segsize;
	int side;
	struct Cns_seg_metadata smd_entry;
	char tmpbuf[21];
	char tmpbuf2[21];
	uid_t uid;
	char vid[CA_MAXVIDLEN+1];
    int checksum_ok;
    
	strcpy (func, "Cns_srv_updateseg_checksum");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "updateseg_checksum", uid, gid, clienthost);
    unmarshall_HYPER (rbp, fileid);
    unmarshall_WORD (rbp, copyno);
    unmarshall_WORD (rbp, fsec);
	sprintf (logbuf, "updateseg_checksum %s %d %d",
             u64tostr (fileid, tmpbuf, 0), copyno, fsec);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
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

	sprintf (logbuf, "old segment: %s %d %d %s %d %c %s %d %d %02x%02x%02x%02x \"%s\" %x",
             u64tostr (old_smd_entry.s_fileid, tmpbuf, 0), old_smd_entry.copyno,
             old_smd_entry.fsec, u64tostr (old_smd_entry.segsize, tmpbuf2, 0),
             old_smd_entry.compression, old_smd_entry.s_status, old_smd_entry.vid,
             old_smd_entry.side, old_smd_entry.fseq, old_smd_entry.blockid[0],
             old_smd_entry.blockid[1], old_smd_entry.blockid[2], old_smd_entry.blockid[3],
             old_smd_entry.checksum_name, old_smd_entry.checksum);
	Cns_logreq (func, logbuf);

    /* Checking that the segment has not checksum */
    if (!(old_smd_entry.checksum_name == NULL
          || old_smd_entry.checksum_name[0] == '\0')) {
        sprintf (logbuf, "old checksum \"%s\" %d non NULL, Cannot overwrite",
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
            sprintf (logbuf, "setsegattrs: NULL checksum name with non zero value, overriding");
            Cns_logreq (func, logbuf);
            smd_entry.checksum = 0;
        } 
    }
    
	sprintf (logbuf, "new segment: %s %d %d %s %d %c %s %d %d %02x%02x%02x%02x \"%s\" %x",
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




/*	Cns_srv_replaceseg - replace file segment */

Cns_srv_replaceseg(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	int copyno;
	u_signed64 fileid;
	struct Cns_file_metadata filentry;
	int fsec;
	int fseq;
	char func[19];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+34];
	struct Cns_seg_metadata old_smd_entry;
	struct Cns_file_metadata parent_dir;
	char *rbp;
	Cns_dbrec_addr rec_addr;
	Cns_dbrec_addr rec_addrs;
	u_signed64 segsize;
	int side;
	struct Cns_seg_metadata smd_entry;
	char tmpbuf[21];
	char tmpbuf2[21];
	uid_t uid;
	char vid[CA_MAXVIDLEN+1];
    int checksum_ok;
    
	strcpy (func, "Cns_srv_replaceseg");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "replaceseg", uid, gid, clienthost);
        unmarshall_HYPER (rbp, fileid);
        unmarshall_WORD (rbp, copyno);
        unmarshall_WORD (rbp, fsec);
	sprintf (logbuf, "replaceseg %s %d %d",
	    u64tostr (fileid, tmpbuf, 0), copyno, fsec);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
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

	sprintf (logbuf, "old segment: %s %d %d %s %d %c %s %d %d %02x%02x%02x%02x \"%s\" %x",
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
	smd_entry.segsize = old_smd_entry.segsize;
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
            sprintf (logbuf, "setsegattrs: NULL checksum name with non zero value, overriding");
            Cns_logreq (func, logbuf);
            smd_entry.checksum = 0;
        } 
    }
    
	sprintf (logbuf, "new segment: %s %d %d %s %d %c %s %d %d %02x%02x%02x%02x \"%s\" %x",
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

/*      Cns_srv_rmdir - remove a directory entry */
 
Cns_srv_rmdir(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	struct Cns_class_metadata class_entry;
	u_signed64 cwd;
	struct Cns_file_metadata direntry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+7];
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addr;
	Cns_dbrec_addr rec_addrc;
	Cns_dbrec_addr rec_addrp;
	Cns_dbrec_addr rec_addru;
	uid_t uid;
	struct Cns_user_metadata umd_entry;

	strcpy (func, "Cns_srv_rmdir");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "rmdir", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	sprintf (logbuf, "rmdir %s", path);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	if (Cns_splitname (cwd, path, basename))
		RETURN (serrno);

	if (*basename == '/')	/* Cns_rmdir / */
		RETURN (EINVAL);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* check parent directory components for write/search permission */

	if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IWRITE|S_IEXEC, uid, gid,
	    clienthost, &parent_dir, &rec_addrp))
		RETURN (serrno);

	/* get and lock requested directory entry */

	if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
	    &direntry, 1, &rec_addr))
		RETURN (serrno);

	if ((direntry.filemode & S_IFDIR) == 0)
		RETURN (ENOTDIR);
	if (direntry.fileid == cwd)
		RETURN (EINVAL);	/* cannot remove current working directory */
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

	/* update nbdirs_using_class in Cns_class_metadata */

	if (direntry.fileclass > 0) {
		if (Cns_get_class_by_id (&thip->dbfd, direntry.fileclass,
		    &class_entry, 1, &rec_addrc))
			RETURN (serrno);
		class_entry.nbdirs_using_class--;
		if (Cns_update_class_entry (&thip->dbfd, &rec_addrc, &class_entry))
			RETURN (serrno);
	}
	RETURN (0);
}

/*	Cns_srv_setatime - set last access time */

Cns_srv_setatime(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	u_signed64 cwd;
	u_signed64 fileid;
	struct Cns_file_metadata filentry;
	char func[17];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+31];
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addr;
	char tmpbuf[21];
	uid_t uid;

	strcpy (func, "Cns_srv_setatime");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "setatime", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
        unmarshall_HYPER (rbp, fileid);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	sprintf (logbuf, "setatime %s %s", u64tostr (fileid, tmpbuf, 0), path);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
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
		if (Cns_splitname (cwd, path, basename))
			RETURN (serrno);

		/* check parent directory components for search permission */

		if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IEXEC, uid, gid,
		    clienthost, &parent_dir, NULL))
			RETURN (serrno);

		/* get/lock basename entry */

		if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
		    &filentry, 1, &rec_addr))
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

/*	Cns_srv_setcomment - add/replace a comment associated with a file/directory */

Cns_srv_setcomment(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	char comment[CA_MAXCOMMENTLEN+1];
	u_signed64 cwd;
	struct Cns_file_metadata filentry;
	char func[19];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+12];
	struct Cns_user_metadata old_umd_entry;
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addru;
	uid_t uid;
	struct Cns_user_metadata umd_entry;

	strcpy (func, "Cns_srv_setcomment");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "setcomment", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	if (unmarshall_STRINGN (rbp, comment, CA_MAXCOMMENTLEN+1))
		RETURN (EINVAL);
	sprintf (logbuf, "setcomment %s", path);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	if (Cns_splitname (cwd, path, basename))
		RETURN (serrno);

	/* check parent directory components for search permission */

	if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IEXEC, uid, gid,
	    clienthost, &parent_dir, NULL))
		RETURN (serrno);

	/* get basename entry */

	if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
	    &filentry, 0, NULL))
		RETURN (serrno);

	/* check if the user is authorized to add/replace the comment on this entry */

	if (uid != filentry.uid &&
	    Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost))
		RETURN (EACCES);

	/* add the comment or replace the comment if it exists */

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
	RETURN (0);
}

/*	Cns_srv_setfsize - set file size and last modification time */

Cns_srv_setfsize(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	u_signed64 cwd;
	u_signed64 fileid;
	struct Cns_file_metadata filentry;
	u_signed64 filesize;
	char func[17];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+52];
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addr;
	char tmpbuf[21];
	char tmpbuf2[21];
	uid_t uid;

	strcpy (func, "Cns_srv_setfsize");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "setfsize", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
        unmarshall_HYPER (rbp, fileid);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	unmarshall_HYPER (rbp, filesize);
	sprintf (logbuf, "setfsize %s %s %s", u64tostr (fileid, tmpbuf, 0),
	    path, u64tostr (filesize, tmpbuf2, 0));
	Cns_logreq (func, logbuf);
 	RESETID(uid, gid);
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
		if (Cns_splitname (cwd, path, basename))
			RETURN (serrno);

		/* check parent directory components for search permission */

		if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IEXEC, uid, gid,
		    clienthost, &parent_dir, NULL))
			RETURN (serrno);

		/* get/lock basename entry */

		if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
		    &filentry, 1, &rec_addr))
			RETURN (serrno);
	}

	/* check if the entry is a regular file and
	   if the user is authorized to set modification time for this entry */

	if (filentry.filemode & S_IFDIR)
		RETURN (EISDIR);
	if (uid != filentry.uid &&
	    Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost))
		RETURN (EACCES);

	/* update entry */

	filentry.filesize = filesize;
	filentry.mtime = time (0);
	filentry.ctime = filentry.mtime;

	if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
		RETURN (serrno);
	RETURN (0);
}

/*	Cns_srv_setsegattrs - set file segment attributes */

Cns_srv_setsegattrs(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	int copyno = 0;
	u_signed64 cwd;
	u_signed64 fileid;
	struct Cns_file_metadata filentry;
	int fsec;
	char func[20];
	gid_t gid;
	int i;
	char logbuf[CA_MAXPATHLEN+34];
	int nbseg;
	struct Cns_seg_metadata old_smd_entry;
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addr;
	Cns_dbrec_addr rec_addrs;
	struct Cns_seg_metadata smd_entry;
	char tmpbuf[21];
	char tmpbuf2[21];
	uid_t uid;

	strcpy (func, "Cns_srv_setsegattrs");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "setsegattrs", uid, gid, clienthost);
        unmarshall_HYPER (rbp, cwd);
        unmarshall_HYPER (rbp, fileid);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	unmarshall_WORD (rbp, nbseg);
	sprintf (logbuf, "setsegattrs %s %s",
	    u64tostr (fileid, tmpbuf, 0), path);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
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
		if (Cns_splitname (cwd, path, basename))
			RETURN (serrno);

		/* check parent directory components for search permission */

		if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IEXEC, uid, gid,
		    clienthost, &parent_dir, NULL))
			RETURN (serrno);

		/* get/lock basename entry */

		if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
		    &filentry, 1, &rec_addr))
			RETURN (serrno);
	}

	/* check if the entry is a regular file */

	if (filentry.filemode & S_IFDIR)
		RETURN (EISDIR);

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
		sprintf (logbuf, "setsegattrs %s %d %d %s %d %c %s %d %02x%02x%02x%02x %s:%x",
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
                RETURN(EINVAL);
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

/*	Cns_srv_shutdown - shutdown the name server */

Cns_srv_shutdown(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	int force = 0;
	char func[17];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+6];
	char *rbp;
	uid_t uid;

	strcpy (func, "Cns_srv_shutdown");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "shutdown", uid, gid, clienthost);
	unmarshall_WORD (rbp, force);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	being_shutdown = force + 1;
	RETURN (0);
}

/*	Cns_srv_stat - get information about a file or a directory */

Cns_srv_stat(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	u_signed64 cwd;
	u_signed64 fileid;
	struct Cns_file_metadata fmd_entry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+6];
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	char repbuf[57];
	char *sbp;
	char tmpbuf[21];
	uid_t uid;
	RESETID(uid, gid);

	strcpy (func, "Cns_srv_stat");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "stat", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	unmarshall_HYPER (rbp, fileid);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	sprintf (logbuf, "stat %s %s", u64tostr(fileid, tmpbuf, 0), path);
	Cns_logreq (func, logbuf);

	if (fileid) {
		/* get basename entry */

		if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid,
		    &fmd_entry, 0, NULL))
			RETURN (serrno);

		/* check parent directory components for search permission */

		if (Cns_chkbackperm (&thip->dbfd, fmd_entry.parent_fileid,
		    S_IEXEC, uid, gid, clienthost))
			RETURN (serrno);
	} else {
		if (strcmp (path, ".") == 0) {
			if (Cns_get_fmd_by_fileid (&thip->dbfd, cwd, &fmd_entry, 0, NULL))
				RETURN (serrno);
		} else {
			if (Cns_splitname (cwd, path, basename))
				RETURN (serrno);

			if (*basename == '/') {	/* Cns_stat / */
				parent_dir.fileid = 0;
			} else { /* check parent directory components for search permission */

				if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IEXEC,
				    uid, gid, clienthost, &parent_dir, NULL))
					RETURN (serrno);
			}

			/* get requested entry */

			if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid,
			    basename, &fmd_entry, 0, NULL))
					RETURN (serrno);
		}
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
	RETURN (0);
}

/*      Cns_srv_undelete - logically restore a file entry */
 
Cns_srv_undelete(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	int bof = 1;
	int c;
	u_signed64 cwd;
	DBLISTPTR dblistptr;
	struct Cns_file_metadata filentry;
	char func[17];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+10];
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addr;	/* file record address */
	Cns_dbrec_addr rec_addrp;	/* parent record address */
	Cns_dbrec_addr rec_addrs;	/* segment record address */
	struct Cns_seg_metadata smd_entry;
	uid_t uid;

	strcpy (func, "Cns_srv_undelete");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "undelete", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	sprintf (logbuf, "undelete %s", path);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	if (Cns_splitname (cwd, path, basename))
		RETURN (serrno);

	if (*basename == '/')	/* Cns_undelete / */
		RETURN (EINVAL);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* check parent directory components for write/search permission */

	if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IWRITE|S_IEXEC, uid, gid,
	    clienthost, &parent_dir, &rec_addrp))
		RETURN (serrno);

	/* get and lock requested file entry */

	if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
	    &filentry, 1, &rec_addr))
		RETURN (serrno);

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
	    &smd_entry, 1, &rec_addrs, 1, &dblistptr);	/* free res */
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
 
Cns_srv_unlink(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char basename[CA_MAXNAMELEN+1];
	int bof = 1;
	int c;
	u_signed64 cwd;
	DBLISTPTR dblistptr;
	struct Cns_file_metadata filentry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+8];
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addr;	/* file record address */
	Cns_dbrec_addr rec_addrp;	/* parent record address */
	Cns_dbrec_addr rec_addrs;	/* segment record address */
	Cns_dbrec_addr rec_addru;	/* comment record address */
	struct Cns_seg_metadata smd_entry;
	uid_t uid;
	struct Cns_user_metadata umd_entry;

	strcpy (func, "Cns_srv_unlink");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "unlink", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	sprintf (logbuf, "unlink %s", path);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	if (Cns_splitname (cwd, path, basename))
		RETURN (serrno);

	if (*basename == '/')	/* Cns_unlink / */
		RETURN (EINVAL);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* check parent directory components for write/search permission */

	if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IWRITE|S_IEXEC, uid, gid,
	    clienthost, &parent_dir, &rec_addrp))
		RETURN (serrno);

	/* get and lock requested file entry */

	if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
	    &filentry, 1, &rec_addr))
		RETURN (serrno);

	if (filentry.filemode & S_IFDIR)
		RETURN (EPERM);

	/* if the parent has the sticky bit set,
	   the user must own the file or the parent or
	   the basename entry must have write permission */

	if (parent_dir.filemode & S_ISVTX &&
	    uid != parent_dir.uid && uid != filentry.uid &&
	    Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost))
		RETURN (EACCES);

	/* delete file segments if any */

	while ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid,
	    &smd_entry, 1, &rec_addrs, 0, &dblistptr)) == 0) {
		if (Cns_delete_smd_entry (&thip->dbfd, &rec_addrs))
			RETURN (serrno);
		bof = 0;
	}
	(void) Cns_get_smd_by_pfid (&thip->dbfd, bof, filentry.fileid,
	    &smd_entry, 1, &rec_addrs, 1, &dblistptr);	/* free res */
	if (c < 0)
		RETURN (serrno);

	/* delete the comment if it exists */

	if (Cns_get_umd_by_fileid (&thip->dbfd, filentry.fileid, &umd_entry, 1,
	    &rec_addru) == 0) {
		if (Cns_delete_umd_entry (&thip->dbfd, &rec_addru))
			RETURN (serrno);
	} else if (serrno != ENOENT)
		RETURN (serrno);

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

/*	Cns_srv_utime - set last access and modification times */

Cns_srv_utime(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	time_t actime;
	char basename[CA_MAXNAMELEN+1];
	u_signed64 cwd;
	struct Cns_file_metadata filentry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+19];
	time_t modtime;
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addr;
	uid_t uid;
	int user_specified_time;

	strcpy (func, "Cns_srv_utime");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	nslogit (func, NS092, "utime", uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	unmarshall_LONG (rbp, user_specified_time);
	if (user_specified_time) {
		unmarshall_TIME_T (rbp, actime);
		unmarshall_TIME_T (rbp, modtime);
	}
	sprintf (logbuf, "utime %s %d", path, user_specified_time);
	Cns_logreq (func, logbuf);
	RESETID(uid, gid);
	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);
 
	if (Cns_splitname (cwd, path, basename))
		RETURN (serrno);

	/* check parent directory components for search permission */

	if (Cns_chkdirperm (&thip->dbfd, cwd, path, S_IEXEC, uid, gid,
	    clienthost, &parent_dir, NULL))
		RETURN (serrno);

	/* get/lock basename entry */

	if (Cns_get_fmd_by_fullid (&thip->dbfd, parent_dir.fileid, basename,
	    &filentry, 1, &rec_addr))
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
