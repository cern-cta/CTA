/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cns_procreq.c,v $ $Revision: 1.6 $ $Date: 2006/05/12 09:44:08 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */
 
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
#include "rfcntl.h"
#include "serrno.h"
#include "u64subr.h"
extern int being_shutdown;
extern char *cmd;
extern char localhost[CA_MAXHOSTNAMELEN+1];
extern int rdonly;
 
get_client_actual_id (thip, uid, gid, user)
struct Cns_srv_thread_info *thip;
uid_t *uid;
gid_t *gid;
char **user;
{
	struct passwd *pw;

#ifdef CSEC
	*uid = thip->Csec_uid;
	*gid = thip->Csec_gid;
	*user = thip->Csec_auth_id;
#else
	if ((pw = Cgetpwuid (*uid)) == NULL)
		*user = "UNKNOWN";
	else
		*user = pw->pw_name;
#endif
	return (0);
}

getpath (thip, cur_fileid, path)
struct Cns_srv_thread_info *thip;
u_signed64 cur_fileid;
char **path;
{
	struct Cns_file_metadata fmd_entry;
	int n;
	char *p;

	p = *path + CA_MAXPATHLEN;
	*p = '\0';
	while (cur_fileid != 2) {
		if (Cns_get_fmd_by_fileid (&thip->dbfd, cur_fileid, &fmd_entry,
		    0, NULL))
			return (serrno);
		n = strlen (fmd_entry.name);
		if ((p -= n) < *path + 1)
			return (SENAMETOOLONG);
		memcpy (p, fmd_entry.name, n);
		*(--p) = '/';
		cur_fileid = fmd_entry.parent_fileid;
	}
	*path = p;
	return (0);
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

marshall_DIRR (sbpp, magic, rep_entry)
char **sbpp;
int magic;
struct Cns_file_replica *rep_entry;
{
	int n;
	char *sbp;

	if (! sbpp) {
		n = HYPERSIZE + 1;
		n += strlen (rep_entry->host) + 1;
		n += strlen (rep_entry->sfn) + 1;
		return (n);
	}
	sbp = *sbpp;
	marshall_HYPER (sbp, rep_entry->fileid);
	marshall_BYTE (sbp, rep_entry->status);
	marshall_STRING (sbp, rep_entry->host);
	marshall_STRING (sbp, rep_entry->sfn);
	*sbpp = sbp;
	return (0);
}

marshall_DIRX (sbpp, magic, fmd_entry)
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

marshall_DIRXR (sbpp, magic, fmd_entry)
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

/*	Cns_srv_aborttrans - abort transaction */

Cns_srv_aborttrans(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
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

/*	Cns_srv_access - check accessibility of a file/directory */

Cns_srv_access(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	int amode;
	u_signed64 cwd;
	struct Cns_file_metadata fmd_entry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+13];
	mode_t mode;
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "access %o %s", amode, path);
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

/*	Cns_srv_accessr - check accessibility of a file replica */

Cns_srv_accessr(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	int amode;
	struct Cns_file_metadata fmd_entry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXSFNLEN+14];
	mode_t mode;
	char *rbp;
	struct Cns_file_replica rep_entry;
	char sfn[CA_MAXSFNLEN+1];
	uid_t uid;
	char *user;

	strcpy (func, "Cns_srv_accessr");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	get_client_actual_id (thip, &uid, &gid, &user);
	nslogit (func, NS092, "accessr", user, uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, sfn, CA_MAXSFNLEN+1))
		RETURNQ (SENAMETOOLONG);
	unmarshall_LONG (rbp, amode);
	sprintf (logbuf, "accessr %o %s", amode, sfn);
	Cns_logreq (func, logbuf);

	if (amode & ~(R_OK | W_OK | X_OK | F_OK))
		RETURNQ (EINVAL);

	if (Cns_get_rep_by_sfn (&thip->dbfd, sfn, &rep_entry, 0, NULL))
                RETURNQ (serrno);

	if ((amode & W_OK) && rep_entry.status != 'P')
		RETURNQ (EACCES);

	/* get basename entry */

	if (Cns_get_fmd_by_fileid (&thip->dbfd, rep_entry.fileid,
	    &fmd_entry, 0, NULL))
		RETURNQ (serrno);

	/* check parent directory components for search permission */

	if (Cns_chkbackperm (&thip->dbfd, fmd_entry.parent_fileid,
	    S_IEXEC, uid, gid, clienthost))
		RETURNQ (serrno);

	/* check permissions for basename */

	if (amode == F_OK)
		RETURNQ (0);
	mode = (amode & (R_OK|W_OK|X_OK)) << 6;
	if (Cns_chkentryperm (&fmd_entry, mode, uid, gid, clienthost))
		RETURNQ (EACCES);
	RETURNQ (0);
}

/*      Cns_srv_addreplica - add a replica for a given file */

Cns_srv_addreplica(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char f_type;
	u_signed64 fileid;
	struct Cns_file_metadata fmd_entry;
	char fs[80];
	char func[19];
	gid_t gid;
	char guid[CA_MAXGUIDLEN+1];
	char logbuf[CA_MAXGUIDLEN+CA_MAXHOSTNAMELEN+CA_MAXSFNLEN+14];
	char poolname[CA_MAXPOOLNAMELEN+1];
	char *rbp;
	struct Cns_file_replica rep_entry;
	char server[CA_MAXHOSTNAMELEN+1];
	char sfn[CA_MAXSFNLEN+1];
	char status;
	uid_t uid;
	char *user;

	strcpy (func, "Cns_srv_addreplica");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	get_client_actual_id (thip, &uid, &gid, &user);
	nslogit (func, NS092, "addreplica", user, uid, gid, clienthost);
	if (rdonly)
		RETURN (EROFS);
	unmarshall_HYPER (rbp, fileid);
	if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN+1))
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, server, CA_MAXHOSTNAMELEN+1))
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, sfn, CA_MAXSFNLEN+1))
		RETURN (SENAMETOOLONG);
	sprintf (logbuf, "addreplica %s %s %s", guid, server, sfn);
	Cns_logreq (func, logbuf);
	if (magic >= CNS_MAGIC2) {
		unmarshall_BYTE (rbp, status);
		if (magic >= CNS_MAGIC3) {
			unmarshall_BYTE (rbp, f_type);
		} else
			f_type = '\0';
		if (unmarshall_STRINGN (rbp, poolname, CA_MAXPOOLNAMELEN+1))
			RETURN (EINVAL);
		if (magic >= CNS_MAGIC3) {
			if (unmarshall_STRINGN (rbp, fs, 80))
				RETURN (EINVAL);
		} else
			fs[0] = '\0';
	} else {
		status = '-';
		f_type = '\0';
		poolname[0] = '\0';
		fs[0] = '\0';
	}
	if (*server == '\0' || *sfn == '\0')
		RETURN (EINVAL);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

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
		if (! *guid)
			RETURN (ENOENT);

		/* get basename entry */

                if (Cns_get_fmd_by_guid (&thip->dbfd, guid, &fmd_entry, 0, NULL))
                        RETURN (serrno);

		/* check parent directory components for search permission */

		if (Cns_chkbackperm (&thip->dbfd, fmd_entry.parent_fileid,
		    S_IEXEC, uid, gid, clienthost))
			RETURN (serrno);
	}

	/* is the sfn valid? */


	/* check if the user is authorized to add a replica for this file */

	if (uid != fmd_entry.uid &&
	    Cns_chkentryperm (&fmd_entry, S_IREAD, uid, gid, clienthost))
		RETURN (EACCES);
	if (fmd_entry.filemode & S_IFDIR)
		RETURN (EISDIR);

	/* add replica */

	memset ((char *) &rep_entry, 0, sizeof(rep_entry));
	rep_entry.fileid = fmd_entry.fileid;
	rep_entry.nbaccesses = 1;
	rep_entry.atime = time (0);
	rep_entry.status = status;
	rep_entry.f_type = f_type;
	strcpy (rep_entry.poolname, poolname);
	strcpy (rep_entry.host, server);
	strcpy (rep_entry.fs, fs);
	strcpy (rep_entry.sfn, sfn);

	if (Cns_insert_rep_entry (&thip->dbfd, &rep_entry))
		RETURN (serrno);
	RETURN (0);
}

/*      Cns_srv_chclass - change class on directory */

Cns_srv_chclass(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
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
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "chclass %s %d %s", path, classid, class_name);
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
	sprintf (logbuf, "chdir %s", path);
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

Cns_srv_chmod(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	u_signed64 cwd;
	struct Cns_file_metadata fmd_entry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+12];
	mode_t mode;
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "chmod %o %s", mode, path);
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

Cns_srv_chown(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
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
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "chown %d:%d %s", new_uid, new_gid, path);
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
	if (*fmd_entry.acl)
		Cns_acl_chown (&fmd_entry);
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
	int bof = 1;
	int c;
	u_signed64 cwd;
	DBLISTPTR dblistptr;
	struct Cns_file_metadata filentry;
	char func[16];
	gid_t gid;
	char guid[CA_MAXGUIDLEN+1];
	char logbuf[CA_MAXPATHLEN+CA_MAXGUIDLEN+18];
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
	struct Cns_file_replica rep_entry;
	struct Cns_seg_metadata smd_entry;
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
	sprintf (logbuf, "creat %s %s %o %o", path, guid, mode, mask);
	Cns_logreq (func, logbuf);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* check parent directory components for write/search permission and
	   get/lock basename entry if it exists */

	if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
	    &parent_dir, &rec_addrp, &filentry, &rec_addr, 0))
	    	RETURN (serrno);

	if (*filentry.name == '/')	/* Cns_creat / */
		RETURN (EISDIR);

	if (filentry.fileid) {	/* file exists */
		if (filentry.filemode & S_IFDIR)
			RETURN (EISDIR);
		if (strcmp (filentry.guid, guid))
			RETURN (EEXIST);

		/* check write permission in basename entry */

		if (Cns_chkentryperm (&filentry, S_IWRITE, uid, gid, clienthost))
			RETURN (EACCES);

		if (strcmp (cmd, "nsdaemon")) {
			/* check if replicas still exit */

			if ((c = Cns_list_rep_entry (&thip->dbfd, 1, filentry.fileid,
			    &rep_entry, 0, NULL, 0, &dblistptr)) == 0) {
				(void) Cns_list_rep_entry (&thip->dbfd, 0, filentry.fileid,
				    &rep_entry, 0, NULL, 1, &dblistptr);	/* free res */
				RETURN (EEXIST);
			}
			(void) Cns_list_rep_entry (&thip->dbfd, 0, filentry.fileid,
			    &rep_entry, 0, NULL, 1, &dblistptr);	/* free res */
			if (c < 0)
				RETURN (serrno);
		} else {
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
		}

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
	} else {	/* must create the file */
		if (strcmp (cmd, "nsdaemon") == 0 && parent_dir.fileclass <= 0)
			RETURN (EINVAL);
		if (strncmp (cmd, "lfc", 3) == 0 && *guid == '\0')
			RETURN (EINVAL);	/* guid is mandatory for the LFC */
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

/*	Cns_srv_delcomment - delete a comment associated with a file/directory */

Cns_srv_delcomment(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	u_signed64 cwd;
	struct Cns_file_metadata filentry;
	char func[19];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+12];
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "delcomment %s", path);
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
 
Cns_srv_delete(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
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
	struct Cns_file_replica rep_entry;
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
	sprintf (logbuf, "delete %s", path);
	Cns_logreq (func, logbuf);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* check parent directory components for write/search permission and
	   get/lock basename entry */

	if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
	    &parent_dir, &rec_addrp, &filentry, &rec_addr, CNS_MUST_EXIST|CNS_NOFOLLOW))
		RETURN (serrno);

	if (*filentry.name == '/')	/* Cns_delete / */
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

	if (strcmp (cmd, "nsdaemon")) {
		/* mark file replicas if any as logically deleted */

		while ((c = Cns_list_rep_entry (&thip->dbfd, bof, filentry.fileid,
		    &rep_entry, 1, &rec_addrs, 0, &dblistptr)) == 0) {
			rep_entry.status = 'D';
			if (Cns_update_rep_entry (&thip->dbfd, &rec_addrs, &rep_entry))
				RETURN (serrno);
			bof = 0;
		}
		(void) Cns_list_rep_entry (&thip->dbfd, bof, filentry.fileid,
		    &rep_entry, 1, &rec_addrs, 1, &dblistptr);	/* free res */
		if (c < 0)
			RETURN (serrno);
	} else {
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
		    &smd_entry, 1, &rec_addrs, 1, &dblistptr);	/* free res */
		if (c < 0)
			RETURN (serrno);
	}

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

/*      Cns_srv_delreplica - delete a replica for a given file */

Cns_srv_delreplica(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	u_signed64 fileid;
	struct Cns_file_metadata fmd_entry;
	char func[19];
	gid_t gid;
	char guid[CA_MAXGUIDLEN+1];
	char logbuf[CA_MAXGUIDLEN+CA_MAXSFNLEN+13];
	char *rbp;
	Cns_dbrec_addr rec_addr;
	struct Cns_file_replica rep_entry;
	char sfn[CA_MAXSFNLEN+1];
	uid_t uid;
	char *user;

	strcpy (func, "Cns_srv_delreplica");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	get_client_actual_id (thip, &uid, &gid, &user);
	nslogit (func, NS092, "delreplica", user, uid, gid, clienthost);
	if (rdonly)
		RETURN (EROFS);
	unmarshall_HYPER (rbp, fileid);
	if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN+1))
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, sfn, CA_MAXSFNLEN+1))
		RETURN (SENAMETOOLONG);
	sprintf (logbuf, "delreplica %s %s", guid, sfn);
	Cns_logreq (func, logbuf);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* get basename entry */

	rep_entry.fileid = 0;
	if (fileid) {
		if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid,
		    &fmd_entry, 0, NULL))
			RETURN (serrno);
	} else if (*guid) {
                if (Cns_get_fmd_by_guid (&thip->dbfd, guid, &fmd_entry, 0, NULL))
                        RETURN (serrno);
	} else {
		if (Cns_get_rep_by_sfn (&thip->dbfd, sfn, &rep_entry, 1, &rec_addr))
			RETURN (serrno);
		if (Cns_get_fmd_by_fileid (&thip->dbfd, rep_entry.fileid,
		    &fmd_entry, 0, NULL))
			RETURN (serrno);
	}

	/* check parent directory components for search permission */

	if (Cns_chkbackperm (&thip->dbfd, fmd_entry.parent_fileid,
	    S_IEXEC, uid, gid, clienthost))
		RETURN (serrno);

	/* check if the user is authorized to delete a replica for this file */

	if (uid != fmd_entry.uid &&
	    Cns_chkentryperm (&fmd_entry, S_IWRITE, uid, gid, clienthost))
		RETURN (EACCES);

	/* delete replica */

	if (rep_entry.fileid == 0 &&
	    Cns_get_rep_by_sfn (&thip->dbfd, sfn, &rep_entry, 1, &rec_addr))
		RETURN (serrno);
	if ((fileid && fileid != rep_entry.fileid) ||
	    (*guid && fmd_entry.fileid != rep_entry.fileid))
		RETURN (ENOENT);
	if (Cns_delete_rep_entry (&thip->dbfd, &rec_addr))
		RETURN (serrno);
	RETURN (0);
}

/*	Cns_srv_du - summarize file space usage */

compute_du4dir (thip, direntry, Lflag, uid, gid, clienthost, nbbytes, nbentries)
struct Cns_srv_thread_info *thip;
struct Cns_file_metadata *direntry;
int Lflag;
uid_t uid;
gid_t gid;
char *clienthost;
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
	struct dirlist *dlc;		/* pointer to current directory in the list */
	struct dirlist *dlf = NULL;	/* pointer to first directory in the list */
	struct dirlist *dll;		/* pointer to last directory in the list */
	struct Cns_file_metadata fmd_entry;

	if (Cns_chkentryperm (direntry, S_IREAD|S_IEXEC, uid, gid, clienthost))
		return (EACCES);
	while ((c = Cns_get_fmd_by_pfid (&thip->dbfd, bod, direntry->fileid,
	    &fmd_entry, 1, 0, &dblistptr)) == 0) {	/* loop on directory entries */
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
		} else {	/* regular file */
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

Cns_srv_du(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	int c;
	u_signed64 cwd;
	struct Cns_file_metadata fmd_entry;
	char func[16];
	gid_t gid;
	int Lflag;
	char logbuf[CA_MAXPATHLEN+4];
	u_signed64 nbbytes = 0;
	u_signed64 nbentries = 0;
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "du %s", path);
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
	} else {	/* regular file */
		nbbytes += fmd_entry.filesize;
		nbentries += 1;
	}
	sbp = repbuf;
	marshall_HYPER (sbp, nbbytes);
	marshall_HYPER (sbp, nbentries);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURNQ (0);
}

/*	Cns_srv_endsess - end session */

Cns_srv_endsess(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
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

/*	Cns_srv_endtrans - end transaction mode */

Cns_srv_endtrans(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
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

Cns_srv_getacl(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	u_signed64 cwd;
	struct Cns_file_metadata fmd_entry;
	char func[16];
	gid_t gid;
	char *iacl;
	char logbuf[CA_MAXPATHLEN+8];
	int nentries = 0;
	char *p;
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "getacl %s", path);
	Cns_logreq (func, logbuf);

	/* check parent directory components for search permission and
	   get basename entry */

	if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost, NULL,
	    NULL, &fmd_entry, NULL, CNS_MUST_EXIST))
		RETURNQ (serrno);

	sbp = repbuf;
	marshall_WORD (sbp, nentries);		/* will be updated */
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
			if (p = strchr (iacl, ','))
				p++;
			marshall_BYTE (sbp, *iacl - '@');
			marshall_LONG (sbp, atoi (iacl + 2));
			marshall_BYTE (sbp, *(iacl + 1) - '0');
			nentries++;
		}
	}
	p = repbuf;
	marshall_WORD (p, nentries);		/* update nentries in reply */
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURNQ (0);
}

/*	Cns_srv_getcomment - get the comment associated with a file/directory */

Cns_srv_getcomment(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	u_signed64 cwd;
	struct Cns_file_metadata filentry;
	char func[19];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+12];
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "getcomment %s", path);
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

/*	Cns_srv_getlinks - get the link entries associated with a given file */

Cns_srv_getlinks(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
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
	char logbuf[CA_MAXPATHLEN+CA_MAXGUIDLEN+11];
	int n;
	char *p;
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "getlinks %s %s", path, guid);
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
		if (*path != '/') {	/* need to get path */
			p = tmp_path;
			if ((c = getpath (thip, fmd_entry.fileid, &p)))
				RETURNQ (c);
			strcpy (lnk_entry.linkname, p);
		} else
			strcpy (lnk_entry.linkname, path);
	}
	marshall_STRING (sbp, lnk_entry.linkname);
	while ((c = Cns_list_lnk_entry (&thip->dbfd, bol, lnk_entry.linkname,
	    &lnk_entry, 0, &dblistptr)) == 0) {
		bol = 0;
		p = tmp_path;
		if ((c = getpath (thip, lnk_entry.fileid, &p)))
			RETURNQ (c);
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

Cns_srv_getpath(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	int c;
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
		if ((c = getpath (thip, cur_fileid, &p)))
			RETURNQ (c);
	}
	sbp = repbuf;
	marshall_STRING (sbp, p);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURNQ (0);
}

/*	Cns_srv_getreplica - get replica entries for a given file */

Cns_srv_getreplica(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	int bol = 1;
	int c;
	u_signed64 cwd;
	DBLISTPTR dblistptr;
	struct Cns_file_metadata fmd_entry;
	char func[19];
	gid_t gid;
	char guid[CA_MAXGUIDLEN+1];
	char logbuf[CA_MAXPATHLEN+CA_MAXGUIDLEN+13];
	int n;
	int nbrepl = 0;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	struct Cns_file_replica rep_entry;
	char repbuf[REPBUFSZ];
	char *sbp = repbuf;
	char se[CA_MAXHOSTNAMELEN+1];
	uid_t uid;
	char *user;

	strcpy (func, "Cns_srv_getreplica");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	get_client_actual_id (thip, &uid, &gid, &user);
	nslogit (func, NS092, "getreplica", user, uid, gid, clienthost);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURNQ (SENAMETOOLONG);
	if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN + 1) < 0)
		RETURNQ (EINVAL);
	if (unmarshall_STRINGN (rbp, se, CA_MAXHOSTNAMELEN + 1) < 0)
		RETURNQ (EINVAL);
	sprintf (logbuf, "getreplica %s %s", path, guid);
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
	while ((c = Cns_list_rep_entry (&thip->dbfd, bol, fmd_entry.fileid,
	    &rep_entry, 0, NULL, 0, &dblistptr)) == 0) {
		bol = 0;
		if (*se && strcmp (rep_entry.host, se))
			continue;
		n = 2 * HYPERSIZE + 2 * TIME_TSIZE + 2;
		n += strlen (rep_entry.poolname) + 1;
		n += strlen (rep_entry.host) + 1;
		n += strlen (rep_entry.fs) + 1;
		n += strlen (rep_entry.sfn) + 1;
		if (sbp - repbuf + n > REPBUFSZ) {
			sendrep (thip->s, MSG_REPLIC, sbp - repbuf, repbuf);
			sbp = repbuf;
		}
		marshall_HYPER (sbp, rep_entry.fileid);
		marshall_HYPER (sbp, rep_entry.nbaccesses);
		marshall_TIME_T (sbp, rep_entry.atime);
		marshall_TIME_T (sbp, rep_entry.ptime);
		marshall_BYTE (sbp, rep_entry.status);
		marshall_BYTE (sbp, rep_entry.f_type);
		marshall_STRING (sbp, rep_entry.poolname);
		marshall_STRING (sbp, rep_entry.host);
		marshall_STRING (sbp, rep_entry.fs);
		marshall_STRING (sbp, rep_entry.sfn);
		nbrepl++;
	}
	(void) Cns_list_rep_entry (&thip->dbfd, bol, fmd_entry.fileid,
	    &rep_entry, 0, NULL, 1, &dblistptr);
	if (c < 0)
		RETURNQ (serrno);
	if (sbp > repbuf)
		sendrep (thip->s, MSG_REPLIC, sbp - repbuf, repbuf);
	sbp = repbuf;
	marshall_LONG (sbp, nbrepl);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURNQ (0);
}

/*	Cns_srv_getsegattrs - get file segments attributes */

Cns_srv_getsegattrs(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
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
	char logbuf[CA_MAXPATHLEN+34];
	int nbseg = 0;
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "getsegattrs %s %s",
	    u64tostr (fileid, tmpbuf, 0), path);
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

/*      Cns_srv_lchown - change owner and group of a file or a directory */

Cns_srv_lchown(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	u_signed64 cwd;
	struct Cns_file_metadata fmd_entry;
	int found;
	char func[16];
	gid_t gid;
	struct group *gr;
	char logbuf[CA_MAXPATHLEN+20];
	char **membername;
	int need_p_admin = 0;
	int need_p_expt_admin = 0;
	gid_t new_gid;
	uid_t new_uid;
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "lchown %d:%d %s", new_uid, new_gid, path);
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
	if (*fmd_entry.acl)
		Cns_acl_chmod (&fmd_entry);
	fmd_entry.ctime = time (0);
	if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &fmd_entry))
		RETURN (serrno);
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
	char *user;

	strcpy (func, "Cns_srv_listclass");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	get_client_actual_id (thip, &uid, &gid, &user);
	nslogit (func, NS092, "listclass", user, uid, gid, clienthost);
	unmarshall_WORD (rbp, listentsz);
	unmarshall_WORD (rbp, bol);

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

Cns_srv_listlinks(magic, req_data, clienthost, thip, lnk_entry, endlist, dblistptr)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
struct Cns_symlinks *lnk_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	int bol;	/* beginning of list flag */
	int c;
	u_signed64 cwd;
	int eol = 0;	/* end of list flag */
	struct Cns_file_metadata fmd_entry;
	char func[18];
	gid_t gid;
	char guid[CA_MAXGUIDLEN+1];
	int listentsz;	/* size of client machine Cns_linkinfo structure */
	char logbuf[CA_MAXPATHLEN+CA_MAXGUIDLEN+12];
	int maxsize;
	int nbentries = 0;
	char outbuf[LISTBUFSZ+4];
	char *p;
	char path[CA_MAXPATHLEN+1];
	char *q;
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
	sprintf (logbuf, "listlinks %s %s", path, guid);
	Cns_logreq (func, logbuf);

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
			if (*path != '/') {	/* need to get path */
				p = tmp_path;
				if ((c = getpath (thip, fmd_entry.fileid, &p)))
					RETURN (c);
				strcpy (lnk_entry->linkname, p);
			} else
				strcpy (lnk_entry->linkname, path);
		}
	}
		
	/* return as many entries as possible to the client */

	maxsize = LISTBUFSZ;
	sbp = outbuf;
	marshall_WORD (sbp, nbentries);		/* will be updated */

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
		if ((c = getpath (thip, lnk_entry->fileid, &p)))
			RETURN (c);
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
reply:
	marshall_WORD (sbp, eol);
	p = outbuf;
	marshall_WORD (p, nbentries);		/* update nbentries in reply */
	sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
	RETURN (0);
}

Cns_srv_listrep4gc(magic, req_data, clienthost, thip, rep_entry, endlist, dblistptr)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
struct Cns_file_replica *rep_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	int bol;	/* beginning of list flag */
	int c;
	int eol = 0;	/* end of list flag */
	char func[19];
	gid_t gid;
	int listentsz;	/* size of client machine Cns_filereplica structure excluding sfn */
	char logbuf[CA_MAXPOOLNAMELEN+12];
	int maxsize;
	int nbentries = 0;
	char outbuf[LISTBUFSZ+4];
	char *p;
	char poolname[CA_MAXPOOLNAMELEN+1];
	char *rbp;
	int rnl;
	char *sbp;
	uid_t uid;
	char *user;

	strcpy (func, "Cns_srv_listrep4gc");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	get_client_actual_id (thip, &uid, &gid, &user);
	nslogit (func, NS092, "listrep4gc", user, uid, gid, clienthost);
	unmarshall_WORD (rbp, listentsz);
	if (unmarshall_STRINGN (rbp, poolname, CA_MAXPOOLNAMELEN+1))
		RETURN (EINVAL);
	unmarshall_WORD (rbp, bol);
	sprintf (logbuf, "listrep4gc %s", poolname);
	Cns_logreq (func, logbuf);

	/* return as many entries as possible to the client */

	maxsize = LISTBUFSZ;
	sbp = outbuf;
	marshall_WORD (sbp, nbentries);		/* will be updated */

	if (bol || endlist)
		c = Cns_list_rep4gc (&thip->dbfd, bol, poolname, rep_entry,
		    endlist, dblistptr);
	else
		c = 0;
	while (c == 0) {
		rnl = strlen (rep_entry->sfn);
		if (listentsz + rnl > maxsize) break;
		marshall_HYPER (sbp, rep_entry->fileid);
		marshall_HYPER (sbp, rep_entry->nbaccesses);
		marshall_TIME_T (sbp, rep_entry->atime);
		marshall_TIME_T (sbp, rep_entry->ptime);
		marshall_BYTE (sbp, rep_entry->status);
		marshall_BYTE (sbp, rep_entry->f_type);
		marshall_STRING (sbp, rep_entry->poolname);
		marshall_STRING (sbp, rep_entry->host);
		marshall_STRING (sbp, rep_entry->fs);
		marshall_STRING (sbp, rep_entry->sfn);
		maxsize -= ((listentsz + rnl + 8) / 8) * 8;
		nbentries++;
		bol = 0;
		c = Cns_list_rep4gc (&thip->dbfd, bol, poolname, rep_entry,
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

Cns_srv_listreplica(magic, req_data, clienthost, thip, fmd_entry, rep_entry, endlist, dblistptr)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
struct Cns_file_metadata *fmd_entry;
struct Cns_file_replica *rep_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	int bol;	/* beginning of list flag */
	int c;
	u_signed64 cwd;
	int eol = 0;	/* end of list flag */
	char func[20];
	gid_t gid;
	char guid[CA_MAXGUIDLEN+1];
	int listentsz;	/* size of client machine Cns_filereplica structure */
	char logbuf[CA_MAXPATHLEN+CA_MAXGUIDLEN+14];
	int maxsize;
	int nbentries = 0;
	char outbuf[LISTBUFSZ+4];
	char *p;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	char *sbp;
	uid_t uid;
	char *user;

	strcpy (func, "Cns_srv_listreplica");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	get_client_actual_id (thip, &uid, &gid, &user);
	nslogit (func, NS092, "listreplica", user, uid, gid, clienthost);
	unmarshall_WORD (rbp, listentsz);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	if (unmarshall_STRINGN (rbp, guid, CA_MAXGUIDLEN + 1) < 0)
		RETURN (EINVAL);
	unmarshall_WORD (rbp, bol);
	sprintf (logbuf, "listreplica %s %s", path, guid);
	Cns_logreq (func, logbuf);

	if (bol) {
		if (*path) {
			/* check parent directory components for search permission and
			   get basename entry */

			if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
			    NULL, NULL, fmd_entry, NULL, CNS_MUST_EXIST))
				RETURN (serrno);
			if (*guid && strcmp (guid, fmd_entry->guid))
				RETURN (EINVAL);
		} else {
			if (! *guid)
				RETURN (ENOENT);

			/* get basename entry */

			if (Cns_get_fmd_by_guid (&thip->dbfd, guid, fmd_entry, 0, NULL))
				RETURN (serrno);

			/* do not check parent directory components for search permission 
			 * as symlinks can anyway point directly at a file
			 */
		}
	}

	/* return as many entries as possible to the client */

	maxsize = LISTBUFSZ;
	sbp = outbuf;
	marshall_WORD (sbp, nbentries);		/* will be updated */

	if (bol || endlist)
		c = Cns_list_rep_entry (&thip->dbfd, bol, fmd_entry->fileid,
		    rep_entry, 0, NULL, endlist, dblistptr);
	else
		c = 0;
	while (c == 0) {
		if (listentsz > maxsize) break;
		if (magic >= CNS_MAGIC2) {
			marshall_HYPER (sbp, rep_entry->fileid);
		}
		marshall_HYPER (sbp, rep_entry->nbaccesses);
		marshall_TIME_T (sbp, rep_entry->atime);
		if (magic >= CNS_MAGIC2) {
			marshall_TIME_T (sbp, rep_entry->ptime);
			marshall_BYTE (sbp, rep_entry->status);
			marshall_BYTE (sbp, rep_entry->f_type);
			marshall_STRING (sbp, rep_entry->poolname);
		}
		marshall_STRING (sbp, rep_entry->host);
		if (magic >= CNS_MAGIC2) {
			marshall_STRING (sbp, rep_entry->fs);
		}
		marshall_STRING (sbp, rep_entry->sfn);
		maxsize -= listentsz;
		nbentries++;
		bol = 0;
		c = Cns_list_rep_entry (&thip->dbfd, bol, fmd_entry->fileid,
		    rep_entry, 0, NULL, endlist, dblistptr);
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

Cns_srv_listreplicax(magic, req_data, clienthost, thip, rep_entry, endlist, dblistptr)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
struct Cns_file_replica *rep_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	int bol;	/* beginning of list flag */
	int c;
	int eol = 0;	/* end of list flag */
	char fs[80];
	char func[21];
	gid_t gid;
	int listentsz;	/* size of client machine Cns_filereplica structure excluding sfn */
	char logbuf[CA_MAXPOOLNAMELEN+CA_MAXHOSTNAMELEN+95];
	int maxsize;
	int nbentries = 0;
	char outbuf[LISTBUFSZ+4];
	char *p;
	char poolname[CA_MAXPOOLNAMELEN+1];
	char *rbp;
	int rnl;
	char *sbp;
	char server[CA_MAXHOSTNAMELEN+1];
	uid_t uid;
	char *user;

	strcpy (func, "Cns_srv_listreplicax");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	get_client_actual_id (thip, &uid, &gid, &user);
	nslogit (func, NS092, "listreplicax", user, uid, gid, clienthost);
	unmarshall_WORD (rbp, listentsz);
	if (unmarshall_STRINGN (rbp, poolname, CA_MAXPOOLNAMELEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, server, CA_MAXHOSTNAMELEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, fs, 80) < 0)
		RETURN (EINVAL);
	unmarshall_WORD (rbp, bol);
	sprintf (logbuf, "listreplicax %s %s %s", poolname, server, fs);
	Cns_logreq (func, logbuf);

	/* return as many entries as possible to the client */

	maxsize = LISTBUFSZ;
	sbp = outbuf;
	marshall_WORD (sbp, nbentries);		/* will be updated */

	if (bol || endlist)
		c = Cns_list_rep4admin (&thip->dbfd, bol, poolname, server, fs,
		    rep_entry, endlist, dblistptr);
	else
		c = 0;
	while (c == 0) {
		rnl = strlen (rep_entry->sfn);
		if (listentsz + rnl > maxsize) break;
		marshall_HYPER (sbp, rep_entry->fileid);
		marshall_HYPER (sbp, rep_entry->nbaccesses);
		marshall_TIME_T (sbp, rep_entry->atime);
		marshall_TIME_T (sbp, rep_entry->ptime);
		marshall_BYTE (sbp, rep_entry->status);
		marshall_BYTE (sbp, rep_entry->f_type);
		marshall_STRING (sbp, rep_entry->poolname);
		marshall_STRING (sbp, rep_entry->host);
		marshall_STRING (sbp, rep_entry->fs);
		marshall_STRING (sbp, rep_entry->sfn);
		maxsize -= ((listentsz + rnl + 8) / 8) * 8;
		nbentries++;
		bol = 0;
		c = Cns_list_rep4admin (&thip->dbfd, bol, poolname, server, fs,
		    rep_entry, endlist, dblistptr);
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
	sprintf (logbuf, "listtape %s %d", vid, bov);
	Cns_logreq (func, logbuf);

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

/*	Cns_srv_lstat - get information about a symbolic link */

Cns_srv_lstat(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	u_signed64 cwd;
	u_signed64 fileid;
	struct Cns_file_metadata fmd_entry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+8];
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "lstat %s %s", u64tostr(fileid, tmpbuf, 0), path);
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
 
Cns_srv_mkdir(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	struct Cns_class_metadata class_entry;
	u_signed64 cwd;
	struct Cns_file_metadata direntry;
	char func[16];
	gid_t gid;
	char guid[CA_MAXGUIDLEN+1];
	char logbuf[CA_MAXPATHLEN+CA_MAXGUIDLEN+18];
	mode_t mask;
	mode_t mode;
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addrc;
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
	sprintf (logbuf, "mkdir %s %s %o %o", path, guid, mode, mask);
	Cns_logreq (func, logbuf);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* check parent directory components for write/search permission and
	   get basename entry if it exists */

	if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
	    &parent_dir, &rec_addrp, &direntry, NULL, CNS_NOFOLLOW))
		RETURN (serrno);

	if (*direntry.name == '/')	/* Cns_mkdir / */
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
	char *user;

	strcpy (func, "Cns_srv_open");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	get_client_actual_id (thip, &uid, &gid, &user);
	nslogit (func, NS092, "open", user, uid, gid, clienthost);
	unmarshall_WORD (rbp, mask);
	unmarshall_HYPER (rbp, cwd);
	if (unmarshall_STRINGN (rbp, path, CA_MAXPATHLEN+1))
		RETURN (SENAMETOOLONG);
	unmarshall_LONG (rbp, oflag);
	oflag = ntohopnflg (oflag);
	unmarshall_LONG (rbp, mode);
	sprintf (logbuf, "open %s %o %o %o", path, oflag, mode, mask);
	Cns_logreq (func, logbuf);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* check parent directory components for (write)/search permission and
	   get basename entry if it exists */

	if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
	    &parent_dir, (oflag & O_CREAT) ? &rec_addrp : NULL,
	    &filentry, (oflag & O_TRUNC) ? &rec_addr : NULL,
	    oflag & (O_CREAT | O_EXCL) ? CNS_NOFOLLOW : 0))
		RETURN (serrno);

	/* check if the file exists already */

	if (filentry.fileid == 0 && (oflag & O_CREAT) == 0)
		RETURN (ENOENT);

	if (filentry.fileid) {	/* file exists */
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
		if (Cns_unique_id (&thip->dbfd, &filentry.fileid) < 0)
			RETURN (serrno);
		/* parent_fileid and name have been set by Cns_parsepath */
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
	gid_t gid;
	char guid[CA_MAXGUIDLEN+1];
	char logbuf[CA_MAXPATHLEN+CA_MAXGUIDLEN+10];
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "opendir %s %s", path, guid);
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
	int n;
	int nbentries = 0;
	char *p;
	char *rbp;
	Cns_dbrec_addr rec_addr;
	struct Cns_file_replica rep_entry;
	char repbuf[REPBUFSZ];
	DBLISTPTR replistptr;
	char sav_name[CA_MAXNAMELEN+1];
	char *sbp;
	char *sbpr = repbuf;
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
	if (getattr == 5 && unmarshall_STRINGN (rbp, se, CA_MAXHOSTNAMELEN+1))
		RETURN (EINVAL);

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
			marshall_DIRX (&sbp, magic, fmd_entry);
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
		} else if (getattr == 4) {	/* readdirxc */
			cml = strlen (umd_entry->comments);
			marshall_DIRX (&sbp, magic, fmd_entry);
			marshall_STRING (sbp, umd_entry->comments);
			nbentries++;
			maxsize -= ((direntsz + fnl + cml + 9) / 8) * 8;
		} else {			/* readdirxr */
			bof = 1;
			while (1) {	/* loop on replicas */
				if (c = Cns_list_rep_entry (&thip->dbfd, bof,
				    fmd_entry->fileid, &rep_entry, 0, NULL,
				    0, &replistptr)) break;
				bof = 0;
				if (*se && strcmp (rep_entry.host, se))
					continue;
				n = marshall_DIRR (NULL, magic, &rep_entry);
				if (sbpr - repbuf + n > REPBUFSZ) {
					sendrep (thip->s, MSG_REPLICP, sbpr - repbuf, repbuf);
					sbpr = repbuf;
				}
				marshall_DIRR (&sbpr, magic, &rep_entry);
			}
			(void) Cns_list_rep_entry (&thip->dbfd, bof,
			    fmd_entry->fileid, &rep_entry, 0, NULL, 1, &replistptr);
			if (c < 0)
				RETURN (serrno);
			marshall_DIRXR (&sbp, magic, fmd_entry);
			nbentries++;
			maxsize -= ((direntsz + fnl + 8) / 8) * 8;
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
			marshall_DIRX (&sbp, magic, fmd_entry);
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
		} else if (getattr == 4) {	/* readdirxc */
			*umd_entry->comments = '\0';
			if (Cns_get_umd_by_fileid (&thip->dbfd, fmd_entry->fileid,
			    umd_entry, 0, NULL) && serrno != ENOENT)
				RETURN (serrno);
			cml = strlen (umd_entry->comments);
			if (fnl + cml > maxsize) break;
			marshall_DIRX (&sbp, magic, fmd_entry);
			marshall_STRING (sbp, umd_entry->comments);
			nbentries++;
			maxsize -= ((direntsz + fnl + cml + 9) / 8) * 8;
		} else {			/* readdirxr */
			if ((fmd_entry->filemode & S_IFLNK) == S_IFLNK) {
				strcpy (sav_name, fmd_entry->name);
				if (Cns_parsepath (&thip->dbfd, dir_fileid,
				    fmd_entry->name, uid, gid, clienthost,
				    NULL, NULL, fmd_entry, NULL, CNS_MUST_EXIST) == 0)
					strcpy (fmd_entry->name, sav_name);
			}
			if (fnl > maxsize) break;
			bof = 1;
			while (1) {	/* loop on replicas */
				if (c = Cns_list_rep_entry (&thip->dbfd, bof,
				    fmd_entry->fileid, &rep_entry, 0, NULL,
				    0, &replistptr)) break;
				bof = 0;
				if (*se && strcmp (rep_entry.host, se))
					continue;
				n = marshall_DIRR (NULL, magic, &rep_entry);
				if (sbpr - repbuf + n > REPBUFSZ) {
					sendrep (thip->s, MSG_REPLICP, sbpr - repbuf, repbuf);
					sbpr = repbuf;
				}
				marshall_DIRR (&sbpr, magic, &rep_entry);
			}
			(void) Cns_list_rep_entry (&thip->dbfd, bof,
			    fmd_entry->fileid, &rep_entry, 0, NULL, 1, &replistptr);
			if (c < 0)
				RETURN (serrno);
			marshall_DIRXR (&sbp, magic, fmd_entry);
			nbentries++;
			maxsize -= ((direntsz + fnl + 8) / 8) * 8;
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
	if (sbpr > repbuf)
		sendrep (thip->s, MSG_REPLICP, sbpr - repbuf, repbuf);
	marshall_WORD (sbp, eod);
	p = dirbuf;
	marshall_WORD (p, nbentries);		/* update nbentries in reply */
	sendrep (thip->s, MSG_DATA, sbp - dirbuf, dirbuf);
	RETURN (0);
}

/*	Cns_srv_readlink - read value of symbolic link */

Cns_srv_readlink(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	u_signed64 cwd;
	struct Cns_file_metadata filentry;
	char func[17];
	gid_t gid;
	struct Cns_symlinks lnk_entry;
	char logbuf[CA_MAXPATHLEN+10];
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "readlink %s", path);
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
	u_signed64 fileid;
	char func[16];
	gid_t gid;
	char logbuf[2*CA_MAXPATHLEN+9];
	int n;
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
	char *rbp;
	Cns_dbrec_addr rec_addrs;	/* segment record address */
	Cns_dbrec_addr rec_addru;	/* comment record address */
	struct Cns_file_replica rep_entry;
	struct Cns_seg_metadata smd_entry;
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
	sprintf (logbuf, "rename %s %s", oldpath, newpath);
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

	if (*old_fmd_entry.name == '/' || *new_fmd_entry.name == '/')	/* nsrename / */
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

	if (new_fmd_entry.fileid) {	/* 'new' basename entry exists already */
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

		if (strcmp (cmd, "nsdaemon")) {
			/* check if replicas exist */

			if ((c = Cns_list_rep_entry (&thip->dbfd, 1,
			    new_fmd_entry.fileid, &rep_entry, 0, NULL, 0,
			    &dblistptr)) == 0) {
				(void) Cns_list_rep_entry (&thip->dbfd, 0,
				   new_fmd_entry.fileid, &rep_entry, 0, NULL, 1,
				   &dblistptr);	/* free res */
				RETURN (EEXIST);
			}
			(void) Cns_list_rep_entry (&thip->dbfd, 0, new_fmd_entry.fileid,
			    &rep_entry, 0, NULL, 1, &dblistptr);	/* free res */
			if (c < 0)
				RETURN (serrno);
		}

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
		if (strcmp (cmd, "nsdaemon") == 0) {
			/* delete file segments if any */

			while ((c = Cns_get_smd_by_pfid (&thip->dbfd, bof,
			    new_fmd_entry.fileid, &smd_entry, 1, &rec_addrs, 0,
			    &dblistptr)) == 0) {
				if (Cns_delete_smd_entry (&thip->dbfd, &rec_addrs))
					RETURN (serrno);
				bof = 0;
			}
			(void) Cns_get_smd_by_pfid (&thip->dbfd, bof, new_fmd_entry.fileid,
			    &smd_entry, 1, &rec_addrs, 1, &dblistptr);	/* free res */
			if (c < 0)
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
	char func[30];
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

	strcpy (func, "Cns_srv_replaceseg");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	get_client_actual_id (thip, &uid, &gid, &user);
	nslogit (func, NS092, "replaceseg", user, uid, gid, clienthost);
        unmarshall_HYPER (rbp, fileid);
        unmarshall_WORD (rbp, copyno);
        unmarshall_WORD (rbp, fsec);
	sprintf (logbuf, "replaceseg %s %d %d",
	    u64tostr (fileid, tmpbuf, 0), copyno, fsec);
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



/*	Cns_srv_replacetapecopy - replace a tapecopy 
 *	
 * This function replaces a tapecopy by another one.
 * For compatibility reasons, it is also able to recieve
 * >1 segs for replacement, although the new stager policy 
 * is not to segment file anymore.
 * ! It deletes the old entries in the DB (no!! update to status 'D') !
 * FE, 05/2006
*/
Cns_srv_replacetapecopy(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	u_signed64 fileid = 0;
	int rc,checksum_ok, nboldsegs, nbseg ,copyno, bof, i;
	DBLISTPTR dblistptr;	//in fact an int
	int CA_MAXSEGS = 20;	// maximum number of segments of a file
	
	gid_t gid;
	uid_t uid;
	char *user;

	struct Cns_seg_metadata new_smd_entry[CA_MAXSEGS]; // the new entries
	struct Cns_seg_metadata old_smd_entry[CA_MAXSEGS]; // the old entries
	struct Cns_file_metadata filentry;
	

	Cns_dbrec_addr backup_rec_addr[CA_MAXSEGS];	// db keys of old segs
	Cns_dbrec_addr rec_addr;			// db key for file
	
	
	char *rbp;		// Pointer to recieve buffer
	
	char func[23];		// Name of the function
	char tmpbuf[21];
	char tmpbuf2[21];
	char logbuf[CA_MAXPATHLEN+34];
	char newvid[CA_MAXVIDLEN+1];
	char oldvid[CA_MAXVIDLEN+1];	
	
	
	/* --------------the header stuff ------------------ */

	strcpy (func, "Cns_srv_replacetapecopy");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	get_client_actual_id (thip, &uid, &gid, &user);
	nslogit (func, NS092, "replacetapecopy", user, uid, gid, clienthost);
	unmarshall_HYPER (rbp, fileid);
	if (unmarshall_STRINGN (rbp, newvid, CA_MAXVIDLEN+1))
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, oldvid, CA_MAXVIDLEN+1))
		RETURN (EINVAL);

	sprintf (logbuf, "for FileId %lld: %s->%s",fileid, oldvid, newvid);
	Cns_logreq (func, logbuf);

	/* check if the user is authorized to replace segment attributes */

	/* if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);
	*/
	
	
	/* start transaction */
	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* get/lock basename entry */
	if (Cns_get_fmd_by_fileid (&thip->dbfd, fileid, &filentry, 1, &rec_addr))
		RETURN (serrno);

	/* check if the entry is a regular file */
	if (filentry.filemode & S_IFDIR)
		RETURN (EISDIR);


	/* -------------- get the old segs for a file ------------------ */
	copyno = -1;		
	bof = 1;	/* first time: open cursor */
	nboldsegs = 0;
	while ((rc = Cns_get_smd_by_pfid (&thip->dbfd, bof,
                                          filentry.fileid, 
                                          &old_smd_entry[nboldsegs],
					  1, 
					  &backup_rec_addr[nboldsegs],
					  0, 
					  &dblistptr)) == 0 )
	{
		// we want only segments, which are on the oldvid !
		if ( strcmp( old_smd_entry[nboldsegs].vid,oldvid )==0 ){
			/* Store the copyno for first right segment.
			   we have to know it for the new segments */
			if (!nboldsegs){
				copyno = old_smd_entry[nboldsegs].copyno;
			}

			/** this marks the old segment as deleted in ns*/
			old_smd_entry[nboldsegs].s_status = 'D';
			nboldsegs++;
		}
		
		bof = 0;	/* no creation of cursor for next call */
		
		/* SHOULD NEVER HAPPEN !*/
		if (nboldsegs > CA_MAXSEGS ){ 
			sprintf (logbuf,"Abort! Too many segments for file %s.",
			  	u64tostr (fileid, tmpbuf, 0), nboldsegs );
			Cns_logreq (func, logbuf);
			RETURN (EINVAL);
		}
	}
		    
   	if (rc < 0){
		RETURN (serrno);
	}
   	
	if ( !nboldsegs || copyno == -1 ) {
		sprintf (logbuf, "Can't find old segs or copyno.Abort!%d,%d", nboldsegs, copyno);
		Cns_logreq (func, logbuf);
		RETURN (-1);
	}
	
	unmarshall_WORD(rbp, nbseg);
	sprintf (logbuf, "Replacing %d old Segments by %d from Stream, the tapecopyno is %d",
		nboldsegs, nbseg, copyno);
	Cns_logreq (func, logbuf);
	


	/* -------------- get the new segs from stream ------------------ */
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
       		}
		else {
			new_smd_entry[i].checksum_name[0] = '\0';
			new_smd_entry[i].checksum = 0;
		}
		
		if (new_smd_entry[i].checksum_name == NULL || 
			 strlen(new_smd_entry[i].checksum_name) == 0) {
			checksum_ok = 0;
		}
		else {
			checksum_ok = 1;
	    	}
		
		if (magic >= CNS_MAGIC4) {
				/* Checking that we can't have a NULL checksum name when a
					checksum is specified */
				if (!checksum_ok && new_smd_entry[i].checksum != 0) {
					sprintf (logbuf, "%s: NULL checksum name with non zero value, overriding", func);
					Cns_logreq (func, logbuf);
					new_smd_entry[i].checksum = 0;
				} 
		}
		
		sprintf (logbuf, "replaceseg %s %d %d %s %d %c %s %d %02x%02x%02x%02x %s:%x",
		    u64tostr (new_smd_entry[i].s_fileid, tmpbuf, 0), new_smd_entry[i].copyno,
		    new_smd_entry[i].fsec, u64tostr (new_smd_entry[i].segsize, tmpbuf2, 0),
		    new_smd_entry[i].compression, new_smd_entry[i].s_status, new_smd_entry[i].vid,
		    new_smd_entry[i].fseq, new_smd_entry[i].blockid[0], new_smd_entry[i].blockid[1],
                    new_smd_entry[i].blockid[2], new_smd_entry[i].blockid[3],
                    new_smd_entry[i].checksum_name, new_smd_entry[i].checksum);
		Cns_logreq (func, logbuf);
		
		
	}	// end for(..)
	
	

	
	
	/* -------------- remove old segs ------------------ */
	for (i=0; i< nboldsegs; i++){
		
		if (Cns_delete_smd_entry (&thip->dbfd,
		                      &backup_rec_addr[i])  )
		{
			sprintf (logbuf,"%s",sstrerror(serrno));
			Cns_logreq (func, logbuf);
			RETURN (serrno);
		}
	}
	
	/* -------------- insert new segs ------------------ */
	for (i=0; i< nbseg; i++){
		/* insert new file segment entry */
		if (Cns_insert_smd_entry (&thip->dbfd,&new_smd_entry[i])){
			sprintf (logbuf,"%s",sstrerror(serrno));
			Cns_logreq (func, logbuf);
			RETURN (serrno);
		}
	}
	
	RETURN (0);
}






/*      Cns_srv_rmdir - remove a directory entry */
 
Cns_srv_rmdir(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
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
	sprintf (logbuf, "rmdir %s", path);
	Cns_logreq (func, logbuf);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* check parent directory components for write/search permission and
	   get/lock basename entry */

	if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
	    &parent_dir, &rec_addrp, &direntry, &rec_addr, CNS_MUST_EXIST|CNS_NOFOLLOW))
		RETURN (serrno);

	if (*direntry.name == '/')	/* Cns_rmdir / */
		RETURN (EINVAL);

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

/*      Cns_srv_setacl - set the Access Control List for a file/directory */

Cns_srv_setacl(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
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
	char logbuf[CA_MAXPATHLEN+8];
	int nentries;
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "setacl %s", path);
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
				fmd_entry.filemode = fmd_entry.filemode & 0177077 |
					(aclp->a_perm << 6);
				break;
			case CNS_ACL_GROUP_OBJ:
				fmd_entry.filemode = fmd_entry.filemode & 0177707 |
					(aclp->a_perm << 3);
				break;
			case CNS_ACL_OTHER:
				fmd_entry.filemode = fmd_entry.filemode & 0177770 |
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
				fmd_entry.filemode = fmd_entry.filemode & 0177077 |
					(aclp->a_perm << 6);
				break;
			case CNS_ACL_GROUP_OBJ:
				fmd_entry.filemode = fmd_entry.filemode & 0177707 |
					(aclp->a_perm << 3);
				break;
			case CNS_ACL_MASK:
				fmd_entry.filemode = (fmd_entry.filemode & ~070) |
				    (fmd_entry.filemode & (aclp->a_perm << 3));
				break;
			case CNS_ACL_OTHER:
				fmd_entry.filemode = fmd_entry.filemode & 0177770 |
					(aclp->a_perm);
				break;
			}
		}
	}

	if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &fmd_entry))
		RETURN (serrno);
	RETURN (0);
}

/*	Cns_srv_setatime - set last access time */

Cns_srv_setatime(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	u_signed64 cwd;
	u_signed64 fileid;
	struct Cns_file_metadata filentry;
	char func[17];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+31];
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "setatime %s %s", u64tostr (fileid, tmpbuf, 0), path);
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

/*	Cns_srv_setcomment - add/replace a comment associated with a file/directory */

Cns_srv_setcomment(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char comment[CA_MAXCOMMENTLEN+1];
	u_signed64 cwd;
	struct Cns_file_metadata filentry;
	char func[19];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+12];
	struct Cns_user_metadata old_umd_entry;
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "setcomment %s", path);
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
	u_signed64 cwd;
	u_signed64 fileid;
	struct Cns_file_metadata filentry;
	u_signed64 filesize;
	char func[17];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+52];
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addr;
	char tmpbuf[21];
	char tmpbuf2[21];
	uid_t uid;
	char *user;

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
	sprintf (logbuf, "setfsize %s %s %s", u64tostr (fileid, tmpbuf, 0),
	    path, u64tostr (filesize, tmpbuf2, 0));
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

/*	Cns_srv_setfsizeg - set file size and last modification time */

Cns_srv_setfsizeg(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char csumtype[3];
	char csumvalue[33];
	struct Cns_file_metadata filentry;
	u_signed64 filesize;
	char func[18];
	gid_t gid;
	char guid[CA_MAXGUIDLEN+1];
	char logbuf[CA_MAXGUIDLEN+32];
	char *rbp;
	Cns_dbrec_addr rec_addr;
	char tmpbuf[21];
	uid_t uid;
	char *user;

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
	if (unmarshall_STRINGN (rbp, csumvalue, 33))
		RETURN (EINVAL);
	if (*csumtype && strcmp (csumtype, "CS") && strcmp (csumtype, "AD") &&
	    strcmp (csumtype, "MD"))
		RETURN (EINVAL);
	sprintf (logbuf, "setfsizeg %s %s", guid, u64tostr (filesize, tmpbuf, 0));
	Cns_logreq (func, logbuf);
 
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

	/* update entry */

	filentry.filesize = filesize;
	filentry.mtime = time (0);
	filentry.ctime = filentry.mtime;
	strcpy (filentry.csumtype, csumtype);
	strcpy (filentry.csumvalue, csumvalue);

	if (Cns_update_fmd_entry (&thip->dbfd, &rec_addr, &filentry))
		RETURN (serrno);
	RETURN (0);
}

/*	Cns_srv_setptime - set replica pin time */

Cns_srv_setptime(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	struct Cns_file_metadata filentry;
	char func[17];
	gid_t gid;
	char logbuf[CA_MAXSFNLEN+21];
	time_t ptime;
	char *rbp;
	Cns_dbrec_addr rec_addr;
	struct Cns_file_replica rep_entry;
	char sfn[CA_MAXSFNLEN+1];
	uid_t uid;
	char *user;

	strcpy (func, "Cns_srv_setptime");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	get_client_actual_id (thip, &uid, &gid, &user);
	nslogit (func, NS092, "setptime", user, uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, sfn, CA_MAXSFNLEN+1))
		RETURN (SENAMETOOLONG);
	unmarshall_TIME_T (rbp, ptime);
	sprintf (logbuf, "setptime %s %d", sfn, ptime);
	Cns_logreq (func, logbuf);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);
 
	if (Cns_get_rep_by_sfn (&thip->dbfd, sfn, &rep_entry, 1, &rec_addr))
                RETURN (serrno);

	/* get basename entry */

	if (Cns_get_fmd_by_fileid (&thip->dbfd, rep_entry.fileid,
	    &filentry, 0, NULL))
		RETURN (serrno);

	/* check parent directory components for search permission */

	if (Cns_chkbackperm (&thip->dbfd, filentry.parent_fileid,
	    S_IEXEC, uid, gid, clienthost))
		RETURN (serrno);

	/* check if the user is authorized to set pin time for this entry */

	if (uid != filentry.uid &&
	    Cns_chkentryperm (&filentry, S_IREAD, uid, gid, clienthost))
		RETURN (EACCES);

	/* update entry */

	rep_entry.ptime = ptime;

	if (Cns_update_rep_entry (&thip->dbfd, &rec_addr, &rep_entry))
		RETURN (serrno);
	RETURN (0);
}

/*	Cns_srv_setratime - set replica last access time */

Cns_srv_setratime(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	struct Cns_file_metadata filentry;
	char func[18];
	gid_t gid;
	char logbuf[CA_MAXSFNLEN+11];
	char *rbp;
	Cns_dbrec_addr rec_addr;
	struct Cns_file_replica rep_entry;
	char sfn[CA_MAXSFNLEN+1];
	uid_t uid;
	char *user;

	strcpy (func, "Cns_srv_setratime");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	get_client_actual_id (thip, &uid, &gid, &user);
	nslogit (func, NS092, "setratime", user, uid, gid, clienthost);
	if (rdonly)
		RETURN (EROFS);
	if (unmarshall_STRINGN (rbp, sfn, CA_MAXSFNLEN+1))
		RETURN (SENAMETOOLONG);
	sprintf (logbuf, "setratime %s", sfn);
	Cns_logreq (func, logbuf);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);
 
	if (Cns_get_rep_by_sfn (&thip->dbfd, sfn, &rep_entry, 1, &rec_addr))
                RETURN (serrno);

	/* get basename entry */

	if (Cns_get_fmd_by_fileid (&thip->dbfd, rep_entry.fileid,
	    &filentry, 0, NULL))
		RETURN (serrno);

	/* check parent directory components for search permission */

	if (Cns_chkbackperm (&thip->dbfd, filentry.parent_fileid,
	    S_IEXEC, uid, gid, clienthost))
		RETURN (serrno);

	/* check if the user is authorized to set access time for this entry */

	if (uid != filentry.uid &&
	    Cns_chkentryperm (&filentry, S_IREAD, uid, gid, clienthost))
		RETURN (EACCES);

	/* update entry */

	rep_entry.atime = time (0);
	rep_entry.nbaccesses++;

	if (Cns_update_rep_entry (&thip->dbfd, &rec_addr, &rep_entry))
		RETURN (serrno);
	RETURN (0);
}

/*	Cns_srv_setrstatus - set replica status */

Cns_srv_setrstatus(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	struct Cns_file_metadata filentry;
	char func[19];
	gid_t gid;
	char logbuf[CA_MAXSFNLEN+14];
	char *rbp;
	Cns_dbrec_addr rec_addr;
	struct Cns_file_replica rep_entry;
	char sfn[CA_MAXSFNLEN+1];
	char status;
	uid_t uid;
	char *user;

	strcpy (func, "Cns_srv_setrstatus");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	get_client_actual_id (thip, &uid, &gid, &user);
	nslogit (func, NS092, "setrstatus", user, uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, sfn, CA_MAXSFNLEN+1))
		RETURN (SENAMETOOLONG);
	unmarshall_BYTE (rbp, status);
	sprintf (logbuf, "setrstatus %s %c", sfn, status);
	Cns_logreq (func, logbuf);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);
 
	if (Cns_get_rep_by_sfn (&thip->dbfd, sfn, &rep_entry, 1, &rec_addr))
                RETURN (serrno);

	/* get basename entry */

	if (Cns_get_fmd_by_fileid (&thip->dbfd, rep_entry.fileid,
	    &filentry, 0, NULL))
		RETURN (serrno);

	/* check parent directory components for search permission */

	if (Cns_chkbackperm (&thip->dbfd, filentry.parent_fileid,
	    S_IEXEC, uid, gid, clienthost))
		RETURN (serrno);

	/* check if the user is authorized to set access time for this entry */

	if (uid != filentry.uid &&
	    Cns_chkentryperm (&filentry, S_IREAD, uid, gid, clienthost))
		RETURN (EACCES);

	/* update entry */

	rep_entry.status = status;

	if (Cns_update_rep_entry (&thip->dbfd, &rec_addr, &rep_entry))
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
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addr;
	Cns_dbrec_addr rec_addrs;
	struct Cns_seg_metadata smd_entry;
	char tmpbuf[21];
	char tmpbuf2[21];
	uid_t uid;
	char *user;

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
	unmarshall_WORD (rbp, nbseg);
	sprintf (logbuf, "setsegattrs %s %s",
	    u64tostr (fileid, tmpbuf, 0), path);
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
	char *rbp;
	uid_t uid;
	char *user;

	strcpy (func, "Cns_srv_shutdown");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	get_client_actual_id (thip, &uid, &gid, &user);
	nslogit (func, NS092, "shutdown", user, uid, gid, clienthost);
	unmarshall_WORD (rbp, force);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	being_shutdown = force + 1;
	RETURN (0);
}

/*	Cns_srv_startsess - start session */

Cns_srv_startsess(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char comment[CA_MAXCOMMENTLEN+1];
	char func[18];
	gid_t gid;
	char logbuf[CA_MAXCOMMENTLEN+13];
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

/*	Cns_srv_starttrans - start transaction mode */

Cns_srv_starttrans(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	char comment[CA_MAXCOMMENTLEN+1];
	char func[19];
	gid_t gid;
	char logbuf[CA_MAXCOMMENTLEN+14];
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

/*	Cns_srv_stat - get information about a file or a directory */

Cns_srv_stat(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	u_signed64 cwd;
	u_signed64 fileid;
	struct Cns_file_metadata fmd_entry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+6];
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "stat %s %s", u64tostr(fileid, tmpbuf, 0), path);
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

/*	Cns_srv_statg - get information about a file or a directory */

Cns_srv_statg(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	u_signed64 cwd;
	u_signed64 fileid;
	struct Cns_file_metadata fmd_entry;
	char func[16];
	gid_t gid;
	char guid[CA_MAXGUIDLEN+1];
	char logbuf[CA_MAXPATHLEN+CA_MAXGUIDLEN+8];
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "statg %s %s", path, guid);
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

/*	Cns_srv_statr - get information about a replica */

Cns_srv_statr(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	struct Cns_file_metadata fmd_entry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXSFNLEN+7];
	char *rbp;
	struct Cns_file_replica rep_entry;
	char repbuf[94];
	char *sbp;
	char sfn[CA_MAXSFNLEN+1];
	uid_t uid;
	char *user;

	strcpy (func, "Cns_srv_statr");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	get_client_actual_id (thip, &uid, &gid, &user);
	nslogit (func, NS092, "statr", user, uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, sfn, CA_MAXSFNLEN+1))
		RETURNQ (SENAMETOOLONG);
	sprintf (logbuf, "statr %s", sfn);
	Cns_logreq (func, logbuf);

	if (Cns_get_rep_by_sfn (&thip->dbfd, sfn, &rep_entry, 0, NULL))
		RETURNQ (serrno);

	/* get basename entry */

	if (Cns_get_fmd_by_fileid (&thip->dbfd, rep_entry.fileid, &fmd_entry,
	    0, NULL))
		RETURNQ (serrno);

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
 
Cns_srv_symlink(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	u_signed64 cwd;
	char func[16];
	gid_t gid;
	char logbuf[2*CA_MAXPATHLEN+10];
	struct Cns_file_metadata fmd_entry;
	char linkname[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "symlink %s %s", target, linkname);
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
 
Cns_srv_undelete(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
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
	struct Cns_file_replica rep_entry;
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
	sprintf (logbuf, "undelete %s", path);
	Cns_logreq (func, logbuf);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* check parent directory components for write/search permission and
	   get/lock basename entry */

	if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
	    &parent_dir, &rec_addrp, &filentry, &rec_addr, CNS_MUST_EXIST|CNS_NOFOLLOW))
		RETURN (serrno);

	if (*filentry.name == '/')	/* Cns_undelete / */
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

	if (strcmp (cmd, "nsdaemon")) {
		/* remove the mark "logically deleted" on the file replicas if any */

		while ((c = Cns_list_rep_entry (&thip->dbfd, bof, filentry.fileid,
		    &rep_entry, 1, &rec_addrs, 0, &dblistptr)) == 0) {
			rep_entry.status = '-';
			if (Cns_update_rep_entry (&thip->dbfd, &rec_addrs, &rep_entry))
				RETURN (serrno);
			bof = 0;
		}
		(void) Cns_list_rep_entry (&thip->dbfd, bof, filentry.fileid,
		    &rep_entry, 1, &rec_addrs, 1, &dblistptr);	/* free res */
		if (c < 0)
			RETURN (serrno);
	} else {
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
	}

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
	int bof = 1;
	int c;
	u_signed64 cwd;
	DBLISTPTR dblistptr;
	struct Cns_file_metadata filentry;
	char func[16];
	gid_t gid;
	struct Cns_symlinks lnk_entry;
	char logbuf[CA_MAXPATHLEN+8];
	struct Cns_file_metadata parent_dir;
	char path[CA_MAXPATHLEN+1];
	char *rbp;
	Cns_dbrec_addr rec_addr;	/* file record address */
	Cns_dbrec_addr rec_addrl;	/* link record address */
	Cns_dbrec_addr rec_addrp;	/* parent record address */
	Cns_dbrec_addr rec_addrs;	/* segment record address */
	Cns_dbrec_addr rec_addru;	/* comment record address */
	struct Cns_file_replica rep_entry;
	struct Cns_seg_metadata smd_entry;
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
	sprintf (logbuf, "unlink %s", path);
	Cns_logreq (func, logbuf);

	/* start transaction */

	(void) Cns_start_tr (thip->s, &thip->dbfd);

	/* check parent directory components for write/search permission and
	   get/lock basename entry */

	if (Cns_parsepath (&thip->dbfd, cwd, path, uid, gid, clienthost,
	    &parent_dir, &rec_addrp, &filentry, &rec_addr, CNS_MUST_EXIST|CNS_NOFOLLOW))
		RETURN (serrno);

	if (*filentry.name == '/')	/* Cns_unlink / */
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

		if (strcmp (cmd, "nsdaemon")) {
			/* check if replicas still exit */

			if ((c = Cns_list_rep_entry (&thip->dbfd, 1, filentry.fileid,
			    &rep_entry, 0, NULL, 0, &dblistptr)) == 0) {
				(void) Cns_list_rep_entry (&thip->dbfd, 0, filentry.fileid,
				    &rep_entry, 0, NULL, 1, &dblistptr);	/* free res */
				RETURN (EEXIST);
			}
			(void) Cns_list_rep_entry (&thip->dbfd, 0, filentry.fileid,
			    &rep_entry, 0, NULL, 1, &dblistptr);	/* free res */
			if (c < 0)
				RETURN (serrno);
		} else {
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
		}

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

/*	Cns_srv_utime - set last access and modification times */

Cns_srv_utime(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct Cns_srv_thread_info *thip;
{
	time_t actime;
	u_signed64 cwd;
	struct Cns_file_metadata filentry;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXPATHLEN+19];
	time_t modtime;
	char path[CA_MAXPATHLEN+1];
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
	sprintf (logbuf, "utime %s %d", path, user_specified_time);
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

	/* Routines for identity mapping */

extern char lcgdmmapfile[];

Cns_vo_from_dn(const char *dn, char *vo)
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
		while (*p == ' ' || *p == '\t')	/* skip leading blanks */
			p++;
		if (*p == '\0') continue;	/* empty line */
		if (*p == '#') continue;	/* comment */
		if (*p == '"') {
			q = p + 1;
			if ((p = strrchr (q, '"')) == NULL) continue;
		} else {
			q = p;
			while (*p !=  ' ' && *p != '\t' && *p != '\0')
				p++;
			if (*p == '\0') continue;	/* no vo */
		}
		*p = '\0';
		if (strcmp (dn, q)) continue;	/* DN does not match */
		p++;
		while (*p == ' ' || *p == '\t')	/* skip blanks between dn and vo */
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

/*	Cns_srv_entergrpmap - define a new group entry in Virtual Id table */

Cns_srv_entergrpmap(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost; 
struct Cns_srv_thread_info *thip;
{
	char func[20];
	gid_t gid;
	struct Cns_groupinfo group_entry;
	char logbuf[278];
	char *rbp;
	Cns_dbrec_addr rec_addr;
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

/*	Cns_srv_enterusrmap - define a new user entry in Virtual Id table */

Cns_srv_enterusrmap(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost; 
struct Cns_srv_thread_info *thip;
{
	char func[20];
	gid_t gid;
	char logbuf[278];
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

/*	Cns_srv_getidmap - get uid/gids associated with a given dn/roles */

getonegid(dbfd, groupname, gid)
struct Cns_dbfd *dbfd;
char *groupname;
gid_t *gid;
{
	int c;
	struct Cns_groupinfo group_entry;

	if ((c = Cns_get_grpinfo_by_name (dbfd, groupname,
	    &group_entry, 0, NULL)) && serrno != ENOENT)
		return (serrno);
	if (c) {	/* must create an entry */
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

getidmap(dbfd, username, nbgroups, groupnames, userid, gids)
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
	if (c) {	/* must create an entry */
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

	if (groupnames == NULL) {	/* not using VOMS */
		if ((c = Cns_vo_from_dn (username, vo)))
			return (c);
		return (getonegid (dbfd, vo, &gids[0]));
	}
	for (i = 0; i < nbgroups; i++) {
		groupname = groupnames[i];
		if (*groupname == '/')
			groupname++;	/* skipping leading slash */
		if ((p = strstr (groupname, "/Role=NULL")))
			*p = '\0';
		else if ((p = strstr (groupname, "/Capability=NULL")))
			*p = '\0';
		if ((c = getonegid (dbfd, groupname, &gids[i])))
			return (c);
	}
	return (0);
}

Cns_srv_getidmap(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost; 
struct Cns_srv_thread_info *thip;
{
	int c;
	char func[17];
	gid_t gid;
	gid_t *gids;
	char **groupnames = NULL;
	int i;
	char logbuf[265];
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
	if (nbgroups) {	/* using VOMS */
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
	} else
		nbgroups = 1;
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

/*	Cns_srv_getgrpbygid - get group name associated with a given gid */

Cns_srv_getgrpbygid(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost; 
struct Cns_srv_thread_info *thip;
{
	char func[20];
	gid_t gid;
	struct Cns_groupinfo group_entry;
	char logbuf[23];
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

	if (Cns_get_grpinfo_by_gid (&thip->dbfd, reqgid, &group_entry, 0, NULL) < 0)
		if (serrno == ENOENT) {
			sendrep (thip->s, MSG_ERR, "No such gid\n");
			RETURNQ (EINVAL);
		} else
			RETURNQ (serrno);
	sbp = repbuf;
	marshall_STRING (sbp, group_entry.groupname);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURNQ (0);
}

/*	Cns_srv_getgrpbynam - get gid associated with a given group name */

Cns_srv_getgrpbynam(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost; 
struct Cns_srv_thread_info *thip;
{
	char func[20];
	gid_t gid;
	struct Cns_groupinfo group_entry;
	char groupname[256];
	char logbuf[268];
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

	if (Cns_get_grpinfo_by_name (&thip->dbfd, groupname, &group_entry, 0,
	    NULL) < 0)
		if (serrno == ENOENT) {
			sendrep (thip->s, MSG_ERR, "No such group\n");
			RETURNQ (EINVAL);
		} else
			RETURNQ (serrno);
	sbp = repbuf;
	marshall_LONG (sbp, group_entry.gid);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURNQ (0);
}

/*	Cns_srv_getusrbynam - get uid associated with a given user name */

Cns_srv_getusrbynam(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost; 
struct Cns_srv_thread_info *thip;
{
	char func[20];
	gid_t gid;
	char logbuf[268];
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

	if (Cns_get_usrinfo_by_name (&thip->dbfd, username, &user_entry, 0,
	    NULL) < 0)
		if (serrno == ENOENT) {
			sendrep (thip->s, MSG_ERR, "No such user\n");
			RETURNQ (EINVAL);
		} else
			RETURNQ (serrno);
	sbp = repbuf;
	marshall_LONG (sbp, user_entry.userid);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURNQ (0);
}

/*	Cns_srv_getusrbyuid - get user name associated with a given uid */

Cns_srv_getusrbyuid(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost; 
struct Cns_srv_thread_info *thip;
{
	char func[20];
	gid_t gid;
	char logbuf[23];
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

	if (Cns_get_usrinfo_by_uid (&thip->dbfd, requid, &user_entry, 0, NULL) < 0)
		if (serrno == ENOENT) {
			sendrep (thip->s, MSG_ERR, "No such uid\n");
			RETURNQ (EINVAL);
		} else
			RETURNQ (serrno);
	sbp = repbuf;
	marshall_STRING (sbp, user_entry.username);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURNQ (0);
}

/*	Cns_srv_modgrpmap - modify group name associated with a given gid */

Cns_srv_modgrpmap(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost; 
struct Cns_srv_thread_info *thip;
{
	char func[18];
	gid_t gid;
	struct Cns_groupinfo group_entry;
	char groupname[256];
	char logbuf[277];
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

	if (Cns_get_grpinfo_by_gid (&thip->dbfd, reqgid, &group_entry, 1, &rec_addr) < 0)
		if (serrno == ENOENT) {
			sendrep (thip->s, MSG_ERR, "No such gid\n");
			RETURN (EINVAL);
		} else
			RETURN (serrno);
	strcpy (group_entry.groupname, groupname);
	if (Cns_update_group_entry (&thip->dbfd, &rec_addr, &group_entry))
		RETURN (serrno);
	RETURN (0);
}

/*	Cns_srv_modusrmap - modify user name associated with a given uid */

Cns_srv_modusrmap(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost; 
struct Cns_srv_thread_info *thip;
{
	char func[18];
	gid_t gid;
	char logbuf[277];
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

	if (Cns_get_usrinfo_by_uid (&thip->dbfd, requid, &user_entry, 1, &rec_addr) < 0)
		if (serrno == ENOENT) {
			sendrep (thip->s, MSG_ERR, "No such uid\n");
			RETURN (EINVAL);
		} else
			RETURN (serrno);
	strcpy (user_entry.username, username);
	if (Cns_update_user_entry (&thip->dbfd, &rec_addr, &user_entry))
		RETURN (serrno);
	RETURN (0);
}

/*	Cns_srv_rmgrpmap - suppress group entry corresponding to a given gid/name */

Cns_srv_rmgrpmap(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost; 
struct Cns_srv_thread_info *thip;
{
	char func[18];
	gid_t gid;
	struct Cns_groupinfo group_entry;
	char groupname[256];
	char logbuf[276];
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
		    1, &rec_addr) < 0)
			if (serrno == ENOENT) {
				sendrep (thip->s, MSG_ERR, "No such gid\n");
				RETURN (EINVAL);
			} else
				RETURN (serrno);
		if (*groupname && strcmp (groupname, group_entry.groupname))
			RETURN (EINVAL);
	} else {
		if (Cns_get_grpinfo_by_name (&thip->dbfd, groupname, &group_entry,
		    1, &rec_addr) < 0)
			if (serrno == ENOENT) {
				sendrep (thip->s, MSG_ERR, "No such group\n");
				RETURN (EINVAL);
			} else
				RETURN (serrno);
	}
	if (Cns_delete_group_entry (&thip->dbfd, &rec_addr))
		RETURN (serrno);
	RETURN (0);
}

/*	Cns_srv_rmusrmap - suppress user entry corresponding to a given uid/name */

Cns_srv_rmusrmap(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost; 
struct Cns_srv_thread_info *thip;
{
	char func[18];
	gid_t gid;
	char logbuf[276];
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
		    1, &rec_addr) < 0)
			if (serrno == ENOENT) {
				sendrep (thip->s, MSG_ERR, "No such uid\n");
				RETURN (EINVAL);
			} else
				RETURN (serrno);
		if (*username && strcmp (username, user_entry.username))
			RETURN (EINVAL);
	} else {
		if (Cns_get_usrinfo_by_name (&thip->dbfd, username, &user_entry,
		    1, &rec_addr) < 0)
			if (serrno == ENOENT) {
				sendrep (thip->s, MSG_ERR, "No such user\n");
				RETURN (EINVAL);
			} else
				RETURN (serrno);
	}
	if (Cns_delete_user_entry (&thip->dbfd, &rec_addr))
		RETURN (serrno);
	RETURN (0);
}
