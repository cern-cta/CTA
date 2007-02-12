/*
 * Copyright (C) 1999-2007 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#if ! defined(_WIN32)
#include <unistd.h>
#endif
#include "Cns.h"
#include "Cns_server.h"
#include "Cupv_api.h"
#include "serrno.h"
extern char localhost[CA_MAXHOSTNAMELEN+1];

/*	Cns_chkbackperm - check permissions backward */

Cns_chkbackperm(dbfd, fileid, mode, uid, gid, clienthost)
struct Cns_dbfd *dbfd;
u_signed64 fileid;
int mode;
uid_t uid;
gid_t gid;
char *clienthost;
{
	u_signed64 cur_fileid = fileid;
	struct Cns_file_metadata fmd_entry;

	while (cur_fileid != 2) {
		if (Cns_get_fmd_by_fileid (dbfd, cur_fileid, &fmd_entry, 0, NULL))
			return (-1);
		
		if ((fmd_entry.filemode & S_IFMT) != S_IFDIR) {
			serrno = ENOTDIR;
			return (-1);
		}
		if  (Cns_chkentryperm (&fmd_entry, S_IEXEC, uid, gid, clienthost))
			return (-1);
		cur_fileid = fmd_entry.parent_fileid;
	}
	return (0);
}

/*	Cns_parsepath - parse a path, resolving symbolic links if any */

Cns_parsepath(dbfd, cwd, path, uid, gid, clienthost, parent_dir, rec_addrp, base_entry, rec_addr, flags)
struct Cns_dbfd *dbfd;
u_signed64 cwd;
char *path;
uid_t uid;
gid_t gid;
char *clienthost;
struct Cns_file_metadata *parent_dir;
Cns_dbrec_addr *rec_addrp;
struct Cns_file_metadata *base_entry;
Cns_dbrec_addr *rec_addr;
int flags;
{
	int c;
	char *component;
	int first_get_done = 0;
	struct Cns_file_metadata fmd_entry;
	int l1, l2;
	struct Cns_symlinks lnk_entry;
	int nblinks = 0;
	char *p;
	u_signed64 parent_fileid;

	if (! cwd && *path != '/') {
		serrno = EINVAL;
		return (-1);
	}
next:

	/* silently remove trailing slashes */

	p = path + strlen (path) - 1;
	while (p > path && *p == '/')
		*p-- = '\0';

	component = path;
	if (*path == '/' && *(path+1) == '\0') { 	/* path == "/" */
		fmd_entry.fileid = 0;
	} else {
		fmd_entry.fileid = cwd;

		/* loop on all components of the path name */

		for (p = path; *p; p++) {
			if (*p != '/') continue;
			if (p != path) {
				if (p == component) {	/* 2 consecutive slashes */
					component = p + 1;
					continue;
				}
				if (*component == '.' && p == (component + 1)) {
					component = p + 1;
					continue;
				}
				*p = '\0';
				if (strcmp (component, "..") == 0) {
					*p = '/';
					if (! first_get_done)	/* path starts with "../" */
						if (Cns_get_fmd_by_fileid (dbfd,
						    fmd_entry.fileid,
						    &fmd_entry, 0, NULL))
							return (-1);
					if (fmd_entry.parent_fileid)
						c = Cns_get_fmd_by_fileid (dbfd,
						    fmd_entry.parent_fileid,
						    &fmd_entry, 0, NULL);
					else
						c = 0;	/* ignore ".." if at root directory */
				} else {
					if (strlen (component) > CA_MAXNAMELEN) {
						*p = '/';
						serrno = SENAMETOOLONG;
						return (-1);
					}
					c = Cns_get_fmd_by_fullid (dbfd, fmd_entry.fileid,
					    component, &fmd_entry, 0, NULL);
					*p = '/';
				}
			} else			/* path starts with "/" */
				c = Cns_get_fmd_by_fullid (dbfd, (u_signed64) 0,
				    "/", &fmd_entry, 0, NULL);
			if (c)
				return (-1);
			
			if ((fmd_entry.filemode & S_IFMT) == S_IFLNK) {
				if (++nblinks > CA_MAXSYMLINKS) {
					serrno = SELOOP;
					return (-1);
				}
				if (Cns_get_lnk_by_fileid (dbfd, fmd_entry.fileid,
				    &lnk_entry, 0, NULL))
					return (-1);
				if ((l1 = strlen (lnk_entry.linkname)) + (l2 = strlen (p)) > CA_MAXPATHLEN) {
					serrno = SENAMETOOLONG;
					return (-1);
				}
				if (path + l1 != p)
					memmove (path + l1, p, l2 + 1);
				memcpy (path, lnk_entry.linkname, l1);
				goto next;
			} if ((fmd_entry.filemode & S_IFMT) != S_IFDIR) {
				serrno = ENOTDIR;
				return (-1);
			}
			if  (Cns_chkentryperm (&fmd_entry, S_IEXEC, uid, gid, clienthost))
				return (-1);
			component = p + 1;
		}
	}

	/* check last component of the path name */

	if (strcmp (component, "..") == 0) {
		if (! first_get_done)		/* path == ".." */
			if (Cns_get_fmd_by_fileid (dbfd, fmd_entry.fileid,
			    &fmd_entry, 0, NULL))
				return (-1);
		if (fmd_entry.parent_fileid) {
			if (Cns_get_fmd_by_fileid (dbfd, fmd_entry.parent_fileid,
			    &fmd_entry, rec_addr ? 1 : 0, rec_addr))
				return (-1);
		} else if (rec_addr)
			if (Cns_get_fmd_by_fileid (dbfd, fmd_entry.fileid,
			    &fmd_entry, 1, rec_addr))
				return (-1);
	} else if (*component && strcmp (component, ".")) {
		if (strlen (component) > CA_MAXNAMELEN) {
			serrno = SENAMETOOLONG;
			return (-1);
		}
		if (Cns_get_fmd_by_fullid (dbfd, fmd_entry.fileid, component,
		    &fmd_entry, 0, NULL)) {
			if (serrno != ENOENT || flags & CNS_MUST_EXIST)
				return (-1);
			parent_fileid = fmd_entry.fileid;
			memset (&fmd_entry, 0, sizeof(fmd_entry));
			fmd_entry.parent_fileid = parent_fileid;
			strcpy (fmd_entry.name, component);
		} else if ((fmd_entry.filemode & S_IFMT) == S_IFLNK &&
		    (flags & CNS_NOFOLLOW) == 0) {
			if (++nblinks > CA_MAXSYMLINKS) {
				serrno = SELOOP;
				return (-1);
			}
			if (Cns_get_lnk_by_fileid (dbfd, fmd_entry.fileid,
			    &lnk_entry, 0, NULL))
				return (-1);
			strcpy (path, lnk_entry.linkname);
			goto next;
		} else if (rec_addr)	/* must lock last component */
			if (Cns_get_fmd_by_fileid (dbfd, fmd_entry.fileid,
			    &fmd_entry, 1, rec_addr))
				return (-1);
	} else {
		if (! first_get_done ||		/* path == "." or "" */
		    rec_addr)
			if (Cns_get_fmd_by_fileid (dbfd, fmd_entry.fileid,
			    &fmd_entry, rec_addr ? 1 : 0, rec_addr))
				return (-1);
	}

	/* lock and return parent if requested */

	if (rec_addrp && fmd_entry.parent_fileid) {
		if (Cns_get_fmd_by_fileid (dbfd, fmd_entry.parent_fileid,
		    parent_dir, 1, rec_addrp))
			return (-1);
		if  (Cns_chkentryperm (parent_dir, S_IEXEC|S_IWRITE, uid, gid, clienthost))
			return (-1);
	}
	memcpy (base_entry, &fmd_entry, sizeof(fmd_entry));
	return (0);
}

/*	Cns_chkentryperm - check permissions in a given directory component */

Cns_chkentryperm(fmd_entry, mode, uid, gid, clienthost)
struct Cns_file_metadata *fmd_entry;
int mode;
uid_t uid;
gid_t gid;
char *clienthost;
{
	if (*fmd_entry->acl == 0) {	/* No extended ACL */
		if (fmd_entry->uid != uid) {
			mode >>= 3;
			if (fmd_entry->gid != gid)
				mode >>= 3;
		}
		if ((fmd_entry->filemode & mode) == mode)
			return (0);
	} else {
		if (Cns_chkaclperm (fmd_entry, mode, uid, gid) == 0)
			return (0);
	}
	return (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN));
}
