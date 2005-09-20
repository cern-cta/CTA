/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgr_procreq.c,v $ $Revision: 1.5 $ $Date: 2005/09/20 12:00:07 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */
 
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/types.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "Cthread_api.h"
#include "Cupv_api.h"
#include "marshall.h"
#include "serrno.h"
#include "u64subr.h"
#include "vmgr.h"
#include "vmgr_server.h"

extern int being_shutdown;
extern char localhost[CA_MAXHOSTNAMELEN+1];

#define RESETID(UID,GID) resetid(&UID, &GID, thip);
 
void resetid(uid_t *u, gid_t *g, struct vmgr_srv_thread_info *thip) {
#ifdef CSEC
  if (thip->Csec_service_type < 0) {
    *u = thip->Csec_uid;
    *g = thip->Csec_gid;
  }
#endif
}

/*	vmgr_logreq - log a request */

/*	Split the message into lines so they don't exceed LOGBUFSZ-1 characters
 *	A backslash is appended to a line to be continued
 *	A continuation line is prefixed by '+ '
 */
void
vmgr_logreq(func, logbuf)
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
		vmgrlogit (func, VMG98, p);
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
	vmgrlogit (func, VMG98, p);
	if (p != logbuf) {
		*p = savechrs2[0];
		*(p + 1) = savechrs2[1];
	}
}

/*	vmgr_srv_deletedenmap - delete a triplet model/media_letter/density */

vmgr_srv_deletedenmap(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	struct vmgr_tape_denmap denmap_entry;
	char density[CA_MAXDENLEN+1];
	char func[22];
	gid_t gid;
	char logbuf[CA_MAXMODELLEN+CA_MAXMLLEN+CA_MAXDENLEN+16];
	char media_letter[CA_MAXMLLEN+1];
	char model[CA_MAXMODELLEN+1];
	char *rbp;
	vmgr_dbrec_addr rec_addr;
	uid_t uid;

	strcpy (func, "vmgr_srv_deletedenmap");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "deletedenmap", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, model, CA_MAXMODELLEN+1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, media_letter, CA_MAXMLLEN+1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, density, CA_MAXDENLEN+1) < 0)
		RETURN (EINVAL);
	sprintf (logbuf, "deletedenmap %s %s %s", model, media_letter, density);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	if (vmgr_get_denmap_entry (&thip->dbfd, model, media_letter, density,
	    &denmap_entry, 1, &rec_addr))
		RETURN (serrno);
	if (vmgr_delete_denmap_entry (&thip->dbfd, &rec_addr))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_deletedgnmap - delete a triplet dgn/model/library */

vmgr_srv_deletedgnmap(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	struct vmgr_tape_dgnmap dgnmap_entry;
	char func[22];
	gid_t gid;
	char library[CA_MAXTAPELIBLEN+1];
	char logbuf[CA_MAXMODELLEN+CA_MAXTAPELIBLEN+15];
	char model[CA_MAXMODELLEN+1];
	char *rbp;
	vmgr_dbrec_addr rec_addr;
	uid_t uid;

	strcpy (func, "vmgr_srv_deletedgnmap");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);
	
	vmgrlogit (func, VMG92, "deletedgnmap", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, model, CA_MAXMODELLEN+1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, library, CA_MAXTAPELIBLEN+1) < 0)
		RETURN (EINVAL);
	sprintf (logbuf, "deletedgnmap %s %s", model, library);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	if (vmgr_get_dgnmap_entry (&thip->dbfd, model, library, &dgnmap_entry,
	    1, &rec_addr))
		RETURN (serrno);
	if (vmgr_delete_dgnmap_entry (&thip->dbfd, &rec_addr))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_deletelibrary - delete a tape library definition */

vmgr_srv_deletelibrary(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	char func[21];
	gid_t gid;
	char library[CA_MAXTAPELIBLEN+1];
	struct vmgr_tape_library library_entry;
	char logbuf[CA_MAXTAPELIBLEN+15];
	char *rbp;
	vmgr_dbrec_addr rec_addr;
	uid_t uid;

	strcpy (func, "vmgr_srv_deletelibrary");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "deletelibrary", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, library, CA_MAXTAPELIBLEN+1) < 0)
		RETURN (EINVAL);
	sprintf (logbuf, "deletelibrary %s", library);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	if (vmgr_get_library_entry (&thip->dbfd, library,
	    &library_entry, 1, &rec_addr))
		RETURN (serrno);
	if (vmgr_delete_library_entry (&thip->dbfd, &rec_addr))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_deletemodel - delete a cartridge model */

vmgr_srv_deletemodel(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	struct vmgr_tape_media cartridge;
	char func[21];
	gid_t gid;
	char logbuf[CA_MAXMODELLEN+CA_MAXMLLEN+14];
	char media_letter[CA_MAXMLLEN+1];
	char model[CA_MAXMODELLEN+1];
	char *rbp;
	vmgr_dbrec_addr rec_addr;
	uid_t uid;

	strcpy (func, "vmgr_srv_deletemodel");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "deletemodel", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, model, CA_MAXMODELLEN+1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, media_letter, CA_MAXMLLEN+1) < 0)
		RETURN (EINVAL);
	sprintf (logbuf, "deletemodel %s %s", model, media_letter);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	if (vmgr_get_model_entry (&thip->dbfd, model, &cartridge, 1, &rec_addr))
		RETURN (serrno);
	if (*media_letter && strcmp (media_letter, " ") &&
	    strcmp (media_letter, cartridge.m_media_letter))
		RETURN (EINVAL);
	if (vmgr_delete_model_entry (&thip->dbfd, &rec_addr))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_deletepool - delete a tape pool definition */

vmgr_srv_deletepool(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	char func[20];
	gid_t gid;
	char logbuf[CA_MAXPOOLNAMELEN+12];
	struct vmgr_tape_pool pool_entry;
	char pool_name[CA_MAXPOOLNAMELEN+1];
	char *rbp;
	vmgr_dbrec_addr rec_addr;
	uid_t uid;

	strcpy (func, "vmgr_srv_deletepool");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "deletepool", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, pool_name, CA_MAXPOOLNAMELEN+1) < 0)
		RETURN (EINVAL);
	sprintf (logbuf, "deletepool %s", pool_name);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	if (vmgr_get_pool_entry (&thip->dbfd, pool_name, &pool_entry,
	    1, &rec_addr))
		RETURN (serrno);
	if (pool_entry.capacity)
		RETURN (EEXIST);
	if (vmgr_delete_pool_entry (&thip->dbfd, &rec_addr))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_deletetape - delete a tape volume */

vmgr_srv_deletetape(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	struct vmgr_tape_denmap denmap_entry;
	char func[20];
	gid_t gid;
	int i;
	struct vmgr_tape_library library_entry;
	char logbuf[CA_MAXVIDLEN+12];
	struct vmgr_tape_pool pool_entry;
	vmgr_dbrec_addr pool_rec_addr;
	char *rbp;
	vmgr_dbrec_addr rec_addr;
	vmgr_dbrec_addr rec_addrl;
	vmgr_dbrec_addr rec_addrs;
	vmgr_dbrec_addr rec_addrt;
	struct vmgr_tape_side side_entry;
	struct vmgr_tape_tag tag_entry;
	struct vmgr_tape_info tape;
	uid_t uid;
	char vid[CA_MAXVIDLEN+1];

	strcpy (func, "vmgr_srv_deletetape");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "deletetape", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1) < 0)
		RETURN (EINVAL);
	sprintf (logbuf, "deletetape %s", vid);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	if (vmgr_get_tape_by_vid (&thip->dbfd, vid, &tape, 1, &rec_addr))
		RETURN (serrno);

	/* delete associated tag if present */

	if (vmgr_get_tag_by_vid (&thip->dbfd, vid, &tag_entry, 1, &rec_addrt) == 0) {
		if (vmgr_delete_tag_entry (&thip->dbfd, &rec_addrt))
			RETURN (serrno);
		} else if (serrno != ENOENT)
			RETURN (serrno);

	/* get native capacity */

	if (vmgr_get_denmap_entry (&thip->dbfd, tape.model, tape.media_letter,
	    tape.density, &denmap_entry, 0, NULL))
		RETURN (serrno);

	/* delete all sides */

	for (i = 0; i < tape.nbsides; i++) {
		if (vmgr_get_side_by_fullid (&thip->dbfd, vid, i, &side_entry,
		    1, &rec_addrs))
			RETURN (serrno);
		if (side_entry.nbfiles)
			RETURN (EEXIST);
		if (i == 0 && vmgr_get_pool_entry (&thip->dbfd,
		    side_entry.poolname, &pool_entry, 1, &pool_rec_addr))
			RETURN (serrno);
		pool_entry.capacity -= denmap_entry.native_capacity;
		if (side_entry.status == 0)
			pool_entry.tot_free_space -= side_entry.estimated_free_space;
		if (vmgr_delete_side_entry (&thip->dbfd, &rec_addrs))
			RETURN (serrno);
	}
	if (vmgr_delete_tape_entry (&thip->dbfd, &rec_addr))
		RETURN (serrno);
	if (vmgr_get_library_entry (&thip->dbfd, tape.library, &library_entry,
	    1, &rec_addrl))
		RETURN (serrno);
	library_entry.nb_free_slots++;
	if (vmgr_update_library_entry (&thip->dbfd, &rec_addrl, &library_entry))
		RETURN (serrno);
	if (vmgr_update_pool_entry (&thip->dbfd, &pool_rec_addr, &pool_entry))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_deltag - delete a tag associated with a tape volume */

vmgr_srv_deltag(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXVIDLEN+8];
	struct vmgr_tape_pool pool_entry;
	char *rbp;
	vmgr_dbrec_addr rec_addr;
	struct vmgr_tape_side side_entry;
	struct vmgr_tape_tag tag_entry;
	uid_t uid;
	char vid[CA_MAXVIDLEN+1];

	strcpy (func, "vmgr_srv_deltag");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "deltag", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1) < 0)
		RETURN (EINVAL);
	sprintf (logbuf, "deltag %s", vid);
	vmgr_logreq (func, logbuf);

	/* get pool name */

	if (vmgr_get_side_by_fullid (&thip->dbfd, vid, 0, &side_entry,
	    0, NULL))
		RETURN (serrno);

	/* get pool entry */

	if (vmgr_get_pool_entry (&thip->dbfd, side_entry.poolname, &pool_entry,
	    0, NULL))
		RETURN (serrno);

	/* check if the user is authorized to delete the tag on this volume */

	if (((pool_entry.uid && pool_entry.uid != uid) ||
	    (pool_entry.gid && pool_entry.gid != gid)) &&
	    Cupv_check (uid, gid, clienthost, localhost, P_TAPE_OPERATOR))
		RETURN (EACCES);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	if (vmgr_get_tag_by_vid (&thip->dbfd, vid, &tag_entry, 1, &rec_addr))
		RETURN (serrno);
	if (vmgr_delete_tag_entry (&thip->dbfd, &rec_addr))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_enterdenmap - enter a new quadruplet model/media_letter/density/capacity */

vmgr_srv_enterdenmap(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	struct vmgr_tape_denmap denmap_entry;
	char func[21];
	gid_t gid;
	char logbuf[CA_MAXMODELLEN+CA_MAXMLLEN+CA_MAXDENLEN+15];
	struct vmgr_tape_media model_entry;
	char *rbp;
	uid_t uid;

	strcpy (func, "vmgr_srv_enterdenmap");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "enterdenmap", uid, gid, clienthost);
	memset ((char *) &denmap_entry, 0, sizeof(denmap_entry));
	if (unmarshall_STRINGN (rbp, denmap_entry.md_model, CA_MAXMODELLEN+1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, denmap_entry.md_media_letter, CA_MAXMLLEN+1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, denmap_entry.md_density, CA_MAXDENLEN+1) < 0)
		RETURN (EINVAL);
	if (magic < VMGR_MAGIC2)
		RETURN (EINVAL);
	unmarshall_LONG (rbp, denmap_entry.native_capacity);
	sprintf (logbuf, "enterdenmap %s %s %s", denmap_entry.md_model,
	    denmap_entry.md_media_letter, denmap_entry.md_density);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	/* check if model exists */

	if (vmgr_get_model_entry (&thip->dbfd, denmap_entry.md_model,
	    &model_entry, 0, NULL))
		RETURN (serrno);
	if (*denmap_entry.md_media_letter &&
	    strcmp (denmap_entry.md_media_letter, " ") &&
	    strcmp (denmap_entry.md_media_letter, model_entry.m_media_letter))
		RETURN (EINVAL);
	strcpy (denmap_entry.md_media_letter, model_entry.m_media_letter);
	if (denmap_entry.native_capacity <= 0)
		RETURN (EINVAL);

	denmap_entry.native_capacity *= 1024;	/* kB */


	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	if (vmgr_insert_denmap_entry (&thip->dbfd, &denmap_entry))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_enterdgnmap - enter a new triplet dgn/model/library */

vmgr_srv_enterdgnmap(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	struct vmgr_tape_dgnmap dgnmap_entry;
	char func[21];
	gid_t gid;
	struct vmgr_tape_library library_entry;
	char logbuf[CA_MAXDGNLEN+CA_MAXMODELLEN+CA_MAXTAPELIBLEN+15];
	struct vmgr_tape_media model_entry;
	char *rbp;
	uid_t uid;

	strcpy (func, "vmgr_srv_enterdgnmap");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "enterdgnmap", uid, gid, clienthost);
	memset ((char *) &dgnmap_entry, 0, sizeof(dgnmap_entry));
	if (unmarshall_STRINGN (rbp, dgnmap_entry.dgn, CA_MAXDGNLEN+1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, dgnmap_entry.model, CA_MAXMODELLEN+1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, dgnmap_entry.library, CA_MAXTAPELIBLEN+1) < 0)
		RETURN (EINVAL);
	sprintf (logbuf, "enterdgnmap %s %s %s", dgnmap_entry.dgn,
	    dgnmap_entry.model, dgnmap_entry.library);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	/* check if model exists */

	if (vmgr_get_model_entry (&thip->dbfd, dgnmap_entry.model, &model_entry,
	    0, NULL))
		RETURN (serrno);

	/* check if library exists */

	if (vmgr_get_library_entry (&thip->dbfd, dgnmap_entry.library,
	    &library_entry, 0, NULL))
		RETURN (serrno);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	if (vmgr_insert_dgnmap_entry (&thip->dbfd, &dgnmap_entry))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_enterlibrary - enter a new tape library definition */

vmgr_srv_enterlibrary(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	char func[22];
	gid_t gid;
	struct vmgr_tape_library library_entry;
	char logbuf[CA_MAXTAPELIBLEN+25];
	char *rbp;
	uid_t uid;

	strcpy (func, "vmgr_srv_enterlibrary");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "enterlibrary", uid, gid, clienthost);
	memset ((char *) &library_entry, 0, sizeof(library_entry));
	if (unmarshall_STRINGN (rbp, library_entry.name, CA_MAXTAPELIBLEN+1) < 0)
		RETURN (EINVAL);
	unmarshall_LONG (rbp, library_entry.capacity);
	unmarshall_LONG (rbp, library_entry.status);
	sprintf (logbuf, "enterlibrary %s %d", library_entry.name, library_entry.capacity);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	if (library_entry.capacity <= 0)
		RETURN (EINVAL);
	library_entry.nb_free_slots = library_entry.capacity;

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	if (vmgr_insert_library_entry (&thip->dbfd, &library_entry))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_entermodel - enter a new cartridge model */

vmgr_srv_entermodel(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	struct vmgr_tape_media cartridge;
	char func[20];
	gid_t gid;
	char logbuf[CA_MAXMODELLEN+CA_MAXMLLEN+13];
	int native_capacity;
	char *rbp;
	uid_t uid;

	strcpy (func, "vmgr_srv_entermodel");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "entermodel", uid, gid, clienthost);
	memset ((char *) &cartridge, 0, sizeof(cartridge));
	if (unmarshall_STRINGN (rbp, cartridge.m_model, CA_MAXMODELLEN+1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, cartridge.m_media_letter, CA_MAXMLLEN+1) < 0)
		RETURN (EINVAL);
	if (magic < VMGR_MAGIC2)
		unmarshall_LONG (rbp, native_capacity);	/* ignore */
	unmarshall_LONG (rbp, cartridge.media_cost);
	sprintf (logbuf, "entermodel %s %s", cartridge.m_model, cartridge.m_media_letter);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	if (vmgr_insert_model_entry (&thip->dbfd, &cartridge))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_enterpool - define a new tape pool */

vmgr_srv_enterpool(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	char func[19];
	gid_t gid;
	char logbuf[CA_MAXPOOLNAMELEN+33];
	struct vmgr_tape_pool pool_entry;
	char *rbp;
	uid_t uid;

	strcpy (func, "vmgr_srv_enterpool");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "enterpool", uid, gid, clienthost);
	memset ((char *) &pool_entry, 0, sizeof(pool_entry));
	if (unmarshall_STRINGN (rbp, pool_entry.name, CA_MAXPOOLNAMELEN+1) < 0)
		RETURN (EINVAL);
	unmarshall_LONG (rbp, pool_entry.uid);
	unmarshall_LONG (rbp, pool_entry.gid);
	sprintf (logbuf, "enterpool %s %d %d", pool_entry.name, pool_entry.uid,
	    pool_entry.gid);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	if (vmgr_insert_pool_entry (&thip->dbfd, &pool_entry))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_entertape - enter a new tape volume */

vmgr_srv_entertape(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	struct vmgr_tape_media cartridge;
	struct vmgr_tape_denmap denmap_entry;
	struct vmgr_tape_dgnmap dgnmap_entry;
	char func[19];
	gid_t gid;
	int i;
	struct vmgr_tape_library library_entry;
	char logbuf[CA_MAXVIDLEN+CA_MAXPOOLNAMELEN+23];
	struct vmgr_tape_pool pool_entry;
	char *rbp;
	vmgr_dbrec_addr rec_addr;
	vmgr_dbrec_addr rec_addrl;
	struct vmgr_tape_side side_entry;
	struct vmgr_tape_info tape;
	uid_t uid;

	strcpy (func, "vmgr_srv_entertape");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "entertape", uid, gid, clienthost);

	memset ((char *) &tape, 0, sizeof(tape));
	memset ((char *) &side_entry, 0, sizeof(side_entry));
	if (unmarshall_STRINGN (rbp, tape.vid, CA_MAXVIDLEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, tape.vsn, CA_MAXVSNLEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, tape.library, CA_MAXTAPELIBLEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, tape.density, CA_MAXDENLEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, tape.lbltype, CA_MAXLBLTYPLEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, tape.model, CA_MAXMODELLEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, tape.media_letter, CA_MAXMLLEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, tape.manufacturer, CA_MAXMANUFLEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, tape.sn, CA_MAXSNLEN + 1) < 0)
		RETURN (EINVAL);
	unmarshall_WORD (rbp, tape.nbsides);
	if (unmarshall_STRINGN (rbp, side_entry.poolname, CA_MAXPOOLNAMELEN + 1) < 0)
		RETURN (EINVAL);
	unmarshall_LONG (rbp, side_entry.status);
	sprintf (logbuf, "entertape %s %s %d", tape.vid, side_entry.poolname,
	    side_entry.status);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_TAPE_OPERATOR))
		RETURN (serrno);

	if (! *tape.vsn)
		strcpy (tape.vsn, tape.vid);
	if (*tape.lbltype) {
		if (strcmp (tape.lbltype, "al") &&
		    strcmp (tape.lbltype, "aul") &&
		    strcmp (tape.lbltype, "nl") &&
		    strcmp (tape.lbltype, "sl"))
			RETURN (EINVAL);
	} else
		strcpy (tape.lbltype, "al");
	if (! *side_entry.poolname)
		strcpy (side_entry.poolname, "default");

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	/* check if pool exists and lock entry */

	if (vmgr_get_pool_entry (&thip->dbfd, side_entry.poolname, &pool_entry,
	    1, &rec_addr))
		if (serrno == ENOENT) {
			sendrep (thip->s, MSG_ERR, "No such pool\n");
			RETURN (EINVAL);
		} else
			RETURN (serrno);

	/* check if model exists */

	memset((void *) &cartridge, 0, sizeof(struct vmgr_tape_media));
	if (vmgr_get_model_entry (&thip->dbfd, tape.model, &cartridge, 0, NULL))
		if (serrno == ENOENT) {
			sendrep (thip->s, MSG_ERR, "No such model\n");
			RETURN (EINVAL);
		} else
			RETURN (serrno);
	if (*tape.media_letter && strcmp (tape.media_letter, " ") &&
	    strcmp (tape.media_letter, cartridge.m_media_letter))
		RETURN (EINVAL);
	strcpy (tape.media_letter, cartridge.m_media_letter);

	/* check if library exists */

	memset((void *) &library_entry, 0, sizeof(struct vmgr_tape_library));
	if (vmgr_get_library_entry (&thip->dbfd, tape.library, &library_entry,
	    1, &rec_addrl))
		if (serrno == ENOENT) {
			sendrep (thip->s, MSG_ERR, "No such library\n");
			RETURN (EINVAL);
		} else
			RETURN (serrno);

	/* check if the combination library/model exists */

	if (vmgr_get_dgnmap_entry (&thip->dbfd, tape.model, tape.library,
	    &dgnmap_entry, 0, NULL))
		if (serrno == ENOENT) {
			sendrep (thip->s, MSG_ERR,
			    "Combination library/model does not exist\n");
			RETURN (EINVAL);
		} else
			RETURN (serrno);

	/* check if density is valid for this model and get native capacity */

	memset((void *) &denmap_entry, 0, sizeof(struct vmgr_tape_denmap));
	if (vmgr_get_denmap_entry (&thip->dbfd, tape.model,
	    tape.media_letter, tape.density, &denmap_entry, 0, NULL))
		if (serrno == ENOENT) {
			sendrep (thip->s, MSG_ERR,
			    "Unsupported density for this model\n");
			RETURN (EINVAL);
		} else
			RETURN (serrno);
	side_entry.estimated_free_space = denmap_entry.native_capacity;

	tape.etime = time (0);
	strcpy (tape.rhost, "N/A");
	strcpy (tape.whost, "N/A");
	if (vmgr_insert_tape_entry (&thip->dbfd, &tape))
		RETURN (serrno);

	strcpy (side_entry.vid, tape.vid);
	for (i = 0; i < tape.nbsides; i++) {
		side_entry.side = i;
		if (vmgr_insert_side_entry (&thip->dbfd, &side_entry))
			RETURN (serrno);
		pool_entry.capacity += denmap_entry.native_capacity;
		if (side_entry.status == 0)
			pool_entry.tot_free_space += denmap_entry.native_capacity;
	}

	library_entry.nb_free_slots--;
	if (vmgr_update_library_entry (&thip->dbfd, &rec_addrl, &library_entry))
		RETURN (serrno);

	if (vmgr_update_pool_entry (&thip->dbfd, &rec_addr, &pool_entry))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_gettag - get the tag associated with a tape volume */

vmgr_srv_gettag(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXVIDLEN+8];
	struct vmgr_tape_tag tag_entry;
	char *rbp;
	char repbuf[CA_MAXTAGLEN+1];
	char *sbp;
	uid_t uid;
	char vid[CA_MAXVIDLEN+1];

	strcpy (func, "vmgr_srv_gettag");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "gettag", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1) < 0)
		RETURN (EINVAL);
	sprintf (logbuf, "gettag %s", vid);
	vmgr_logreq (func, logbuf);

	if (vmgr_get_tag_by_vid (&thip->dbfd, vid, &tag_entry, 0, NULL))
		RETURN (serrno);
        sbp = repbuf;
        marshall_STRING (sbp, tag_entry.text);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURN (0);
}

/*	vmgr_srv_gettape - get a tape volume to store a given amount of data */

vmgr_srv_gettape(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	char Condition[512];
	struct vmgr_tape_dgnmap dgnmap_entry;
	int fseq;
	char func[17];
	gid_t gid;
	int i;
	char logbuf[CA_MAXPOOLNAMELEN+30];
	static int onealloc;
	struct vmgr_tape_pool pool_entry;
	char poolname[CA_MAXPOOLNAMELEN+1];
	char *rbp;
	vmgr_dbrec_addr rec_addr;
	char repbuf[55];
	int save_serrno;
	char *sbp;
	struct vmgr_tape_side side_entry;
	struct vmgr_tape_side side_entry1;
	u_signed64 Size;
	struct vmgr_tape_info tape;
	char tmpbuf[21];
	u_signed64 u64;
	uid_t uid;

	strcpy (func, "vmgr_srv_gettape");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "gettape", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, poolname, CA_MAXPOOLNAMELEN + 1) < 0)
		RETURN (EINVAL);
	unmarshall_HYPER (rbp, Size);
	if (unmarshall_STRINGN (rbp, Condition, 512) < 0)
		RETURN (EINVAL);
	sprintf (logbuf, "gettape %s %s", poolname, u64tostr(Size, tmpbuf, 0));
	vmgr_logreq (func, logbuf);

	if (! *poolname)
		strcpy (poolname, "default");
	else {	/* check if pool exists and permissions are ok */
		if (vmgr_get_pool_entry (&thip->dbfd, poolname, &pool_entry,
		    0, NULL))
			RETURN (serrno);
		if (((pool_entry.uid > 0 && uid != pool_entry.uid) ||
		    (pool_entry.gid > 0 && gid != pool_entry.gid)) &&
		    Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
			RETURN (EACCES);
	}
	if (Size <= 0)
		RETURN (EINVAL);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);
	if (Cthread_mutex_lock (&onealloc))
		RETURN (serrno);

	/* get and lock entry */

	memset ((void *) &side_entry, 0, sizeof(struct vmgr_tape_side));
	if (vmgr_get_side_by_size (&thip->dbfd, poolname, Size, &side_entry, 1,
	    &rec_addr)) {
		if (serrno == ENOENT)
			serrno = ENOSPC;
		save_serrno = serrno;
		(void) Cthread_mutex_unlock (&onealloc);
		RETURN (save_serrno);
	}

	/* update entry */

	side_entry.status = TAPE_BUSY;

	if (vmgr_update_side_entry (&thip->dbfd, &rec_addr, &side_entry)) {
		save_serrno = serrno;
		(void) Cthread_mutex_unlock (&onealloc);
		RETURN (save_serrno);
	}

	/* get base entry */

	memset((void *) &tape, 0, sizeof(struct vmgr_tape_info));
	if (vmgr_get_tape_by_vid (&thip->dbfd, side_entry.vid, &tape, 0, NULL)) {
		save_serrno = serrno;
		(void) Cthread_mutex_unlock (&onealloc);
		RETURN (save_serrno);
	}

	/* if the media offer several sides, mark all sides busy
	   if they are not FULL */

	for (i = 0; i < tape.nbsides; i++) {
		if (i == side_entry.side) continue;
		if (vmgr_get_side_by_fullid (&thip->dbfd, side_entry.vid, i,
		    &side_entry1, 1, &rec_addr)) {
			save_serrno = serrno;
			(void) Cthread_mutex_unlock (&onealloc);
			RETURN (save_serrno);
		}
		if (side_entry1.status == 0)
			side_entry1.status = TAPE_BUSY;
		if (vmgr_update_side_entry (&thip->dbfd, &rec_addr,
		    &side_entry1)) {
			save_serrno = serrno;
			(void) Cthread_mutex_unlock (&onealloc);
			RETURN (save_serrno);
		}
	}

	/* get dgn */

	if (vmgr_get_dgnmap_entry (&thip->dbfd, tape.model, tape.library,
	    &dgnmap_entry, 0, NULL)) {
		save_serrno = serrno;
		(void) Cthread_mutex_unlock (&onealloc);
		RETURN (save_serrno);
	}

	fseq = side_entry.nbfiles + 1;
	u64 = (u_signed64) side_entry.estimated_free_space * 1024;
	vmgrlogit(func, "%s %d %d\n", tape.vid, side_entry.side, fseq);
	sbp = repbuf;
	marshall_STRING (sbp, tape.vid);
	marshall_STRING (sbp, tape.vsn);
	marshall_STRING (sbp, dgnmap_entry.dgn);
	marshall_STRING (sbp, tape.density);
	marshall_STRING (sbp, tape.lbltype);
	marshall_STRING (sbp, tape.model);
	if (magic >= VMGR_MAGIC2) {
		marshall_WORD (sbp, side_entry.side);
	}
	marshall_LONG (sbp, fseq);
	marshall_HYPER (sbp, u64);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	(void) Cthread_mutex_unlock (&onealloc);
	RETURN (0);
}

/*      vmgr_srv_listdenmap - list triplets model/media_letter/density */

vmgr_srv_listdenmap(magic, req_data, clienthost, thip, endlist, dblistptr)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
int endlist;
DBLISTPTR *dblistptr;
{
	int bol;	/* beginning of list flag */
	int c;
	struct vmgr_tape_denmap denmap_entry;
	int eol = 0;	/* end of list flag */
	char func[20];
	gid_t gid;
	int listentsz;	/* size of client machine vmgr_tape_denmap structure */
	int maxnbentries;	/* maximum number of entries/call */
	int native_capacity;
	int nbentries = 0;
	char outbuf[LISTBUFSZ+4];
	char *p;
	char *rbp;
	char *sbp;
	uid_t uid;

	strcpy (func, "vmgr_srv_listdenmap");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "listdenmap", uid, gid, clienthost);
	unmarshall_WORD (rbp, listentsz);
	unmarshall_WORD (rbp, bol);

	/* return as many entries as possible to the client */

	maxnbentries = LISTBUFSZ / listentsz;
	sbp = outbuf;
	marshall_WORD (sbp, nbentries);		/* will be updated */

	while (nbentries < maxnbentries &&
	    (c = vmgr_list_denmap_entry (&thip->dbfd, bol, &denmap_entry,
		endlist, dblistptr)) == 0) {
		native_capacity = denmap_entry.native_capacity / 1024;
		marshall_STRING (sbp, denmap_entry.md_model);
		marshall_STRING (sbp, denmap_entry.md_media_letter);
		marshall_STRING (sbp, denmap_entry.md_density);
		if (magic >= VMGR_MAGIC2)
			marshall_LONG (sbp, native_capacity);
		nbentries++;
		bol = 0;
	}
	if (c < 0)
		RETURN (serrno);
	if (c == 1)
		eol = 1;

	marshall_WORD (sbp, eol);
	p = outbuf;
	marshall_WORD (p, nbentries);		/* update nbentries in reply */
	sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
	RETURN (0);
}

/*      vmgr_srv_listdgnmap - list triplets model/media_letter/density */

vmgr_srv_listdgnmap(magic, req_data, clienthost, thip, endlist, dblistptr)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
int endlist;
DBLISTPTR *dblistptr;
{
	int bol;	/* beginning of list flag */
	int c;
	struct vmgr_tape_dgnmap dgnmap_entry;
	int eol = 0;	/* end of list flag */
	char func[20];
	gid_t gid;
	int listentsz;	/* size of client machine vmgr_tape_dgnmap structure */
	int maxnbentries;	/* maximum number of entries/call */
	int nbentries = 0;
	char outbuf[LISTBUFSZ+4];
	char *p;
	char *rbp;
	char *sbp;
	uid_t uid;

	strcpy (func, "vmgr_srv_listdgnmap");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "listdgnmap", uid, gid, clienthost);
	unmarshall_WORD (rbp, listentsz);
	unmarshall_WORD (rbp, bol);

	/* return as many entries as possible to the client */

	maxnbentries = LISTBUFSZ / listentsz;
	sbp = outbuf;
	marshall_WORD (sbp, nbentries);		/* will be updated */

	while (nbentries < maxnbentries &&
	    (c = vmgr_list_dgnmap_entry (&thip->dbfd, bol, &dgnmap_entry,
		endlist, dblistptr)) == 0) {
		marshall_STRING (sbp, dgnmap_entry.dgn);
		marshall_STRING (sbp, dgnmap_entry.model);
		marshall_STRING (sbp, dgnmap_entry.library);
		nbentries++;
		bol = 0;
	}
	if (c < 0)
		RETURN (serrno);
	if (c == 1)
		eol = 1;

	marshall_WORD (sbp, eol);
	p = outbuf;
	marshall_WORD (p, nbentries);		/* update nbentries in reply */
	sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
	RETURN (0);
}

/*      vmgr_srv_listlibrary - list tape library entries */

vmgr_srv_listlibrary(magic, req_data, clienthost, thip, endlist, dblistptr)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
int endlist;
DBLISTPTR *dblistptr;
{
	int bol;	/* beginning of list flag */
	int c;
	int capacity;
	int eol = 0;	/* end of list flag */
	char func[21];
	gid_t gid;
	struct vmgr_tape_library library_entry;
	int listentsz;	/* size of client machine vmgr_tape_library structure */
	int maxnbentries;	/* maximum number of entries/call */
	int nb_free_slots;
	int nbentries = 0;
	char outbuf[LISTBUFSZ+4];
	char *p;
	char *rbp;
	char *sbp;
	uid_t uid;

	strcpy (func, "vmgr_srv_listlibrary");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "listlibrary", uid, gid, clienthost);
	unmarshall_WORD (rbp, listentsz);
	unmarshall_WORD (rbp, bol);

	/* return as many entries as possible to the client */

	maxnbentries = LISTBUFSZ / listentsz;
	sbp = outbuf;
	marshall_WORD (sbp, nbentries);		/* will be updated */

	while (nbentries < maxnbentries &&
	    (c = vmgr_list_library_entry (&thip->dbfd, bol, &library_entry,
		endlist, dblistptr)) == 0) {
		marshall_STRING (sbp, library_entry.name);
		marshall_LONG (sbp, library_entry.capacity);
		marshall_LONG (sbp, library_entry.nb_free_slots);
		marshall_LONG (sbp, library_entry.status);
		nbentries++;
		bol = 0;
	}
	if (c < 0)
		RETURN (serrno);
	if (c == 1)
		eol = 1;

	marshall_WORD (sbp, eol);
	p = outbuf;
	marshall_WORD (p, nbentries);		/* update nbentries in reply */
	sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
	RETURN (0);
}

/*      vmgr_srv_listmodel - list cartridge model entries */

vmgr_srv_listmodel(magic, req_data, clienthost, thip, endlist, dblistptr)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
int endlist;
DBLISTPTR *dblistptr;
{
	int bol;	/* beginning of list flag */
	int c;
	struct vmgr_tape_media cartridge;
	int eol = 0;	/* end of list flag */
	char func[19];
	gid_t gid;
	int listentsz;	/* size of client machine vmgr_tape_media structure */
	int maxnbentries;	/* maximum number of entries/call */
	int nbentries = 0;
	char outbuf[LISTBUFSZ+4];
	char *p;
	char *rbp;
	char *sbp;
	uid_t uid;

	strcpy (func, "vmgr_srv_listmodel");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "listmodel", uid, gid, clienthost);
	unmarshall_WORD (rbp, listentsz);
	unmarshall_WORD (rbp, bol);

	/* return as many entries as possible to the client */

	maxnbentries = LISTBUFSZ / listentsz;
	sbp = outbuf;
	marshall_WORD (sbp, nbentries);		/* will be updated */

	while (nbentries < maxnbentries &&
	    (c = vmgr_list_model_entry (&thip->dbfd, bol, &cartridge,
		endlist, dblistptr)) == 0) {
		marshall_STRING (sbp, cartridge.m_model);
		marshall_STRING (sbp, cartridge.m_media_letter);
		if (magic < VMGR_MAGIC2)
			marshall_LONG (sbp, 0);	/* dummy */
		marshall_LONG (sbp, cartridge.media_cost);
		nbentries++;
		bol = 0;
	}
	if (c < 0)
		RETURN (serrno);
	if (c == 1)
		eol = 1;

	marshall_WORD (sbp, eol);
	p = outbuf;
	marshall_WORD (p, nbentries);		/* update nbentries in reply */
	sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
	RETURN (0);
}

/*      vmgr_srv_listpool - list tape pool entries */

vmgr_srv_listpool(magic, req_data, clienthost, thip, endlist, dblistptr)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
int endlist;
DBLISTPTR *dblistptr;
{
	int bol;	/* beginning of list flag */
	int c;
	u_signed64 capacity;
	int eol = 0;	/* end of list flag */
	char func[18];
	gid_t gid;
	int listentsz;	/* size of client machine vmgr_tape_pool structure */
	int maxnbentries;	/* maximum number of entries/call */
	int nbentries = 0;
	char outbuf[LISTBUFSZ+4];
	char *p;
	struct vmgr_tape_pool pool_entry;
	char *rbp;
	char *sbp;
	u_signed64 tot_free_space;
	uid_t uid;

	strcpy (func, "vmgr_srv_listpool");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "listpool", uid, gid, clienthost);
	unmarshall_WORD (rbp, listentsz);
	unmarshall_WORD (rbp, bol);

	/* return as many entries as possible to the client */

	maxnbentries = LISTBUFSZ / listentsz;
	sbp = outbuf;
	marshall_WORD (sbp, nbentries);		/* will be updated */

	while (nbentries < maxnbentries &&
	    (c = vmgr_list_pool_entry (&thip->dbfd, bol, &pool_entry,
		endlist, dblistptr)) == 0) {
		marshall_STRING (sbp, pool_entry.name);
		marshall_LONG (sbp, pool_entry.uid);
		marshall_LONG (sbp, pool_entry.gid);
		capacity = pool_entry.capacity * 1024;
		marshall_HYPER (sbp, capacity);
		tot_free_space = pool_entry.tot_free_space * 1024;
		marshall_HYPER (sbp, tot_free_space);
		nbentries++;
		bol = 0;
	}
	if (c < 0)
		RETURN (serrno);
	if (c == 1)
		eol = 1;

	marshall_WORD (sbp, eol);
	p = outbuf;
	marshall_WORD (p, nbentries);		/* update nbentries in reply */
	sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
	RETURN (0);
}

/*      vmgr_srv_listtape - list tape volume entries */

vmgr_srv_listtape(magic, req_data, clienthost, thip, tape, endlist, dblistptr)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
struct vmgr_tape_info *tape;
int endlist;
DBLISTPTR *dblistptr;
{
	int bol;	/* beginning of list flag */
	int c;
	struct vmgr_tape_dgnmap dgnmap_entry;
	int eol = 0;	/* end of list flag */
	char func[18];
	gid_t gid;
	int listentsz;	/* size of client machine vmgr_tape_info structure */
	int maxnbentries;	/* maximum number of entries/call */
	int nbentries = 0;
	char outbuf[LISTBUFSZ+4];
	char *p;
	char pool_name[CA_MAXPOOLNAMELEN+1];
	struct vmgr_tape_side side_entry;
	char *rbp;
	char *sbp;
	uid_t uid;
	char vid[CA_MAXVIDLEN+1];

	strcpy (func, "vmgr_srv_listtape");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "listtape", uid, gid, clienthost);
	unmarshall_WORD (rbp, listentsz);
	if (magic >= VMGR_MAGIC2) {
		if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN + 1) < 0)
			RETURN (EINVAL);
	} else
		vid[0] = '\0';
	if (unmarshall_STRINGN (rbp, pool_name, CA_MAXPOOLNAMELEN + 1) < 0)
		RETURN (EINVAL);
	unmarshall_WORD (rbp, bol);

	/* return as many entries as possible to the client */

	maxnbentries = LISTBUFSZ / ((MARSHALLED_TAPE_ENTRYSZ > listentsz) ?
		MARSHALLED_TAPE_ENTRYSZ : listentsz);
	sbp = outbuf;
	marshall_WORD (sbp, nbentries);		/* will be updated */

	while (nbentries < maxnbentries &&
	    (c = vmgr_list_side_entry (&thip->dbfd, bol, vid, pool_name,
		&side_entry, endlist, dblistptr)) == 0) {
		if (bol || strcmp (tape->vid, side_entry.vid))
			if (vmgr_get_tape_by_vid (&thip->dbfd, side_entry.vid,
			    tape, 0, NULL))
				RETURN (serrno);
		marshall_STRING (sbp, tape->vid);
		marshall_STRING (sbp, tape->vsn);
		if (magic < VMGR_MAGIC2) {
			if (vmgr_get_dgnmap_entry (&thip->dbfd, tape->model,
			    tape->library, &dgnmap_entry, 0, NULL))
				RETURN (serrno);
			marshall_STRING (sbp, dgnmap_entry.dgn);
		} else
			marshall_STRING (sbp, tape->library);
		marshall_STRING (sbp, tape->density);
		if (magic < VMGR_MAGIC2)
			tape->lbltype[2] = '\0';
		marshall_STRING (sbp, tape->lbltype);
		marshall_STRING (sbp, tape->model);
		marshall_STRING (sbp, tape->media_letter);
		marshall_STRING (sbp, tape->manufacturer);
		marshall_STRING (sbp, tape->sn);
		if (magic >= VMGR_MAGIC2) {
			marshall_WORD (sbp, tape->nbsides);
			marshall_TIME_T (sbp, tape->etime);
			marshall_WORD (sbp, side_entry.side);
		}
		marshall_STRING (sbp, side_entry.poolname);
		marshall_LONG (sbp, side_entry.estimated_free_space);
		marshall_LONG (sbp, side_entry.nbfiles);
		marshall_LONG (sbp, tape->rcount);
		marshall_LONG (sbp, tape->wcount);
		if (magic >= VMGR_MAGIC2) {
			marshall_STRING (sbp, tape->rhost);
			marshall_STRING (sbp, tape->whost);
			marshall_LONG (sbp, tape->rjid);
			marshall_LONG (sbp, tape->wjid);
		}
		marshall_TIME_T (sbp, tape->rtime);
		marshall_TIME_T (sbp, tape->wtime);
		marshall_LONG (sbp, side_entry.status);
		nbentries++;
		bol = 0;
	}
	if (c < 0)
		RETURN (serrno);
	if (c == 1)
		eol = 1;

	marshall_WORD (sbp, eol);
	p = outbuf;
	marshall_WORD (p, nbentries);		/* update nbentries in reply */
	sendrep (thip->s, MSG_DATA, sbp - outbuf, outbuf);
	RETURN (0);
}

/*	vmgr_srv_modifylibrary - modify an existing tape library */

vmgr_srv_modifylibrary(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	int capacity;
	char func[23];
	gid_t gid;
	char library[CA_MAXTAPELIBLEN+1];
	struct vmgr_tape_library library_entry;
	char logbuf[CA_MAXTAPELIBLEN+16];
	char *rbp;
	vmgr_dbrec_addr rec_addr;
	int status;
	uid_t uid;

	strcpy (func, "vmgr_srv_modifylibrary");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "modifylibrary", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, library, CA_MAXTAPELIBLEN+1) < 0)
		RETURN (EINVAL);
	unmarshall_LONG (rbp, capacity);
	unmarshall_LONG (rbp, status);
	sprintf (logbuf, "modifylibrary %s %d", library, capacity);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	/* get and lock entry */

	memset((void *) &library_entry, 0, sizeof(struct vmgr_tape_library));
	if (vmgr_get_library_entry (&thip->dbfd, library, &library_entry,
	    1, &rec_addr))
		RETURN (serrno);

	/* update entry */

	if (capacity > 0) {
		library_entry.nb_free_slots += capacity - library_entry.capacity;
		library_entry.capacity = capacity;
	}
	if (status > 0)
		library_entry.status = status;

	if (vmgr_update_library_entry (&thip->dbfd, &rec_addr, &library_entry))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_modifymodel - modify an existing cartridge model */

vmgr_srv_modifymodel(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	struct vmgr_tape_media cartridge;
	char func[21];
	gid_t gid;
	char logbuf[CA_MAXMODELLEN+CA_MAXMLLEN+14];
	int media_cost;
	char media_letter[CA_MAXMLLEN+1];
	char model[CA_MAXMODELLEN+1];
	int native_capacity;
	char *rbp;
	vmgr_dbrec_addr rec_addr;
	uid_t uid;

	strcpy (func, "vmgr_srv_modifymodel");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "modifymodel", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, model, CA_MAXMODELLEN+1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, media_letter, CA_MAXMLLEN+1) < 0)
		RETURN (EINVAL);
	if (magic < VMGR_MAGIC2)
		unmarshall_LONG (rbp, native_capacity);	/* ignore */
	unmarshall_LONG (rbp, media_cost);
	sprintf (logbuf, "modifymodel %s %s", model, media_letter);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	/* get and lock entry */

	memset((void *) &cartridge, 0, sizeof(struct vmgr_tape_media));
	if (vmgr_get_model_entry (&thip->dbfd, model, &cartridge, 1, &rec_addr))
		RETURN (serrno);
	if (*media_letter && strcmp (media_letter, " ") &&
	    strcmp (media_letter, cartridge.m_media_letter))
		RETURN (EINVAL);

	/* update entry */

	if (media_cost)
		cartridge.media_cost = media_cost;

	if (vmgr_update_model_entry (&thip->dbfd, &rec_addr, &cartridge))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_modifypool - modify an existing tape pool definition */

vmgr_srv_modifypool(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	char func[20];
	gid_t gid;
	char logbuf[CA_MAXPOOLNAMELEN+34];
	struct vmgr_tape_pool pool_entry;
	gid_t pool_group;
	char pool_name[CA_MAXPOOLNAMELEN+1];
	uid_t pool_user;
	char *rbp;
	vmgr_dbrec_addr rec_addr;
	uid_t uid;

	strcpy (func, "vmgr_srv_modifypool");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "modifypool", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, pool_name, CA_MAXPOOLNAMELEN+1) < 0)
		RETURN (EINVAL);
	unmarshall_LONG (rbp, pool_user);
	unmarshall_LONG (rbp, pool_group);
	sprintf (logbuf, "modifypool %s %d %d", pool_name, pool_user, pool_group);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	/* get and lock entry */

	memset((void *) &pool_entry, 0, sizeof(struct vmgr_tape_pool));
	if (vmgr_get_pool_entry (&thip->dbfd, pool_name, &pool_entry,
	    1, &rec_addr))
		RETURN (serrno);

	/* update entry */

	if (pool_user != -1)
		pool_entry.uid = pool_user;
	if (pool_group != -1)
		pool_entry.gid = pool_group;

	if (vmgr_update_pool_entry (&thip->dbfd, &rec_addr, &pool_entry))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_modifytape - modify an existing tape volume */

vmgr_srv_modifytape(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	int capacity_changed = 0;
	struct vmgr_tape_media cartridge;
	struct vmgr_tape_denmap denmap_entry;
	char density[CA_MAXDENLEN+1];
	int density_changed = 0;
	struct vmgr_tape_dgnmap dgnmap_entry;
	char func[20];
	gid_t gid;
	int i;
	char lbltype[CA_MAXLBLTYPLEN+1];
	char library[CA_MAXTAPELIBLEN+1];
	struct vmgr_tape_library library_entry;
	char logbuf[CA_MAXVIDLEN+CA_MAXPOOLNAMELEN+24];
	char manufacturer[CA_MAXMANUFLEN+1];
	int need_update = 0;
	struct vmgr_tape_pool newpool_entry;
	vmgr_dbrec_addr newpool_rec_addr;
	struct vmgr_tape_denmap olddenmap_entry;
	struct vmgr_tape_pool oldpool_entry;
	vmgr_dbrec_addr oldpool_rec_addr;
	char *rbp;
	vmgr_dbrec_addr rec_addr;
	vmgr_dbrec_addr rec_addrl;
	vmgr_dbrec_addr rec_addrs;
	struct vmgr_tape_side side_entry;
	char sn[CA_MAXSNLEN+1];
	int status;
	struct vmgr_tape_info tape;
	char poolname[CA_MAXPOOLNAMELEN+1];
	uid_t uid;
	char vid[CA_MAXVIDLEN+1];
	char vsn[CA_MAXVSNLEN+1];

	strcpy (func, "vmgr_srv_modifytape");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "modifytape", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, vsn, CA_MAXVSNLEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, library, CA_MAXTAPELIBLEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, density, CA_MAXDENLEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, lbltype, CA_MAXLBLTYPLEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, manufacturer, CA_MAXMANUFLEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, sn, CA_MAXSNLEN + 1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, poolname, CA_MAXPOOLNAMELEN + 1) < 0)
		RETURN (EINVAL);
	unmarshall_LONG (rbp, status);
	sprintf (logbuf, "modifytape %s %s %d", vid, poolname, status);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_TAPE_OPERATOR))
		RETURN (serrno);

	if (*lbltype) {
		if (strcmp (lbltype, "al") &&
		    strcmp (lbltype, "aul") &&
		    strcmp (lbltype, "nl") &&
		    strcmp (lbltype, "sl"))
			RETURN (EINVAL);
	}

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	/* get and lock tape entry */

	memset((void *) &tape, 0, sizeof(struct vmgr_tape_info));
	if (vmgr_get_tape_by_vid (&thip->dbfd, vid, &tape, 1, &rec_addr))
		RETURN (serrno);

	if (*vsn && strcmp (vsn, tape.vsn)) {
		strcpy (tape.vsn, vsn);
		need_update++;
	}

	/* check if library exists and update the number of free slots */

	if (*library && strcmp (library, tape.library)) {
		memset((void *) &library_entry, 0, sizeof(struct vmgr_tape_library));
		if (vmgr_get_library_entry (&thip->dbfd, library, &library_entry,
		    1, &rec_addrl))
			if (serrno == ENOENT) {
				sendrep (thip->s, MSG_ERR, "No such library\n");
				RETURN (EINVAL);
			} else
				RETURN (serrno);

		/* check if the combination library/model exists */

		if (vmgr_get_dgnmap_entry (&thip->dbfd, tape.model, library,
		    &dgnmap_entry, 0, NULL))
			if (serrno == ENOENT) {
				sendrep (thip->s, MSG_ERR,
				    "Combination library/model does not exist\n");
				RETURN (EINVAL);
			} else
				RETURN (serrno);

		library_entry.nb_free_slots--;
		if (vmgr_update_library_entry (&thip->dbfd, &rec_addrl,
		    &library_entry))
			RETURN (serrno);

		memset((void *) &library_entry, 0, sizeof(struct vmgr_tape_library));
		if (vmgr_get_library_entry (&thip->dbfd, tape.library, &library_entry,
		    1, &rec_addrl))
			RETURN (serrno);
		library_entry.nb_free_slots++;
		if (vmgr_update_library_entry (&thip->dbfd, &rec_addrl,
		    &library_entry))
			RETURN (serrno);
		strcpy (tape.library, library);
		need_update++;
	}

	/* get current tape capacity */

	if (vmgr_get_denmap_entry (&thip->dbfd, tape.model,
	    tape.media_letter, tape.density, &olddenmap_entry, 0, NULL))
		RETURN (serrno);

	/* check if density is valid for this model */

	if (*density && strcmp (density, tape.density)) {
		memset((void *) &denmap_entry, 0, sizeof(struct vmgr_tape_denmap));
		if (vmgr_get_denmap_entry (&thip->dbfd, tape.model,
		    tape.media_letter, density, &denmap_entry, 0, NULL))
			if (serrno == ENOENT) {
				sendrep (thip->s, MSG_ERR,
				    "Unsupported density for this model\n");
				RETURN (EINVAL);
			} else
				RETURN (serrno);
		strcpy (tape.density, density);
		density_changed = 1;
		if (denmap_entry.native_capacity != olddenmap_entry.native_capacity)
			capacity_changed = 1;
		need_update++;
	} else
		memcpy (&denmap_entry, &olddenmap_entry, sizeof(denmap_entry));

	if (*lbltype && strcmp (lbltype, tape.lbltype)) {
		strcpy (tape.lbltype, lbltype);
		need_update++;
	}
	if (*manufacturer && strcmp (manufacturer, tape.manufacturer)) {
		strcpy (tape.manufacturer, manufacturer);
		need_update++;
	}
	if (*sn && strcmp (sn, tape.sn)) {
		strcpy (tape.sn, sn);
		need_update++;
	}

	if (need_update)
		if (vmgr_update_tape_entry (&thip->dbfd, &rec_addr, &tape))
			RETURN (serrno);

	for (i = 0; i < tape.nbsides; i++) {
		need_update = 0;

		/* get and lock side */

		if (vmgr_get_side_by_fullid (&thip->dbfd, vid, i, &side_entry,
		    1, &rec_addrs))
			RETURN (serrno);

		if (density_changed &&
		    (side_entry.nbfiles || (side_entry.status & TAPE_BUSY)))
			RETURN (EEXIST);
		if (*poolname == 0 ||
		    strcmp (poolname, side_entry.poolname) == 0) { /* same pool */
			if (((side_entry.status & ~TAPE_BUSY) == 0 &&
			    (status & ~TAPE_BUSY) > 0) ||
			    ((side_entry.status & ~TAPE_BUSY) > 0 &&
			    (status & ~TAPE_BUSY) == 0) ||
			    capacity_changed) {
				if (vmgr_get_pool_entry (&thip->dbfd,
				    side_entry.poolname, &oldpool_entry, 1,
				    &oldpool_rec_addr))
					RETURN (serrno);
				if (capacity_changed) {
					oldpool_entry.capacity +=
					    denmap_entry.native_capacity -
					    olddenmap_entry.native_capacity;
					if (side_entry.status == 0)
						oldpool_entry.tot_free_space -=
						    side_entry.estimated_free_space;
					side_entry.estimated_free_space =
					    denmap_entry.native_capacity;
					need_update++;
					if (status == 0 ||
					    (status < 0 && side_entry.status == 0))
						oldpool_entry.tot_free_space +=
						    side_entry.estimated_free_space;
				} else {	/* status changed */
					if (status & ~TAPE_BUSY)
						oldpool_entry.tot_free_space -=
						    side_entry.estimated_free_space;
					else
						oldpool_entry.tot_free_space +=
						    side_entry.estimated_free_space;
				}
				if (vmgr_update_pool_entry (&thip->dbfd,
				    &oldpool_rec_addr, &oldpool_entry))
					RETURN (serrno);
			}
		} else {	/* move to another pool */
			if (vmgr_get_pool_entry (&thip->dbfd, side_entry.poolname,
			    &oldpool_entry, 1, &oldpool_rec_addr))
				RETURN (serrno);
			oldpool_entry.capacity -= olddenmap_entry.native_capacity;
			if (side_entry.status == 0)
				oldpool_entry.tot_free_space -= side_entry.estimated_free_space;
			if (vmgr_update_pool_entry (&thip->dbfd,
			    &oldpool_rec_addr, &oldpool_entry))
				RETURN (serrno);
			if (capacity_changed) {
				side_entry.estimated_free_space = denmap_entry.native_capacity;
				need_update++;
			}
			if (vmgr_get_pool_entry (&thip->dbfd, poolname,
			    &newpool_entry, 1, &newpool_rec_addr))
				RETURN (serrno);
			newpool_entry.capacity += denmap_entry.native_capacity;
			if (status == 0 || (status < 0 && side_entry.status == 0))
				newpool_entry.tot_free_space += side_entry.estimated_free_space;
			if (vmgr_update_pool_entry (&thip->dbfd,
			    &newpool_rec_addr, &newpool_entry))
				RETURN (serrno);
			strcpy (side_entry.poolname, poolname);
			need_update++;
		}
		if (status >= 0) {
			if (status & TAPE_FULL)
				side_entry.estimated_free_space = 0;
			else if (side_entry.estimated_free_space == 0)
				status |= TAPE_FULL;
			side_entry.status = status;
			need_update++;
		}

		if (need_update)
			if (vmgr_update_side_entry (&thip->dbfd, &rec_addrs, &side_entry))
				RETURN (serrno);
	}
	RETURN (0);
}

/*	vmgr_srv_querypool - query about a tape pool */

vmgr_srv_querypool(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	u_signed64 capacity;
	char func[19];
	gid_t gid;
	char logbuf[CA_MAXPOOLNAMELEN+11];
	struct vmgr_tape_pool pool_entry;
	char pool_name[CA_MAXPOOLNAMELEN+1];
	char *rbp;
	char repbuf[24];
	char *sbp;
	u_signed64 tot_free_space;
	uid_t uid;

	strcpy (func, "vmgr_srv_querypool");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "querypool", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, pool_name, CA_MAXPOOLNAMELEN + 1) < 0)
		RETURN (EINVAL);
	sprintf (logbuf, "querypool %s", pool_name);
	vmgr_logreq (func, logbuf);

	memset((void *) &pool_entry, 0, sizeof(struct vmgr_tape_pool));
	if (vmgr_get_pool_entry (&thip->dbfd, pool_name, &pool_entry,
	    0, NULL))
		RETURN (serrno);

	sbp = repbuf;
	marshall_LONG (sbp, pool_entry.uid);
	marshall_LONG (sbp, pool_entry.gid);
	capacity = pool_entry.capacity * 1024;
	marshall_HYPER (sbp, capacity);
	tot_free_space = pool_entry.tot_free_space * 1024;
	marshall_HYPER (sbp, tot_free_space);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURN (0);
}

/*	vmgr_srv_querylibrary - query about a tape library */

vmgr_srv_querylibrary(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	int capacity;
	char func[22];
	gid_t gid;
	char library[CA_MAXTAPELIBLEN+1];
	struct vmgr_tape_library library_entry;
	char logbuf[CA_MAXTAPELIBLEN+14];
	char *rbp;
	char repbuf[12];
	char *sbp;
	uid_t uid;

	strcpy (func, "vmgr_srv_querylibrary");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "querylibrary", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, library, CA_MAXTAPELIBLEN+1) < 0)
		RETURN (EINVAL);
	sprintf (logbuf, "querylibrary %s", library);
	vmgr_logreq (func, logbuf);

	memset((void *) &library_entry, 0, sizeof(struct vmgr_tape_library));
	if (vmgr_get_library_entry (&thip->dbfd, library, &library_entry,
	    0, NULL))
		RETURN (serrno);

	sbp = repbuf;
	marshall_LONG (sbp, library_entry.capacity);
	marshall_LONG (sbp, library_entry.nb_free_slots);
	marshall_LONG (sbp, library_entry.status);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURN (0);
}

/*	vmgr_srv_querymodel - query about a cartridge model */

vmgr_srv_querymodel(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	struct vmgr_tape_media cartridge;
	char func[20];
	gid_t gid;
	char logbuf[CA_MAXMODELLEN+CA_MAXMLLEN+13];
	char media_letter[CA_MAXMLLEN+1];
	char model[CA_MAXMODELLEN+1];
	char *rbp;
	char repbuf[11];
	char *sbp;
	uid_t uid;

	strcpy (func, "vmgr_srv_querymodel");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "querymodel", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, model, CA_MAXMODELLEN+1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, media_letter, CA_MAXMLLEN+1) < 0)
		RETURN (EINVAL);
	sprintf (logbuf, "querymodel %s %s", model, media_letter);
	vmgr_logreq (func, logbuf);

	memset((void *) &cartridge, 0, sizeof(struct vmgr_tape_media));
	if (vmgr_get_model_entry (&thip->dbfd, model, &cartridge, 0, NULL))
		RETURN (serrno);
	if (*media_letter && strcmp (media_letter, " ") &&
	    strcmp (media_letter, cartridge.m_media_letter))
		RETURN (EINVAL);

	sbp = repbuf;
	marshall_STRING (sbp, cartridge.m_media_letter);
	if (magic < VMGR_MAGIC2)
		marshall_LONG (sbp, 0);	/* dummy */
	marshall_LONG (sbp, cartridge.media_cost);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURN (0);
}

/*	vmgr_srv_querytape - query about a tape volume */

vmgr_srv_querytape(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	struct vmgr_tape_dgnmap dgnmap_entry;
	char func[19];
	gid_t gid;
	char logbuf[CA_MAXVIDLEN+11];
	char *rbp;
	char repbuf[177];
	char *sbp;
	int side;
	struct vmgr_tape_side side_entry;
	struct vmgr_tape_info tape;
	uid_t uid;
	char vid[CA_MAXVIDLEN+1];

	strcpy (func, "vmgr_srv_querytape");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "querytape", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1) < 0)
		RETURN (EINVAL);
	if (magic >= VMGR_MAGIC2) {
		unmarshall_WORD (rbp, side);
	} else
		side = 0;
	sprintf (logbuf, "querytape %s %d", vid, side);
	vmgr_logreq (func, logbuf);

	memset((void *) &tape, 0, sizeof(struct vmgr_tape_info));
	if (vmgr_get_tape_by_vid (&thip->dbfd, vid, &tape, 0, NULL))
		RETURN (serrno);

	/* get side specific info */

	if (vmgr_get_side_by_fullid (&thip->dbfd, vid, side, &side_entry,
	    0, NULL))
		RETURN (serrno);

	/* get dgn */

	if (vmgr_get_dgnmap_entry (&thip->dbfd, tape.model, tape.library,
	    &dgnmap_entry, 0, NULL))
		RETURN (serrno);

	sbp = repbuf;
	marshall_STRING (sbp, tape.vsn);
	if (magic >= VMGR_MAGIC2) {
		marshall_STRING (sbp, tape.library);
	}
	marshall_STRING (sbp, dgnmap_entry.dgn);
	marshall_STRING (sbp, tape.density);
	marshall_STRING (sbp, tape.lbltype);
	marshall_STRING (sbp, tape.model);
	marshall_STRING (sbp, tape.media_letter);
	marshall_STRING (sbp, tape.manufacturer);
	marshall_STRING (sbp, tape.sn);
	if (magic >= VMGR_MAGIC2) {
		marshall_WORD (sbp, tape.nbsides);
		marshall_TIME_T (sbp, tape.etime);
		marshall_WORD (sbp, side_entry.side);
	}
	marshall_STRING (sbp, side_entry.poolname);
	marshall_LONG (sbp, side_entry.estimated_free_space);
	marshall_LONG (sbp, side_entry.nbfiles);
	marshall_LONG (sbp, tape.rcount);
	marshall_LONG (sbp, tape.wcount);
	if (magic >= VMGR_MAGIC2) {
		marshall_STRING (sbp, tape.rhost);
		marshall_STRING (sbp, tape.whost);
		marshall_LONG (sbp, tape.rjid);
		marshall_LONG (sbp, tape.wjid);
	}
	marshall_TIME_T (sbp, tape.rtime);
	marshall_TIME_T (sbp, tape.wtime);
	marshall_LONG (sbp, side_entry.status);
	sendrep (thip->s, MSG_DATA, sbp - repbuf, repbuf);
	RETURN (0);
}

/*	vmgr_srv_reclaim - reset tape volume content information */

vmgr_srv_reclaim(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	struct vmgr_tape_denmap denmap_entry;
	char func[17];
	gid_t gid;
	int i;
	char logbuf[CA_MAXVIDLEN+9];
	struct vmgr_tape_pool pool_entry;
	char *rbp;
	vmgr_dbrec_addr rec_addrp;
	vmgr_dbrec_addr rec_addrs;
	struct vmgr_tape_side side_entry;
	struct vmgr_tape_info tape;
	uid_t uid;
	char vid[CA_MAXVIDLEN+1];

	strcpy (func, "vmgr_srv_reclaim");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "reclaim", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1) < 0)
		RETURN (EINVAL);
	sprintf (logbuf, "reclaim %s", vid);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	/* get and lock entries */

	memset((void *) &tape, 0, sizeof(struct vmgr_tape_info));
	if (vmgr_get_tape_by_vid (&thip->dbfd, vid, &tape, 0, NULL))
		RETURN (serrno);

	memset ((char *) &denmap_entry, 0, sizeof(denmap_entry));
	if (vmgr_get_denmap_entry (&thip->dbfd, tape.model, tape.media_letter,
	    tape.density, &denmap_entry, 0, NULL))
		RETURN (serrno);

	/* update entries */

	memset((void *) &pool_entry, 0, sizeof(struct vmgr_tape_pool));
	for (i = 0; i < tape.nbsides; i++) {
		if (vmgr_get_side_by_fullid (&thip->dbfd, vid, i, &side_entry,
		    1, &rec_addrs))
			RETURN (serrno);
		if (i == 0 && vmgr_get_pool_entry (&thip->dbfd,
		    side_entry.poolname, &pool_entry, 1, &rec_addrp))
			RETURN (serrno);

		side_entry.estimated_free_space = denmap_entry.native_capacity;
		side_entry.nbfiles = 0;
		side_entry.status = 0;

		if (vmgr_update_side_entry (&thip->dbfd, &rec_addrs, &side_entry))
			RETURN (serrno);

		pool_entry.tot_free_space += denmap_entry.native_capacity;
	}
	if (vmgr_update_pool_entry (&thip->dbfd, &rec_addrp, &pool_entry))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_settag - add/replace a tag associated with a tape volume */

vmgr_srv_settag(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXVIDLEN+8];
	struct vmgr_tape_tag old_tag_entry;
	struct vmgr_tape_pool pool_entry;
	char *rbp;
	vmgr_dbrec_addr rec_addr;
	struct vmgr_tape_side side_entry;
	struct vmgr_tape_tag tag_entry;
	uid_t uid;

	strcpy (func, "vmgr_srv_settag");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "settag", uid, gid, clienthost);

	memset ((char *) &tag_entry, 0, sizeof(tag_entry));
	if (unmarshall_STRINGN (rbp, tag_entry.vid, CA_MAXVIDLEN+1) < 0)
		RETURN (EINVAL);
	if (unmarshall_STRINGN (rbp, tag_entry.text, CA_MAXTAGLEN+1) < 0)
		RETURN (EINVAL);
	sprintf (logbuf, "settag %s", tag_entry.vid);
	vmgr_logreq (func, logbuf);

	/* get pool name */

	if (vmgr_get_side_by_fullid (&thip->dbfd, tag_entry.vid, 0, &side_entry,
	    0, NULL))
		RETURN (serrno);

	/* get pool entry */

	if (vmgr_get_pool_entry (&thip->dbfd, side_entry.poolname, &pool_entry,
	    0, NULL))
		RETURN (serrno);

	/* check if the user is authorized to add/replace the tag on this volume */

	if (((pool_entry.uid && pool_entry.uid != uid) ||
	    (pool_entry.gid && pool_entry.gid != gid)) &&
	    Cupv_check (uid, gid, clienthost, localhost, P_TAPE_OPERATOR))
		RETURN (EACCES);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	/* add the tag entry or replace the tag text if the entry exists */

	if (vmgr_insert_tag_entry (&thip->dbfd, &tag_entry)) {
		if (serrno != EEXIST ||
		    vmgr_get_tag_by_vid (&thip->dbfd, tag_entry.vid,
			&old_tag_entry, 1, &rec_addr) ||
		    vmgr_update_tag_entry (&thip->dbfd, &rec_addr, &tag_entry))
			RETURN (serrno);
	}
	RETURN (0);
}

/*	vmgr_srv_shutdown - shutdown the volume manager */

vmgr_srv_shutdown(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	int force = 0;
	char func[18];
	gid_t gid;
	char *rbp;
	uid_t uid;

	strcpy (func, "vmgr_srv_shutdown");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "shutdown", uid, gid, clienthost);
	unmarshall_WORD (rbp, force);

	if (Cupv_check (uid, gid, clienthost, localhost, P_ADMIN))
		RETURN (serrno);

	being_shutdown = force + 1;
	RETURN (0);
}

/*	vmgr_srv_tpmounted - update tape volume content information */

vmgr_srv_tpmounted(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	char func[19];
	gid_t gid;
	char hostname[CA_MAXHOSTNAMELEN+1];
	int jid;
	char logbuf[CA_MAXVIDLEN+13];
	int mode;
	char *p;
	char *rbp;
	vmgr_dbrec_addr rec_addr;
	struct vmgr_tape_info tape;
	uid_t uid;
	char vid[CA_MAXVIDLEN+1];

	strcpy (func, "vmgr_srv_tpmounted");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "tpmounted", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1) < 0)
		RETURN (EINVAL);
	unmarshall_WORD (rbp, mode);
	if (magic >= VMGR_MAGIC2) {
		unmarshall_LONG (rbp, jid);
	} else
		jid = 0;
	sprintf (logbuf, "tpmounted %s %d", vid, mode);
	vmgr_logreq (func, logbuf);

	if (Cupv_check (uid, gid, clienthost, localhost, P_TAPE_SYSTEM))
		RETURN (serrno);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	/* get and lock entry */

	memset((void *) &tape, 0, sizeof(struct vmgr_tape_info));
	if (vmgr_get_tape_by_vid (&thip->dbfd, vid, &tape, 1, &rec_addr))
		RETURN (serrno);

	/* update entry */

	strcpy (hostname, clienthost);
	if (p = strchr (hostname, '.')) *p = '\0';
	*(hostname + CA_MAXSHORTHOSTLEN) = '\0';
	if (mode) {
		tape.wcount++;
		strcpy (tape.whost, hostname);
		tape.wjid = jid;
		tape.wtime = time (0);
	} else {
		tape.rcount++;
		strcpy (tape.rhost, hostname);
		tape.rjid = jid;
		tape.rtime = time (0);
	}

	if (vmgr_update_tape_entry (&thip->dbfd, &rec_addr, &tape))
		RETURN (serrno);
	RETURN (0);
}

/*	vmgr_srv_updatetape - update tape volume content information */

vmgr_srv_updatetape(magic, req_data, clienthost, thip)
int magic;
char *req_data;
char *clienthost;
struct vmgr_srv_thread_info *thip;
{
	u_signed64 BytesWritten;
	int CompressionFactor;
	char func[20];
	int FilesWritten;
	int Flags;
	gid_t gid;
	int i;
	char logbuf[CA_MAXVIDLEN+52];
	int n;
	struct vmgr_tape_pool pool_entry;
	char *rbp;
	vmgr_dbrec_addr rec_addr;
	vmgr_dbrec_addr rec_addrp;
	int side;
	struct vmgr_tape_side side_entry;
	struct vmgr_tape_side side_entry1;
	struct vmgr_tape_info tape;
	char tmpbuf[21];
	uid_t uid;
	char vid[CA_MAXVIDLEN+1];

	strcpy (func, "vmgr_srv_updatetape");
	rbp = req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);

	RESETID(uid, gid);

	vmgrlogit (func, VMG92, "updatetape", uid, gid, clienthost);
	if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1) < 0)
		RETURN (EINVAL);
	if (magic >= VMGR_MAGIC2) {
		unmarshall_WORD (rbp, side);
	} else
		side = 0;
	unmarshall_HYPER (rbp, BytesWritten);
	unmarshall_WORD (rbp, CompressionFactor);
	unmarshall_WORD (rbp, FilesWritten);
	unmarshall_WORD (rbp, Flags);
	sprintf (logbuf, "updatetape %s %d %s %d %d %d",
		vid, side,
		u64tostr(BytesWritten, tmpbuf, 0),
		(int) CompressionFactor,
		(int) FilesWritten,
		(int) Flags);
	vmgr_logreq (func, logbuf);

	/* start transaction */

	(void) vmgr_start_tr (thip->s, &thip->dbfd);

	/* get and lock entries */

	memset((void *) &side_entry, 0, sizeof(struct vmgr_tape_side));
	if (vmgr_get_side_by_fullid (&thip->dbfd, vid, side, &side_entry, 1,
	    &rec_addr))
		RETURN (serrno);

	memset((void *) &pool_entry, 0, sizeof(struct vmgr_tape_pool));
	if (vmgr_get_pool_entry (&thip->dbfd, side_entry.poolname, &pool_entry,
	    1, &rec_addrp))
		RETURN (serrno);

	/* update entries */

	if (CompressionFactor == 0)
		n = BytesWritten / 1024;
	else
		n = (BytesWritten / 1024) * 100 / CompressionFactor;
	if ((Flags & TAPE_FULL) || (n > side_entry.estimated_free_space))
		n = side_entry.estimated_free_space;
	side_entry.estimated_free_space -= n;
	side_entry.nbfiles += FilesWritten;
	side_entry.status &= ~TAPE_BUSY;
	if (side_entry.status == 0 && (Flags & (DISABLED|EXPORTED|TAPE_RDONLY|ARCHIVED)) != 0)
		n = side_entry.estimated_free_space;
	else if (side_entry.status)
		n = 0;		/* do not decrement pool free space if
				   status was already non zero */
	side_entry.status |= Flags;
	if (side_entry.estimated_free_space == 0)
		side_entry.status |= TAPE_FULL;

	if (vmgr_update_side_entry (&thip->dbfd, &rec_addr, &side_entry))
		RETURN (serrno);

	if ((Flags & TAPE_BUSY) == 0) {	/* reset BUSY flags on all sides */
		if (vmgr_get_tape_by_vid (&thip->dbfd, vid, &tape, 0, NULL))
			RETURN (serrno);
		for (i = 0; i < tape.nbsides; i++) {
			if (i == side_entry.side) continue;
			if (vmgr_get_side_by_fullid (&thip->dbfd, vid, i,
			    &side_entry1, 1, &rec_addr))
				RETURN (serrno);
			if ((side_entry1.status & TAPE_BUSY) == 0 && Flags == 0)
				continue;
			side_entry1.status &= ~TAPE_BUSY;
			if (Flags & EXPORTED)
				side_entry1.status |= EXPORTED;
			if (vmgr_update_side_entry (&thip->dbfd, &rec_addr,
			    &side_entry1))
				RETURN (serrno);
		}
	}

	if (n) {
		pool_entry.tot_free_space -= n;
		if (vmgr_update_pool_entry (&thip->dbfd, &rec_addrp, &pool_entry))
			RETURN (serrno);
	}
	RETURN (0);
}
