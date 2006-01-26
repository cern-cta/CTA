/*
 * Copyright (C) 2001-2005 by CERN/IT/DS/HSM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cns_mysql_ifce.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:19 $ CERN IT-DS/HSM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <errmsg.h>
#include <mysqld_error.h>
#include "Cns.h"
#include "Cns_server.h"
#include "Cthread_api.h"
#include "serrno.h"
#include "u64subr.h"

Cns_init_dbpkg()
{
	int i;

	return (0);
}

static void
Cns_mysql_error(func, sql_method, dbfd)
char *func;
char *sql_method;
struct Cns_dbfd *dbfd;
{
	extern char db_name[33];
	extern char db_pwd[33];
	extern char db_srvr[33];
	extern char db_user[33];
	int error = mysql_errno (&dbfd->mysql);

	nslogit (func, "%s error: %s\n", sql_method, mysql_error (&dbfd->mysql));
	if (error == CR_SERVER_GONE_ERROR || error == CR_SERVER_LOST) {
		nslogit (func, "Trying to reconnect\n");
		Cns_closedb (dbfd);
		if (Cns_opendb (db_srvr, db_user, db_pwd, db_name, dbfd) < 0)
			nslogit (func, "Reconnect failed\n");
	}
	serrno = SEINTERNAL;
}

Cns_abort_tr(dbfd)
struct Cns_dbfd *dbfd;
{
	(void) mysql_query (&dbfd->mysql, "ROLLBACK");
	dbfd->tr_mode = 0;
	dbfd->tr_started = 0;
	return (0);
}

Cns_closedb(dbfd)
struct Cns_dbfd *dbfd;
{
	mysql_close (&dbfd->mysql);
	return (0);
}

Cns_decode_class_entry(row, lock, rec_addr, class_entry)
MYSQL_ROW row;
int lock;
Cns_dbrec_addr *rec_addr;
struct Cns_class_metadata *class_entry;
{
	int i = 0;

	if (lock)
		strcpy (*rec_addr, row[i++]);
	class_entry->classid = atoi (row[i++]);
	strcpy (class_entry->name, row[i++]);
	class_entry->uid = atoi (row[i++]);
	class_entry->gid = atoi (row[i++]);
	class_entry->min_filesize = atoi (row[i++]);
	class_entry->max_filesize = atoi (row[i++]);
	class_entry->flags = atoi (row[i++]);
	class_entry->maxdrives = atoi (row[i++]);
	class_entry->max_segsize = atoi (row[i++]);
	class_entry->migr_time_interval = atoi (row[i++]);
	class_entry->mintime_beforemigr = atoi (row[i++]);
	class_entry->nbcopies = atoi (row[i++]);
	class_entry->nbdirs_using_class = atoi (row[i++]);
	class_entry->retenp_on_disk = atoi (row[i]);
}

Cns_decode_fmd_entry(row, lock, rec_addr, fmd_entry)
MYSQL_ROW row;
int lock;
Cns_dbrec_addr *rec_addr;
struct Cns_file_metadata *fmd_entry;
{
	int i = 0;

	if (lock)
		strcpy (*rec_addr, row[i++]);
	fmd_entry->fileid = strtou64 (row[i++]);
	fmd_entry->parent_fileid = strtou64 (row[i++]);
	strcpy (fmd_entry->guid, row[i] ? row[i] : "");
	i++;
	strcpy (fmd_entry->name, row[i++]);
	fmd_entry->filemode = atoi (row[i++]);
	fmd_entry->nlink = atoi (row[i++]);
	fmd_entry->uid = atoi (row[i++]);
	fmd_entry->gid = atoi (row[i++]);
	fmd_entry->filesize = strtou64 (row[i++]);
	fmd_entry->atime = atoi (row[i++]);
	fmd_entry->mtime = atoi (row[i++]);
	fmd_entry->ctime = atoi (row[i++]);
	fmd_entry->fileclass = atoi (row[i++]);
	fmd_entry->status = *row[i++];
	strcpy (fmd_entry->csumtype, row[i++]);
	strcpy (fmd_entry->csumvalue, row[i++]);
	strcpy (fmd_entry->acl, row[i]);
}

Cns_decode_rep_entry (row, lock, rec_addr, rep_entry)
MYSQL_ROW row;
int lock;
Cns_dbrec_addr *rec_addr;
struct Cns_file_replica *rep_entry;
{
	int i = 0;

	if (lock)
		strcpy (*rec_addr, row[i++]);
	rep_entry->fileid = strtou64 (row[i++]);
	rep_entry->nbaccesses = strtou64 (row[i++]);
	rep_entry->atime = atoi (row[i++]);
	rep_entry->ptime = atoi (row[i++]);
	rep_entry->status = *row[i++];
	rep_entry->f_type = row[i] ? *row[i] : '\0';
	i++;
	strcpy (rep_entry->poolname, row[i++]);
	strcpy (rep_entry->host, row[i++]);
	strcpy (rep_entry->fs, row[i++]);
	strcpy (rep_entry->sfn, row[i]);
}

Cns_decode_smd_entry(row, lock, rec_addr, smd_entry)
MYSQL_ROW row;
int lock;
Cns_dbrec_addr *rec_addr;
struct Cns_seg_metadata *smd_entry;
{
	unsigned int blockid_tmp[4];
	int i = 0;

	if (lock)
		strcpy (*rec_addr, row[i++]);
	smd_entry->s_fileid = strtou64 (row[i++]);
	smd_entry->copyno = atoi (row[i++]);
	smd_entry->fsec = atoi (row[i++]);
	smd_entry->segsize = strtou64 (row[i++]);
	smd_entry->compression = atoi (row[i++]);
	smd_entry->s_status = *row[i++];
	strcpy (smd_entry->vid, row[i++]);
	smd_entry->side = atoi (row[i++]);
	smd_entry->fseq = atoi (row[i++]);
	sscanf (row[i], "%02x%02x%02x%02x", &blockid_tmp[0], &blockid_tmp[1],
	    &blockid_tmp[2], &blockid_tmp[3]);
	for (i = 0; i < 4; i++)
	  smd_entry->blockid[i] = blockid_tmp[i];
}

Cns_decode_tppool_entry(row, lock, rec_addr, tppool_entry)
MYSQL_ROW row;
int lock;
Cns_dbrec_addr *rec_addr;
struct Cns_tp_pool *tppool_entry;
{
	int i = 0;

	if (lock)
		strcpy (*rec_addr, row[i++]);
	tppool_entry->classid = atoi (row[i++]);
	strcpy (tppool_entry->tape_pool, row[i]);
}

Cns_delete_class_entry(dbfd, rec_addr)
struct Cns_dbfd *dbfd;
Cns_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM Cns_class_metadata WHERE ROWID = %s";
	char func[23];
	char sql_stmt[70];

	strcpy (func, "Cns_delete_class_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "DELETE", dbfd);
		return (-1);
	}
	return (0);
}

Cns_delete_fmd_entry(dbfd, rec_addr)
struct Cns_dbfd *dbfd;
Cns_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM Cns_file_metadata WHERE ROWID = %s";
	char func[21];
	char sql_stmt[70];

	strcpy (func, "Cns_delete_fmd_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "DELETE", dbfd);
		return (-1);
	}
	return (0);
}

Cns_delete_lnk_entry(dbfd, rec_addr)
struct Cns_dbfd *dbfd;
Cns_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM Cns_symlinks WHERE ROWID = %s";
	char func[21];
	char sql_stmt[70];

	strcpy (func, "Cns_delete_lnk_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "DELETE", dbfd);
		return (-1);
	}
	return (0);
}

Cns_delete_rep_entry(dbfd, rec_addr)
struct Cns_dbfd *dbfd;
Cns_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM Cns_file_replica WHERE ROWID = %s";
	char func[21];
	char sql_stmt[70];

	strcpy (func, "Cns_delete_rep_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "DELETE", dbfd);
		return (-1);
	}
	return (0);
}

Cns_delete_smd_entry(dbfd, rec_addr)
struct Cns_dbfd *dbfd;
Cns_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM Cns_seg_metadata WHERE ROWID = %s";
	char func[21];
	char sql_stmt[70];

	strcpy (func, "Cns_delete_smd_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "DELETE", dbfd);
		return (-1);
	}
	return (0);
}

Cns_delete_tppool_entry(dbfd, rec_addr)
struct Cns_dbfd *dbfd;
Cns_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM Cns_tp_pool WHERE ROWID = %s";
	char func[24];
	char sql_stmt[70];

	strcpy (func, "Cns_delete_tppool_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "DELETE", dbfd);
		return (-1);
	}
	return (0);
}

Cns_delete_umd_entry(dbfd, rec_addr)
struct Cns_dbfd *dbfd;
Cns_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM Cns_user_metadata WHERE ROWID = %s";
	char func[21];
	char sql_stmt[70];

	strcpy (func, "Cns_delete_umd_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "DELETE", dbfd);
		return (-1);
	}
	return (0);
}

Cns_end_tr(dbfd)
struct Cns_dbfd *dbfd;
{
	(void) mysql_query (&dbfd->mysql, "COMMIT");
	dbfd->tr_mode = 0;
	dbfd->tr_started = 0;
	return (0);
}

Cns_exec_query(func, dbfd, sql_stmt, res)
char *func;
struct Cns_dbfd *dbfd;
char *sql_stmt;
MYSQL_RES **res;
{
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "mysql_query", dbfd);
		return (-1);
	}
	if ((*res = mysql_store_result (&dbfd->mysql)) == NULL) {
		Cns_mysql_error (func, "mysql_store_res", dbfd);
		return (-1);
	}
	return (0);
}

Cns_get_class_by_id(dbfd, classid, class_entry, lock, rec_addr)
struct Cns_dbfd *dbfd;
int classid;
struct Cns_class_metadata *class_entry;
int lock;
Cns_dbrec_addr *rec_addr;
{
	char func[20];
	static char query[] =
		"SELECT \
		 CLASSID, NAME, OWNER_UID, GID, MIN_FILESIZE, MAX_FILESIZE, \
		 FLAGS, MAXDRIVES, MAX_SEGSIZE, MIGR_TIME_INTERVAL, \
		 MINTIME_BEFOREMIGR, NBCOPIES, \
		 NBDIRS_USING_CLASS, RETENP_ON_DISK \
		FROM Cns_class_metadata \
		WHERE classid = %d";
	static char query4upd[] =
		"SELECT ROWID, \
		 CLASSID, NAME, OWNER_UID, GID, MIN_FILESIZE, MAX_FILESIZE, \
		 FLAGS, MAXDRIVES, MAX_SEGSIZE, MIGR_TIME_INTERVAL, \
		 MINTIME_BEFOREMIGR, NBCOPIES, \
		 NBDIRS_USING_CLASS, RETENP_ON_DISK \
		FROM Cns_class_metadata \
		WHERE classid = %d \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_class_by_id");
	sprintf (sql_stmt, lock ? query4upd : query, classid);
	if (Cns_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}
	Cns_decode_class_entry (row, lock, rec_addr, class_entry);
	mysql_free_result (res);
	return (0);
}

Cns_get_class_by_name(dbfd, class_name, class_entry, lock, rec_addr)
struct Cns_dbfd *dbfd;
char *class_name;
struct Cns_class_metadata *class_entry;
int lock;
Cns_dbrec_addr *rec_addr;
{
	char func[22];
	static char query[] =
		"SELECT \
		 CLASSID, NAME, OWNER_UID, GID, MIN_FILESIZE, MAX_FILESIZE, \
		 FLAGS, MAXDRIVES, MAX_SEGSIZE, MIGR_TIME_INTERVAL, \
		 MINTIME_BEFOREMIGR, NBCOPIES, \
		 NBDIRS_USING_CLASS, RETENP_ON_DISK \
		FROM Cns_class_metadata \
		WHERE name = '%s'";
	static char query4upd[] =
		"SELECT ROWID, \
		 CLASSID, NAME, OWNER_UID, GID, MIN_FILESIZE, MAX_FILESIZE, \
		 FLAGS, MAXDRIVES, MAX_SEGSIZE, MIGR_TIME_INTERVAL, \
		 MINTIME_BEFOREMIGR, NBCOPIES, \
		 NBDIRS_USING_CLASS, RETENP_ON_DISK \
		FROM Cns_class_metadata \
		WHERE name = '%s' \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_class_by_name");
	sprintf (sql_stmt, lock ? query4upd : query, class_name);
	if (Cns_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}
	Cns_decode_class_entry (row, lock, rec_addr, class_entry);
	mysql_free_result (res);
	return (0);
}

Cns_get_fmd_by_fileid(dbfd, fileid, fmd_entry, lock, rec_addr)
struct Cns_dbfd *dbfd;
u_signed64 fileid;
struct Cns_file_metadata *fmd_entry;
int lock;
Cns_dbrec_addr *rec_addr;
{
	char fileid_str[21];
	char func[22];
	static char query[] =
		"SELECT \
		 FILEID, PARENT_FILEID, GUID, NAME, FILEMODE, NLINK, OWNER_UID, \
		 GID, FILESIZE, ATIME, MTIME, CTIME, FILECLASS, STATUS, CSUMTYPE, \
		 CSUMVALUE, ACL \
		FROM Cns_file_metadata \
		WHERE fileid = %s";
	static char query4upd[] =
		"SELECT ROWID, \
		 FILEID, PARENT_FILEID, GUID, NAME, FILEMODE, NLINK, OWNER_UID, \
		 GID, FILESIZE, ATIME, MTIME, CTIME, FILECLASS, STATUS, CSUMTYPE, \
		 CSUMVALUE, ACL \
		FROM Cns_file_metadata \
		WHERE fileid = %s \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_fmd_by_fileid");
	sprintf (sql_stmt, lock ? query4upd : query,
	    u64tostr (fileid, fileid_str, -1));
	if (Cns_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}
	Cns_decode_fmd_entry (row, lock, rec_addr, fmd_entry);
	mysql_free_result (res);
	return (0);
}

Cns_get_fmd_by_fullid(dbfd, parent_fileid, name, fmd_entry, lock, rec_addr)
struct Cns_dbfd *dbfd;
u_signed64 parent_fileid;
char *name;
struct Cns_file_metadata *fmd_entry;
int lock;
Cns_dbrec_addr *rec_addr;
{
	char escaped_name[CA_MAXNAMELEN*2+1];
	char func[22];
	char parent_fileid_str[21];
	static char query[] =
		"SELECT \
		 FILEID, PARENT_FILEID, GUID, NAME, FILEMODE, NLINK, OWNER_UID, \
		 GID, FILESIZE, ATIME, MTIME, CTIME, FILECLASS, STATUS, CSUMTYPE, \
		 CSUMVALUE, ACL \
		FROM Cns_file_metadata \
		WHERE parent_fileid = %s \
		AND name = '%s'";
	static char query4upd[] =
		"SELECT ROWID, \
		 FILEID, PARENT_FILEID, GUID, NAME, FILEMODE, NLINK, OWNER_UID, \
		 GID, FILESIZE, ATIME, MTIME, CTIME, FILECLASS, STATUS, CSUMTYPE, \
		 CSUMVALUE, ACL \
		FROM Cns_file_metadata \
		WHERE parent_fileid = %s \
		AND name = '%s' \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_fmd_by_fullid");
	mysql_real_escape_string (&dbfd->mysql, escaped_name, name,
	    strlen (name));
	sprintf (sql_stmt, lock ? query4upd : query,
	    u64tostr (parent_fileid, parent_fileid_str, 0), escaped_name);
	if (Cns_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}
	Cns_decode_fmd_entry (row, lock, rec_addr, fmd_entry);
	mysql_free_result (res);
	return (0);
}

Cns_get_fmd_by_guid(dbfd, guid, fmd_entry, lock, rec_addr)
struct Cns_dbfd *dbfd;
char *guid;
struct Cns_file_metadata *fmd_entry;
int lock;
Cns_dbrec_addr *rec_addr;
{
	char func[20];
	static char query[] =
		"SELECT \
		 FILEID, PARENT_FILEID, GUID, NAME, FILEMODE, NLINK, OWNER_UID, \
		 GID, FILESIZE, ATIME, MTIME, CTIME, FILECLASS, STATUS, CSUMTYPE, \
		 CSUMVALUE, ACL \
		FROM Cns_file_metadata \
		WHERE guid = '%s'";
	static char query4upd[] =
		"SELECT ROWID, \
		 FILEID, PARENT_FILEID, GUID, NAME, FILEMODE, NLINK, OWNER_UID, \
		 GID, FILESIZE, ATIME, MTIME, CTIME, FILECLASS, STATUS, CSUMTYPE, \
		 CSUMVALUE, ACL \
		FROM Cns_file_metadata \
		WHERE guid = '%s' \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_fmd_by_guid");
	sprintf (sql_stmt, lock ? query4upd : query, guid);
	if (Cns_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}
	Cns_decode_fmd_entry (row, lock, rec_addr, fmd_entry);
	mysql_free_result (res);
	return (0);
}

Cns_get_fmd_by_pfid(dbfd, bod, parent_fileid, fmd_entry, getattr, endlist, dblistptr)
struct Cns_dbfd *dbfd;
int bod;
u_signed64 parent_fileid;
struct Cns_file_metadata *fmd_entry;
int getattr;
int endlist;
DBLISTPTR *dblistptr;
{
	char func[20];
	char parent_fileid_str[21];
	static char query[] =
		"SELECT \
		 FILEID, PARENT_FILEID, GUID, NAME, FILEMODE, NLINK, OWNER_UID, \
		 GID, FILESIZE, ATIME, MTIME, CTIME, FILECLASS, STATUS, CSUMTYPE, \
		 CSUMVALUE, ACL \
		FROM Cns_file_metadata \
		WHERE parent_fileid = %s \
		ORDER BY name";
	static char query_name[] =
		"SELECT \
		 NAME \
		FROM Cns_file_metadata \
		WHERE parent_fileid = %s \
		ORDER BY name";
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_fmd_by_pfid");
	if (endlist) {
		if (*dblistptr)
			mysql_free_result (*dblistptr);
		return (1);
	}
	if (bod) {
		sprintf (sql_stmt, getattr ? query : query_name,
		    u64tostr (parent_fileid, parent_fileid_str, -1));
		if (Cns_exec_query (func, dbfd, sql_stmt, dblistptr))
			return (-1);
	}
	if ((row = mysql_fetch_row (*dblistptr)) == NULL)
		return (1);
	if (! getattr)
		strcpy (fmd_entry->name, row[0]);
	else
		Cns_decode_fmd_entry (row, 0, NULL, fmd_entry);
	return (0);
}

Cns_get_lnk_by_fileid(dbfd, fileid, lnk_entry, lock, rec_addr)
struct Cns_dbfd *dbfd;
u_signed64 fileid;
struct Cns_symlinks *lnk_entry;
int lock;
Cns_dbrec_addr *rec_addr;
{
	char fileid_str[21];
	char func[22];
	static char query[] =
		"SELECT \
		 FILEID, LINKNAME \
		FROM Cns_symlinks \
		WHERE fileid = %s";
	static char query4upd[] =
		"SELECT ROWID, \
		 FILEID, LINKNAME \
		FROM Cns_symlinks \
		WHERE fileid = %s \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_lnk_by_fileid");
	sprintf (sql_stmt, lock ? query4upd : query,
	    u64tostr (fileid, fileid_str, -1));
	if (Cns_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}
	if (lock)
		strcpy (*rec_addr, row[0]);
	lnk_entry->fileid = fileid;
	strcpy (lnk_entry->linkname, row[lock ? 2 : 1]);
	mysql_free_result (res);
	return (0);
}

Cns_get_max_copyno (dbfd, fileid, copyno)
struct Cns_dbfd *dbfd;
u_signed64 fileid;
int *copyno;
{
	char fileid_str[21];
	char func[19];
	static char query[] =
		"SELECT COPYNO \
		FROM Cns_seg_metadata \
		WHERE s_fileid = %s \
		ORDER BY copyno DESC LIMIT 1";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_max_copyno");
	sprintf (sql_stmt, query, u64tostr (fileid, fileid_str, -1));
	if (Cns_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}
	*copyno = atoi (row[0]);
	mysql_free_result (res);
	return (0);
}

Cns_get_rep_by_sfn(dbfd, sfn, rep_entry, lock, rec_addr)
struct Cns_dbfd *dbfd;
char *sfn;
struct Cns_file_replica *rep_entry;
int lock;
Cns_dbrec_addr *rec_addr;
{
	char func[19];
	static char query[] =
		"SELECT \
		 FILEID, NBACCESSES, ATIME, PTIME, STATUS, \
		 F_TYPE, POOLNAME, HOST, FS, SFN \
		FROM Cns_file_replica \
		WHERE sfn = '%s'";
	static char query4upd[] =
		"SELECT ROWID, \
		 FILEID, NBACCESSES, ATIME, PTIME, STATUS, \
		 F_TYPE, POOLNAME, HOST, FS, SFN \
		FROM Cns_file_replica \
		WHERE sfn = '%s' \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1236];

	strcpy (func, "Cns_get_rep_by_sfn");
	sprintf (sql_stmt, lock ? query4upd : query, sfn);
	if (Cns_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}
	Cns_decode_rep_entry (row, lock, rec_addr, rep_entry);
	mysql_free_result (res);
	return (0);
}

Cns_get_smd_by_fullid(dbfd, fileid, copyno, fsec, smd_entry, lock, rec_addr)
struct Cns_dbfd *dbfd;
u_signed64 fileid;
int copyno;
int fsec;
struct Cns_seg_metadata *smd_entry;
int lock;
Cns_dbrec_addr *rec_addr;
{
	char fileid_str[21];
	char func[22];
	static char query[] =
		"SELECT \
		 S_FILEID, COPYNO, FSEC, SEGSIZE, COALESCE(COMPRESSION,100), \
		 S_STATUS, VID, SIDE, FSEQ, BLOCKID, CHECKSUM_NAME, CHECKSUM \
		FROM Cns_seg_metadata \
		WHERE s_fileid = %s \
		AND copyno = %d AND fsec = %d";
	static char query4upd[] =
		"SELECT ROWID, \
		 S_FILEID, COPYNO, FSEC, SEGSIZE, COALESCE(COMPRESSION,100), \
		 S_STATUS, VID, SIDE, FSEQ, BLOCKID, CHECKSUM_NAME, CHECKSUM  \
		FROM Cns_seg_metadata \
		WHERE s_fileid = %s \
		AND copyno = %d AND fsec = %d \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_smd_by_fullid");
	sprintf (sql_stmt, lock ? query4upd : query,
	    u64tostr (fileid, fileid_str, -1), copyno, fsec);
	if (Cns_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}
	Cns_decode_smd_entry (row, lock, rec_addr, smd_entry);
	mysql_free_result (res);
	return (0);
}

Cns_get_smd_by_pfid(dbfd, bof, fileid, smd_entry, lock, rec_addr, endlist, dblistptr)
struct Cns_dbfd *dbfd;
int bof;
u_signed64 fileid;
struct Cns_seg_metadata *smd_entry;
int lock;
Cns_dbrec_addr *rec_addr;
int endlist;
DBLISTPTR *dblistptr;
{
	char fileid_str[21];
	char func[20];
	static char query[] =
		"SELECT \
		 S_FILEID, COPYNO, FSEC, SEGSIZE, COALESCE(COMPRESSION,100), \
		 S_STATUS, VID, SIDE, FSEQ, BLOCKID, CHECKSUM_NAME, CHECKSUM \
		FROM Cns_seg_metadata \
		WHERE s_fileid = %s \
		ORDER BY copyno, fsec";
	static char query4upd[] =
		"SELECT ROWID, \
		 S_FILEID, COPYNO, FSEC, SEGSIZE, COALESCE(COMPRESSION,100), \
		 S_STATUS, VID, SIDE, FSEQ, BLOCKID, CHECKSUM_NAME, CHECKSUM \
		FROM Cns_seg_metadata \
		WHERE s_fileid = %s \
		ORDER BY copyno, fsec \
		FOR UPDATE";
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_smd_by_pfid");
	if (endlist) {
		if (*dblistptr) {
			mysql_free_result (*dblistptr);
			*dblistptr = NULL;
		}
		return (1);
	}
	if (bof) {
		sprintf (sql_stmt, lock ? query4upd : query,
		    u64tostr (fileid, fileid_str, -1));
		if (Cns_exec_query (func, dbfd, sql_stmt, dblistptr))
			return (-1);
	}
	if ((row = mysql_fetch_row (*dblistptr)) == NULL)
		return (1);
	Cns_decode_smd_entry (row, lock, rec_addr, smd_entry);
	return (0);
}

Cns_get_smd_by_vid(dbfd, bov, vid, smd_entry, endlist, dblistptr)
struct Cns_dbfd *dbfd;
int bov;
char *vid;
struct Cns_seg_metadata *smd_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	char func[19];
	static char query[] =
		"SELECT \
		 S_FILEID, COPYNO, FSEC, SEGSIZE, COALESCE(COMPRESSION,100), \
		 S_STATUS, VID, SIDE, FSEQ, BLOCKID, CHECKSUM_NAME, CHECKSUM \
		FROM Cns_seg_metadata \
		WHERE vid = '%s' \
		ORDER BY side, fseq";
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_smd_by_vid");
	if (endlist) {
		if (*dblistptr)
			mysql_free_result (*dblistptr);
		return (1);
	}
	if (bov) {
		sprintf (sql_stmt, query, vid);
		if (Cns_exec_query (func, dbfd, sql_stmt, dblistptr))
			return (-1);
	}
	if ((row = mysql_fetch_row (*dblistptr)) == NULL)
		return (1);
	Cns_decode_smd_entry (row, 0, NULL, smd_entry);
	return (0);
}

Cns_get_tppool_by_cid(dbfd, bol, classid, tppool_entry, lock, rec_addr, endlist, dblistptr)
struct Cns_dbfd *dbfd;
int bol;
int classid;
struct Cns_tp_pool *tppool_entry;
int lock;
Cns_dbrec_addr *rec_addr;
int endlist;
DBLISTPTR *dblistptr;
{
	char func[22];
	static char query[] =
		"SELECT \
		 CLASSID, TAPE_POOL \
		FROM Cns_tp_pool \
		WHERE classid = %d";
	static char query4upd[] =
		"SELECT ROWID, \
		 CLASSID, TAPE_POOL \
		FROM Cns_tp_pool \
		WHERE classid = %d \
		FOR UPDATE";
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_tppool_by_cid");
	if (endlist) {
		if (*dblistptr)
			mysql_free_result (*dblistptr);
		return (1);
	}
	if (bol) {
		sprintf (sql_stmt, lock ? query4upd : query, classid);
		if (Cns_exec_query (func, dbfd, sql_stmt, dblistptr))
			return (-1);
	}
	if ((row = mysql_fetch_row (*dblistptr)) == NULL)
		return (1);
	Cns_decode_tppool_entry (row, lock, rec_addr, tppool_entry);
	return (0);
}

Cns_get_umd_by_fileid(dbfd, fileid, umd_entry, lock, rec_addr)
struct Cns_dbfd *dbfd;
u_signed64 fileid;
struct Cns_user_metadata *umd_entry;
int lock;
Cns_dbrec_addr *rec_addr;
{
	char fileid_str[21];
	char func[22];
	static char query[] =
		"SELECT \
		 U_FILEID, COMMENTS \
		FROM Cns_user_metadata \
		WHERE u_fileid = %s";
	static char query4upd[] =
		"SELECT ROWID, \
		 U_FILEID, COMMENTS \
		FROM Cns_user_metadata \
		WHERE u_fileid = %s \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_umd_by_fileid");
	sprintf (sql_stmt, lock ? query4upd : query,
	    u64tostr (fileid, fileid_str, -1));
	if (Cns_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}
	if (lock)
		strcpy (*rec_addr, row[0]);
	umd_entry->u_fileid = fileid;
	strcpy (umd_entry->comments, row[lock ? 2 : 1]);
	mysql_free_result (res);
	return (0);
}

Cns_insert_class_entry(dbfd, class_entry)
struct Cns_dbfd *dbfd;
struct Cns_class_metadata *class_entry;
{
	char func[23];
	static char insert_stmt[] =
		"INSERT INTO Cns_class_metadata \
		(CLASSID, NAME, OWNER_UID, GID, MIN_FILESIZE, MAX_FILESIZE, \
		 FLAGS, MAXDRIVES, MAX_SEGSIZE, MIGR_TIME_INTERVAL, \
		 MINTIME_BEFOREMIGR, NBCOPIES, \
		 NBDIRS_USING_CLASS, RETENP_ON_DISK) \
		VALUES \
		(%d, '%s', %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d)";
	char sql_stmt[1024];

	strcpy (func, "Cns_insert_class_entry");
	sprintf (sql_stmt, insert_stmt,
	    class_entry->classid, class_entry->name,
	    class_entry->uid, class_entry->gid,
	    class_entry->min_filesize, class_entry->max_filesize,
	    class_entry->flags, class_entry->maxdrives,
	    class_entry->max_segsize, class_entry->migr_time_interval,
	    class_entry->mintime_beforemigr, class_entry->nbcopies,
	    class_entry->nbdirs_using_class, class_entry->retenp_on_disk);
	
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			Cns_mysql_error (func, "INSERT", dbfd);
		}
		return (-1);
	}
	return (0);
}

Cns_insert_fmd_entry(dbfd, fmd_entry)
struct Cns_dbfd *dbfd;
struct Cns_file_metadata *fmd_entry;
{
	char escaped_name[CA_MAXNAMELEN*2+1];
	char fileid_str[21];
	char filesize_str[21];
	char func[21];
	static char insert_stmt[] =
		"INSERT INTO Cns_file_metadata \
		(FILEID, PARENT_FILEID, GUID, NAME, FILEMODE, NLINK, OWNER_UID, \
		 GID, FILESIZE, ATIME, MTIME, CTIME, FILECLASS, STATUS, CSUMTYPE, \
		 CSUMVALUE, ACL) \
		VALUES \
		(%s, %s, '%s', '%s', %d, %d, %d, %d, %s, %d, %d, %d, %d, '%c', '%s', '%s', '%s')";
	static char insert_stmt_null_guid[] =
		"INSERT INTO Cns_file_metadata \
		(FILEID, PARENT_FILEID, GUID, NAME, FILEMODE, NLINK, OWNER_UID, \
		 GID, FILESIZE, ATIME, MTIME, CTIME, FILECLASS, STATUS, CSUMTYPE, \
		 CSUMVALUE, ACL) \
		VALUES \
		(%s, %s, NULL, '%s', %d, %d, %d, %d, %s, %d, %d, %d, %d, '%c', '%s', '%s', '%s')";
	char parent_fileid_str[21];
	char sql_stmt[1024];

	strcpy (func, "Cns_insert_fmd_entry");
	mysql_real_escape_string (&dbfd->mysql, escaped_name, fmd_entry->name,
	    strlen (fmd_entry->name));
	if (*fmd_entry->guid)
		sprintf (sql_stmt, insert_stmt,
		    u64tostr (fmd_entry->fileid, fileid_str, -1),
		    u64tostr (fmd_entry->parent_fileid, parent_fileid_str, -1),
		    fmd_entry->guid, escaped_name, fmd_entry->filemode, fmd_entry->nlink,
		    fmd_entry->uid, fmd_entry->gid,
		    u64tostr (fmd_entry->filesize, filesize_str, -1),
		    (int)fmd_entry->atime, (int)fmd_entry->mtime, (int)fmd_entry->ctime,
		    fmd_entry->fileclass, fmd_entry->status, fmd_entry->csumtype,
		    fmd_entry->csumvalue, fmd_entry->acl);
	else	/* must insert a NULL guid and not an empty string because of
		   the UNIQUE constraint on guid */
		sprintf (sql_stmt, insert_stmt_null_guid,
		    u64tostr (fmd_entry->fileid, fileid_str, -1),
		    u64tostr (fmd_entry->parent_fileid, parent_fileid_str, -1),
		    escaped_name, fmd_entry->filemode, fmd_entry->nlink,
		    fmd_entry->uid, fmd_entry->gid,
		    u64tostr (fmd_entry->filesize, filesize_str, -1),
		    (int)fmd_entry->atime, (int)fmd_entry->mtime, (int)fmd_entry->ctime,
		    fmd_entry->fileclass, fmd_entry->status, fmd_entry->csumtype,
		    fmd_entry->csumvalue, fmd_entry->acl);

	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			Cns_mysql_error (func, "INSERT", dbfd);
		}
		return (-1);
	}
	return (0);
}

Cns_insert_lnk_entry(dbfd, lnk_entry)
struct Cns_dbfd *dbfd;
struct Cns_symlinks *lnk_entry;
{
	char fileid_str[21];
	char func[21];
	static char insert_stmt[] =
		"INSERT INTO Cns_symlinks \
		(FILEID, LINKNAME) \
		VALUES \
		(%s, '%s')";
	char sql_stmt[1107];

	strcpy (func, "Cns_insert_lnk_entry");
	sprintf (sql_stmt, insert_stmt,
	    u64tostr (lnk_entry->fileid, fileid_str, -1),
	    lnk_entry->linkname);

	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			Cns_mysql_error (func, "INSERT", dbfd);
		}
		return (-1);
	}
	return (0);
}

Cns_insert_rep_entry(dbfd, rep_entry)
struct Cns_dbfd *dbfd;
struct Cns_file_replica *rep_entry;
{
	char fileid_str[21];
	char func[21];
	static char insert_stmt[] =
		"INSERT INTO Cns_file_replica \
		(FILEID, NBACCESSES, ATIME, PTIME, STATUS, \
		 F_TYPE, POOLNAME, HOST, FS, SFN) \
		VALUES \
		(%s, %s, %d, %d, '%c', '%c', '%s', '%s', '%s', '%s')";
	static char insert_stmt_null_ftype[] =
		"INSERT INTO Cns_file_replica \
		(FILEID, NBACCESSES, ATIME, PTIME, STATUS, \
		 F_TYPE, POOLNAME, HOST, FS, SFN) \
		VALUES \
		(%s, %s, %d, %d, '%c', NULL, '%s', '%s', '%s', '%s')";
	char nbacces_str[21];
	char sql_stmt[1476];

	strcpy (func, "Cns_insert_rep_entry");
	if (rep_entry->f_type)
		sprintf (sql_stmt, insert_stmt,
		    u64tostr (rep_entry->fileid, fileid_str, -1),
		    u64tostr (rep_entry->nbaccesses, nbacces_str, -1),
		    rep_entry->atime, rep_entry->ptime, rep_entry->status,
		    rep_entry->f_type, rep_entry->poolname, rep_entry->host,
		    rep_entry->fs, rep_entry->sfn);
	else
		sprintf (sql_stmt, insert_stmt_null_ftype,
		    u64tostr (rep_entry->fileid, fileid_str, -1),
		    u64tostr (rep_entry->nbaccesses, nbacces_str, -1),
		    rep_entry->atime, rep_entry->ptime, rep_entry->status,
		    rep_entry->poolname, rep_entry->host,
		    rep_entry->fs, rep_entry->sfn);

	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			Cns_mysql_error (func, "INSERT", dbfd);
		}
		return (-1);
	}
	return (0);
}

Cns_insert_smd_entry(dbfd, smd_entry)
struct Cns_dbfd *dbfd;
struct Cns_seg_metadata *smd_entry;
{
	char fileid_str[21];
	char func[21];
	static char insert_stmt[] =
		"INSERT INTO Cns_seg_metadata \
		(S_FILEID, COPYNO, FSEC, \
		 SEGSIZE, COMPRESSION, S_STATUS, \
		 VID, SIDE, FSEQ, BLOCKID, \
		CHECKSUM_NAME, CHECKSUM) \
		VALUES \
		(%s, %d, %d, %s, %d, '%c', '%s', %d, %d, '%02x%02x%02x%02x', '%s', %d)";
	char segsize_str[21];
	char sql_stmt[1024];

	strcpy (func, "Cns_insert_smd_entry");
	sprintf (sql_stmt, insert_stmt,
	    u64tostr (smd_entry->s_fileid, fileid_str, -1),
	    smd_entry->copyno, smd_entry->fsec,
	    u64tostr (smd_entry->segsize, segsize_str, -1),
	    smd_entry->compression, smd_entry->s_status,
	    smd_entry->vid, smd_entry->side, smd_entry->fseq,
	    smd_entry->blockid[0], smd_entry->blockid[1],
	    smd_entry->blockid[2], smd_entry->blockid[3],
	    smd_entry->checksum_name, smd_entry->checksum);

	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			Cns_mysql_error (func, "INSERT", dbfd);
		}
		return (-1);
	}
	return (0);
}

Cns_insert_tppool_entry(dbfd, tppool_entry)
struct Cns_dbfd *dbfd;
struct Cns_tp_pool *tppool_entry;
{
	char func[24];
	static char insert_stmt[] =
		"INSERT INTO Cns_tp_pool \
		(CLASSID, TAPE_POOL) \
		VALUES \
		(%d, '%s')";
	char sql_stmt[1024];

	strcpy (func, "Cns_insert_tppool_entry");
	sprintf (sql_stmt, insert_stmt,
	    tppool_entry->classid, tppool_entry->tape_pool);

	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			Cns_mysql_error (func, "INSERT", dbfd);
		}
		return (-1);
	}
	return (0);
}

Cns_insert_umd_entry(dbfd, umd_entry)
struct Cns_dbfd *dbfd;
struct Cns_user_metadata *umd_entry;
{
	char fileid_str[21];
	char func[21];
	static char insert_stmt[] =
		"INSERT INTO Cns_user_metadata \
		(U_FILEID, COMMENTS) \
		VALUES \
		(%s, '%s')";
	char sql_stmt[1024];

	strcpy (func, "Cns_insert_umd_entry");
	sprintf (sql_stmt, insert_stmt,
	    u64tostr (umd_entry->u_fileid, fileid_str, -1),
	    umd_entry->comments);

	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			Cns_mysql_error (func, "INSERT", dbfd);
		}
		return (-1);
	}
	return (0);
}

Cns_list_class_entry(dbfd, bol, class_entry, endlist, dblistptr)
struct Cns_dbfd *dbfd;
int bol;
struct Cns_class_metadata *class_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	char func[21];
	static char query[] =
		"SELECT \
		 CLASSID, NAME, \
		 OWNER_UID, GID, \
		 MIN_FILESIZE, MAX_FILESIZE, \
		 FLAGS, MAXDRIVES, \
		 MAX_SEGSIZE, MIGR_TIME_INTERVAL, \
		 MINTIME_BEFOREMIGR, NBCOPIES, \
		 NBDIRS_USING_CLASS, RETENP_ON_DISK \
		FROM Cns_class_metadata \
		ORDER BY classid";
	MYSQL_ROW row;

	strcpy (func, "Cns_list_class_entry");
	if (endlist) {
		if (*dblistptr)
			mysql_free_result (*dblistptr);
		return (1);
	}
	if (bol) {
		if (Cns_exec_query (func, dbfd, query, dblistptr))
			return (-1);
	}
	if ((row = mysql_fetch_row (*dblistptr)) == NULL)
		return (1);
	Cns_decode_class_entry (row, 0, NULL, class_entry);
	return (0);
}

Cns_list_lnk_entry(dbfd, bol, linkname, lnk_entry, endlist, dblistptr)
struct Cns_dbfd *dbfd;
int bol;
char *linkname;
struct Cns_symlinks *lnk_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	char fileid_str[21];
	char func[19];
	static char query[] =
		"SELECT \
		 FILEID, LINKNAME \
		FROM Cns_symlinks \
		WHERE linkname = '%s'";
	MYSQL_ROW row;
	char sql_stmt[1090];

	strcpy (func, "Cns_list_lnk_entry");
	if (endlist) {
		if (*dblistptr)
			mysql_free_result (*dblistptr);
		return (1);
	}
	if (bol) {
		sprintf (sql_stmt, query, linkname);
		if (Cns_exec_query (func, dbfd, sql_stmt, dblistptr))
			return (-1);
	}
	if ((row = mysql_fetch_row (*dblistptr)) == NULL)
		return (1);
	lnk_entry->fileid = strtou64 (row[0]);
	strcpy (lnk_entry->linkname, row[1]);
	return (0);
}

Cns_list_rep4admin(dbfd, bol, poolname, server, fs, rep_entry, endlist, dblistptr)
struct Cns_dbfd *dbfd;
int bol;
char *poolname;
char *server;
char *fs;
struct Cns_file_replica *rep_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	char fileid_str[21];
	char func[19];
	static char queryf[] =
		"SELECT \
		 FILEID, NBACCESSES, ATIME, PTIME, STATUS, \
		 F_TYPE, POOLNAME, HOST, FS, SFN \
		FROM Cns_file_replica \
		WHERE host = '%s' AND fs = '%s'";
	static char queryp[] =
		"SELECT \
		 FILEID, NBACCESSES, ATIME, PTIME, STATUS, \
		 F_TYPE, POOLNAME, HOST, FS, SFN \
		FROM Cns_file_replica \
		WHERE poolname = '%s'";
	static char querys[] =
		"SELECT \
		 FILEID, NBACCESSES, ATIME, PTIME, STATUS, \
		 F_TYPE, POOLNAME, HOST, FS, SFN \
		FROM Cns_file_replica \
		WHERE host = '%s'";
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_list_rep4admin");
	if (endlist) {
		if (*dblistptr)
			mysql_free_result (*dblistptr);
		return (1);
	}
	if (bol) {
		if (*poolname)
			sprintf (sql_stmt, queryp, poolname);
		else if (*fs)
			sprintf (sql_stmt, queryf, server, fs);
		else
			sprintf (sql_stmt, querys, server);
		if (Cns_exec_query (func, dbfd, sql_stmt, dblistptr))
			return (-1);
	}
	if ((row = mysql_fetch_row (*dblistptr)) == NULL)
		return (1);
	Cns_decode_rep_entry (row, 0, NULL, rep_entry);
	return (0);
}

Cns_list_rep4gc(dbfd, bol, poolname, rep_entry, endlist, dblistptr)
struct Cns_dbfd *dbfd;
int bol;
char *poolname;
struct Cns_file_replica *rep_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	char fileid_str[21];
	char func[16];
	static char query[] =
		"SELECT \
		 FILEID, NBACCESSES, ATIME, PTIME, STATUS, \
		 F_TYPE, POOLNAME, HOST, FS, SFN \
		FROM Cns_file_replica \
		WHERE poolname = '%s' AND status = '-' AND ptime < %d \
		ORDER BY atime";
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_list_rep4gc");
	if (endlist) {
		if (*dblistptr)
			mysql_free_result (*dblistptr);
		return (1);
	}
	if (bol) {
		sprintf (sql_stmt, query, poolname, time (0));
		if (Cns_exec_query (func, dbfd, sql_stmt, dblistptr))
			return (-1);
	}
	if ((row = mysql_fetch_row (*dblistptr)) == NULL)
		return (1);
	Cns_decode_rep_entry (row, 0, NULL, rep_entry);
	return (0);
}

Cns_list_rep_entry(dbfd, bol, fileid, rep_entry, lock, rec_addr, endlist, dblistptr)
struct Cns_dbfd *dbfd;
int bol;
u_signed64 fileid;
struct Cns_file_replica *rep_entry;
int lock;
Cns_dbrec_addr *rec_addr;
int endlist;
DBLISTPTR *dblistptr;
{
	char fileid_str[21];
	char func[19];
	static char query[] =
		"SELECT \
		 FILEID, NBACCESSES, ATIME, PTIME, STATUS, \
		 F_TYPE, POOLNAME, HOST, FS, SFN \
		FROM Cns_file_replica \
		WHERE fileid = %s \
		ORDER BY sfn";
	static char query4upd[] =
		"SELECT ROWID, \
		 FILEID, NBACCESSES, ATIME, PTIME, STATUS, \
		 F_TYPE, POOLNAME, HOST, FS, SFN \
		FROM Cns_file_replica \
		WHERE fileid = %s \
		ORDER BY sfn";
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_list_rep_entry");
	if (endlist) {
		if (*dblistptr)
			mysql_free_result (*dblistptr);
		return (1);
	}
	if (bol) {
		sprintf (sql_stmt, lock ? query4upd : query,
		    u64tostr (fileid, fileid_str, -1));
		if (Cns_exec_query (func, dbfd, sql_stmt, dblistptr))
			return (-1);
	}
	if ((row = mysql_fetch_row (*dblistptr)) == NULL)
		return (1);
	Cns_decode_rep_entry (row, lock, rec_addr, rep_entry);
	return (0);
}

Cns_opendb(db_srvr, db_user, db_pwd, db_name, dbfd)
char *db_srvr;
char *db_user;
char *db_pwd;
char *db_name;
struct Cns_dbfd *dbfd;
{
	char func[16];
	int ntries;

	strcpy (func, "Cns_opendb");
	(void) mysql_init (&dbfd->mysql);
	ntries = 0;
	while (1) {
		if (mysql_real_connect (&dbfd->mysql, db_srvr, db_user, db_pwd,
		    db_name, 0, NULL, 0)) return (0);
		if (ntries++ >= MAXRETRY) break;
		sleep (RETRYI);
	}
	Cns_mysql_error (func, "CONNECT", dbfd);
	return (-1);
}

Cns_start_tr(s, dbfd)
int s;
struct Cns_dbfd *dbfd;
{
	if (! dbfd->tr_started) {
		(void) mysql_query (&dbfd->mysql, "BEGIN");
		dbfd->tr_started = 1;
	}
	return (0);
}

Cns_unique_id(dbfd, unique_id)
struct Cns_dbfd *dbfd;
u_signed64 *unique_id;
{
	char func[16];
	static char insert_stmt[] =
		"INSERT INTO Cns_unique_id (ID) VALUES (3)";
	static char query_stmt[] =
		"SELECT ID FROM Cns_unique_id FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];
	u_signed64 uniqueid;
	char uniqueid_str[21];
	static char update_stmt[] =
		"UPDATE Cns_unique_id SET ID = %s";

	strcpy (func, "Cns_unique_id");

	if (Cns_exec_query (func, dbfd, query_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		if (mysql_query (&dbfd->mysql, insert_stmt)) {
			Cns_mysql_error (func, "INSERT", dbfd);
			return (-1);
		}
		*unique_id = 3;
	} else {
		uniqueid = strtou64 (row[0]) + 1;
		mysql_free_result (res);
		sprintf (sql_stmt, update_stmt, u64tostr (uniqueid, uniqueid_str, -1));
		if (mysql_query (&dbfd->mysql, sql_stmt)) {
			Cns_mysql_error (func, "UPDATE", dbfd);
			return (-1);
		}
		*unique_id = uniqueid;
	}
	return (0);
}

Cns_update_class_entry(dbfd, rec_addr, class_entry)
struct Cns_dbfd *dbfd;
Cns_dbrec_addr *rec_addr;
struct Cns_class_metadata *class_entry;
{
	char func[23];
	char sql_stmt[1024];
	static char update_stmt[] =
		"UPDATE Cns_class_metadata SET \
		NAME = '%s', OWNER_UID = %d, GID = %d, MIN_FILESIZE = %d, \
		MAX_FILESIZE = %d, FLAGS = %d, MAXDRIVES = %d, \
		MAX_SEGSIZE = %d, MIGR_TIME_INTERVAL = %d, \
		MINTIME_BEFOREMIGR = %d, NBCOPIES = %d, \
		NBDIRS_USING_CLASS = %d, RETENP_ON_DISK = %d \
		WHERE ROWID = %s";

	strcpy (func, "Cns_update_class_entry");
	sprintf (sql_stmt, update_stmt,
	    class_entry->name, class_entry->uid, class_entry->gid,
	    class_entry->min_filesize, class_entry->max_filesize,
	    class_entry->flags, class_entry->maxdrives,
	    class_entry->max_segsize, class_entry->migr_time_interval,
	    class_entry->mintime_beforemigr, class_entry->nbcopies,
	    class_entry->nbdirs_using_class, class_entry->retenp_on_disk,
	    *rec_addr);

	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "UPDATE", dbfd);
		return (-1);
	}
	return (0);
}

Cns_update_fmd_entry(dbfd, rec_addr, fmd_entry)
struct Cns_dbfd *dbfd;
Cns_dbrec_addr *rec_addr;
struct Cns_file_metadata *fmd_entry;
{
	char escaped_name[CA_MAXNAMELEN*2+1];
	char filesize_str[21];
	char func[21];
	char parent_fileid_str[21];
	char sql_stmt[1024];
	static char update_stmt[] =
		"UPDATE Cns_file_metadata SET \
		PARENT_FILEID = %s, GUID = '%s', NAME = '%s', FILEMODE = %d, \
		NLINK = %d, OWNER_UID = %d, GID = %d, FILESIZE = %s, ATIME = %d, \
		MTIME = %d, CTIME = %d, FILECLASS = %d, STATUS = '%c', \
		CSUMTYPE = '%s', CSUMVALUE = '%s', ACL = '%s' \
		WHERE ROWID = %s";
	static char update_stmt_null_guid[] =
		"UPDATE Cns_file_metadata SET \
		PARENT_FILEID = %s, GUID = NULL, NAME = '%s', FILEMODE = %d, \
		NLINK = %d, OWNER_UID = %d, GID = %d, FILESIZE = %s, ATIME = %d, \
		MTIME = %d, CTIME = %d, FILECLASS = %d, STATUS = '%c', \
		CSUMTYPE = '%s', CSUMVALUE = '%s', ACL = '%s' \
		WHERE ROWID = %s";

	strcpy (func, "Cns_update_fmd_entry");
	mysql_real_escape_string (&dbfd->mysql, escaped_name, fmd_entry->name,
	    strlen (fmd_entry->name));
	if (*fmd_entry->guid)
		sprintf (sql_stmt, update_stmt,
		    u64tostr (fmd_entry->parent_fileid, parent_fileid_str, -1),
		    fmd_entry->guid, escaped_name, fmd_entry->filemode, fmd_entry->nlink,
		    fmd_entry->uid, fmd_entry->gid,
		    u64tostr (fmd_entry->filesize, filesize_str, -1),
		    fmd_entry->atime, fmd_entry->mtime, fmd_entry->ctime,
		    fmd_entry->fileclass, fmd_entry->status, fmd_entry->csumtype,
		    fmd_entry->csumvalue, fmd_entry->acl, *rec_addr);
	else	/* must insert a NULL guid and not an empty string because of
		   the UNIQUE constraint on guid */
		sprintf (sql_stmt, update_stmt_null_guid,
		    u64tostr (fmd_entry->parent_fileid, parent_fileid_str, -1),
		    escaped_name, fmd_entry->filemode, fmd_entry->nlink,
		    fmd_entry->uid, fmd_entry->gid,
		    u64tostr (fmd_entry->filesize, filesize_str, -1),
		    fmd_entry->atime, fmd_entry->mtime, fmd_entry->ctime,
		    fmd_entry->fileclass, fmd_entry->status, fmd_entry->csumtype,
		    fmd_entry->csumvalue, fmd_entry->acl, *rec_addr);

	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "UPDATE", dbfd);
		return (-1);
	}
	return (0);
}

Cns_update_rep_entry(dbfd, rec_addr, rep_entry)
struct Cns_dbfd *dbfd;
Cns_dbrec_addr *rec_addr;
struct Cns_file_replica *rep_entry;
{
	char func[21];
	char nbacces_str[21];
	char sql_stmt[1458];
	static char update_stmt[] =
		"UPDATE Cns_file_replica SET \
		NBACCESSES = %s, ATIME = %d, PTIME = %d, STATUS = '%c', \
		F_TYPE = '%c', POOLNAME = '%s', HOST = '%s', FS = '%s', SFN = '%s' \
		WHERE ROWID = %s";
	static char update_stmt_null_ftype[] =
		"UPDATE Cns_file_replica SET \
		NBACCESSES = %s, ATIME = %d, PTIME = %d, STATUS = '%c', \
		F_TYPE = NULL, POOLNAME = '%s', HOST = '%s', FS = '%s', SFN = '%s' \
		WHERE ROWID = %s";

	strcpy (func, "Cns_update_rep_entry");
	if (rep_entry->f_type)
		sprintf (sql_stmt, update_stmt,
		    u64tostr (rep_entry->nbaccesses, nbacces_str, -1),
		    rep_entry->atime, rep_entry->ptime, rep_entry->status,
		    rep_entry->f_type, rep_entry->poolname, rep_entry->host,
		    rep_entry->fs, rep_entry->sfn, *rec_addr);
	else
		sprintf (sql_stmt, update_stmt_null_ftype,
		    u64tostr (rep_entry->nbaccesses, nbacces_str, -1),
		    rep_entry->atime, rep_entry->ptime, rep_entry->status,
		    rep_entry->poolname, rep_entry->host,
		    rep_entry->fs, rep_entry->sfn, *rec_addr);

	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "UPDATE", dbfd);
		return (-1);
	}
	return (0);
}

Cns_update_smd_entry(dbfd, rec_addr, smd_entry)
struct Cns_dbfd *dbfd;
Cns_dbrec_addr *rec_addr;
struct Cns_seg_metadata *smd_entry;
{
	char func[21];
	char segsize_str[21];
	char sql_stmt[1024];
	static char update_stmt[] =
		"UPDATE Cns_seg_metadata SET \
		SEGSIZE	= %s, COMPRESSION = %d, S_STATUS = '%c', \
		VID = '%s', SIDE = %d, FSEQ = %d, BLOCKID = '%02x%02x%02x%02x', \
		CHECKSUM_NAME = '%s', CHECKSUM = %d \
		WHERE ROWID = %s";

	strcpy (func, "Cns_update_smd_entry");
	sprintf (sql_stmt, update_stmt,
	    u64tostr (smd_entry->segsize, segsize_str, -1),
	    smd_entry->compression, smd_entry->s_status,
	    smd_entry->vid, smd_entry->side, smd_entry->fseq,
	    smd_entry->blockid[0], smd_entry->blockid[1],
	    smd_entry->blockid[2], smd_entry->blockid[3],
	    smd_entry->checksum_name, smd_entry->checksum, 
	    *rec_addr);

	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "UPDATE", dbfd);
		return (-1);
	}
	return (0);
}

Cns_update_umd_entry(dbfd, rec_addr, umd_entry)
struct Cns_dbfd *dbfd;
Cns_dbrec_addr *rec_addr;
struct Cns_user_metadata *umd_entry;
{
	char func[21];
	char sql_stmt[1024];
	static char update_stmt[] =
		"UPDATE Cns_user_metadata SET \
		COMMENTS = '%s' \
		WHERE ROWID = %s";

	strcpy (func, "Cns_update_umd_entry");
	sprintf (sql_stmt, update_stmt,
	    umd_entry->comments, *rec_addr);

	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "UPDATE", dbfd);
		return (-1);
	}
	return (0);
}

	/* Routines for identity mapping */

Cns_delete_group_entry(dbfd, rec_addr)
struct Cns_dbfd *dbfd;
Cns_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM Cns_groupinfo WHERE ROWID = %s";
	char func[23];
	char sql_stmt[70];

	strcpy (func, "Cns_delete_group_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "DELETE", dbfd);
		return (-1);
	}
	return (0);
}

Cns_delete_user_entry(dbfd, rec_addr)
struct Cns_dbfd *dbfd;
Cns_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM Cns_userinfo WHERE ROWID = %s";
	char func[22];
	char sql_stmt[70];

	strcpy (func, "Cns_delete_user_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "DELETE", dbfd);
		return (-1);
	}
	return (0);
}

Cns_get_grpinfo_by_gid(dbfd, gid, group_entry, lock, rec_addr)
struct Cns_dbfd *dbfd;
gid_t gid;
struct Cns_groupinfo *group_entry;
int lock;
Cns_dbrec_addr *rec_addr;
{
	char func[23];
	int i = 0;
	static char query[] =
		"SELECT GID, GROUPNAME FROM Cns_groupinfo \
		WHERE gid = %d";
	static char query4upd[] =
		"SELECT ROWID, GID, GROUPNAME FROM Cns_groupinfo \
		WHERE gid = %d FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_grpinfo_by_gid");
	sprintf (sql_stmt, lock ? query4upd : query, gid);
	if (Cns_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}
	if (lock)
		strcpy (*rec_addr, row[i++]);
	group_entry->gid = atoi (row[i++]);
	strcpy (group_entry->groupname, row[i]);
	mysql_free_result (res);
	return (0);
}

Cns_get_grpinfo_by_name(dbfd, name, group_entry, lock, rec_addr)
struct Cns_dbfd *dbfd;
char *name;
struct Cns_groupinfo *group_entry;
int lock;
Cns_dbrec_addr *rec_addr;
{
	char func[24];
	int i = 0;
	static char query[] =
		"SELECT GID, GROUPNAME FROM Cns_groupinfo \
		WHERE groupname = '%s'";
	static char query4upd[] =
		"SELECT ROWID, GID, GROUPNAME FROM Cns_groupinfo \
		WHERE groupname = '%s' FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_grpinfo_by_name");
	sprintf (sql_stmt, lock ? query4upd : query, name);
	if (Cns_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}
	if (lock)
		strcpy (*rec_addr, row[i++]);
	group_entry->gid = atoi (row[i++]);
	strcpy (group_entry->groupname, row[i]);
	mysql_free_result (res);
	return (0);
}

Cns_get_usrinfo_by_name(dbfd, name, user_entry, lock, rec_addr)
struct Cns_dbfd *dbfd;
char *name;
struct Cns_userinfo *user_entry;
int lock;
Cns_dbrec_addr *rec_addr;
{
	char func[24];
	int i = 0;
	static char query[] =
		"SELECT USERID, USERNAME FROM Cns_userinfo \
		WHERE username = '%s'";
	static char query4upd[] =
		"SELECT ROWID, USERID, USERNAME FROM Cns_userinfo \
		WHERE username = '%s' FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_usrinfo_by_name");
	sprintf (sql_stmt, lock ? query4upd : query, name);
	if (Cns_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}
	if (lock)
		strcpy (*rec_addr, row[i++]);
	user_entry->userid = atoi (row[i++]);
	strcpy (user_entry->username, row[i]);
	mysql_free_result (res);
	return (0);
}

Cns_get_usrinfo_by_uid(dbfd, uid, user_entry, lock, rec_addr)
struct Cns_dbfd *dbfd;
uid_t uid;
struct Cns_userinfo *user_entry;
int lock;
Cns_dbrec_addr *rec_addr;
{
	char func[23];
	int i = 0;
	static char query[] =
		"SELECT USERID, USERNAME FROM Cns_userinfo \
		WHERE userid = %d";
	static char query4upd[] =
		"SELECT ROWID, USERID, USERNAME FROM Cns_userinfo \
		WHERE userid = %d FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "Cns_get_usrinfo_by_uid");
	sprintf (sql_stmt, lock ? query4upd : query, uid);
	if (Cns_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}
	if (lock)
		strcpy (*rec_addr, row[i++]);
	user_entry->userid = atoi (row[i++]);
	strcpy (user_entry->username, row[i]);
	mysql_free_result (res);
	return (0);
}

Cns_insert_group_entry(dbfd, group_entry)
struct Cns_dbfd *dbfd;
struct Cns_groupinfo *group_entry;
{
	char func[23];
	static char insert_stmt[] =
		"INSERT INTO Cns_groupinfo (GID, GROUPNAME) VALUES (%d, '%s')";
	char sql_stmt[1024];

	strcpy (func, "Cns_insert_group_entry");
	sprintf (sql_stmt, insert_stmt, group_entry->gid, group_entry->groupname);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			Cns_mysql_error (func, "INSERT", dbfd);
		}
		return (-1);
	}
	return (0);
}

Cns_insert_user_entry(dbfd, user_entry)
struct Cns_dbfd *dbfd;
struct Cns_userinfo *user_entry;
{
	char func[22];
	static char insert_stmt[] =
		"INSERT INTO Cns_userinfo (USERID, USERNAME) VALUES (%d, '%s')";
	char sql_stmt[1024];

	strcpy (func, "Cns_insert_user_entry");
	sprintf (sql_stmt, insert_stmt, user_entry->userid, user_entry->username);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			Cns_mysql_error (func, "INSERT", dbfd);
		}
		return (-1);
	}
	return (0);
}

int oneuniquegid;
Cns_unique_gid(dbfd, unique_id)
struct Cns_dbfd *dbfd;
unsigned int *unique_id;
{
	char func[15];
	static char insert_stmt[] =
		"INSERT INTO Cns_unique_gid (ID) VALUES (101)";
	static char query_stmt[] =
		"SELECT ID FROM Cns_unique_gid FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];
	unsigned int uniqueid;
	static char update_stmt[] =
		"UPDATE Cns_unique_gid SET ID = %d";

	strcpy (func, "Cns_unique_gid");
	(void) Cthread_mutex_lock (&oneuniquegid);
	if (Cns_exec_query (func, dbfd, query_stmt, &res)) {
		(void) Cthread_mutex_unlock (&oneuniquegid);
		return (-1);
	}
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		if (mysql_query (&dbfd->mysql, insert_stmt)) {
			(void) Cthread_mutex_unlock (&oneuniquegid);
			Cns_mysql_error (func, "INSERT", dbfd);
			return (-1);
		}
		*unique_id = 101;
	} else {
		uniqueid = atoi (row[0]) + 1;
		mysql_free_result (res);
		sprintf (sql_stmt, update_stmt, uniqueid);
		if (mysql_query (&dbfd->mysql, sql_stmt)) {
			(void) Cthread_mutex_unlock (&oneuniquegid);
			Cns_mysql_error (func, "UPDATE", dbfd);
			return (-1);
		}
		*unique_id = uniqueid;
	}
	(void) Cthread_mutex_unlock (&oneuniquegid);
	return (0);
}

int oneuniqueuid;
Cns_unique_uid(dbfd, unique_id)
struct Cns_dbfd *dbfd;
unsigned int *unique_id;
{
	char func[15];
	static char insert_stmt[] =
		"INSERT INTO Cns_unique_uid (ID) VALUES (101)";
	static char query_stmt[] =
		"SELECT ID FROM Cns_unique_uid FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];
	unsigned int uniqueid;
	static char update_stmt[] =
		"UPDATE Cns_unique_uid SET ID = %d";

	strcpy (func, "Cns_unique_uid");
	(void) Cthread_mutex_lock (&oneuniqueuid);
	if (Cns_exec_query (func, dbfd, query_stmt, &res)) {
		(void) Cthread_mutex_unlock (&oneuniqueuid);
		return (-1);
	}
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		if (mysql_query (&dbfd->mysql, insert_stmt)) {
			(void) Cthread_mutex_unlock (&oneuniqueuid);
			Cns_mysql_error (func, "INSERT", dbfd);
			return (-1);
		}
		*unique_id = 101;
	} else {
		uniqueid = atoi (row[0]) + 1;
		mysql_free_result (res);
		sprintf (sql_stmt, update_stmt, uniqueid);
		if (mysql_query (&dbfd->mysql, sql_stmt)) {
			(void) Cthread_mutex_unlock (&oneuniqueuid);
			Cns_mysql_error (func, "UPDATE", dbfd);
			return (-1);
		}
		*unique_id = uniqueid;
	}
	(void) Cthread_mutex_unlock (&oneuniqueuid);
	return (0);
}

Cns_update_group_entry(dbfd, rec_addr, group_entry)
struct Cns_dbfd *dbfd;
Cns_dbrec_addr *rec_addr;
struct Cns_groupinfo *group_entry;
{
	char func[23];
	char sql_stmt[1024];
	static char update_stmt[] =
		"UPDATE Cns_groupinfo SET GROUPNAME = '%s' WHERE ROWID = %s";

	strcpy (func, "Cns_update_group_entry");
	sprintf (sql_stmt, update_stmt, group_entry->groupname, *rec_addr);

	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "UPDATE", dbfd);
		return (-1);
	}
	return (0);
}

Cns_update_unique_gid(dbfd, unique_id)
struct Cns_dbfd *dbfd;
unsigned int unique_id;
{
	char func[22];
	static char insert_stmt[] =
		"INSERT INTO Cns_unique_gid (ID) VALUES (%d)";
	static char query_stmt[] =
		"SELECT ID FROM Cns_unique_gid FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];
	static char update_stmt[] =
		"UPDATE Cns_unique_gid SET ID = %d";

	strcpy (func, "Cns_update_unique_gid");
	(void) Cthread_mutex_lock (&oneuniquegid);
	if (Cns_exec_query (func, dbfd, query_stmt, &res)) {
		(void) Cthread_mutex_unlock (&oneuniquegid);
		return (-1);
	}
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		sprintf (sql_stmt, insert_stmt, unique_id);
		if (mysql_query (&dbfd->mysql, insert_stmt)) {
			(void) Cthread_mutex_unlock (&oneuniquegid);
			Cns_mysql_error (func, "INSERT", dbfd);
			return (-1);
		}
	} else {
		mysql_free_result (res);
		if (unique_id > atoi (row[0])) {
			sprintf (sql_stmt, update_stmt, unique_id);
			if (mysql_query (&dbfd->mysql, sql_stmt)) {
				(void) Cthread_mutex_unlock (&oneuniquegid);
				Cns_mysql_error (func, "UPDATE", dbfd);
				return (-1);
			}
		}
	}
	(void) Cthread_mutex_unlock (&oneuniquegid);
	return (0);
}

Cns_update_unique_uid(dbfd, unique_id)
struct Cns_dbfd *dbfd;
unsigned int unique_id;
{
	char func[22];
	static char insert_stmt[] =
		"INSERT INTO Cns_unique_uid (ID) VALUES (%d)";
	static char query_stmt[] =
		"SELECT ID FROM Cns_unique_uid FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];
	static char update_stmt[] =
		"UPDATE Cns_unique_uid SET ID = %d";

	strcpy (func, "Cns_update_unique_uid");
	(void) Cthread_mutex_lock (&oneuniqueuid);
	if (Cns_exec_query (func, dbfd, query_stmt, &res)) {
		(void) Cthread_mutex_unlock (&oneuniquegid);
		return (-1);
	}
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		sprintf (sql_stmt, insert_stmt, unique_id);
		if (mysql_query (&dbfd->mysql, insert_stmt)) {
			(void) Cthread_mutex_unlock (&oneuniqueuid);
			Cns_mysql_error (func, "INSERT", dbfd);
			return (-1);
		}
	} else {
		mysql_free_result (res);
		if (unique_id > atoi (row[0])) {
			sprintf (sql_stmt, update_stmt, unique_id);
			if (mysql_query (&dbfd->mysql, sql_stmt)) {
				(void) Cthread_mutex_unlock (&oneuniqueuid);
				Cns_mysql_error (func, "UPDATE", dbfd);
				return (-1);
			}
		}
	}
	(void) Cthread_mutex_unlock (&oneuniqueuid);
	return (0);
}

Cns_update_user_entry(dbfd, rec_addr, user_entry)
struct Cns_dbfd *dbfd;
Cns_dbrec_addr *rec_addr;
struct Cns_userinfo *user_entry;
{
	char func[22];
	char sql_stmt[1024];
	static char update_stmt[] =
		"UPDATE Cns_userinfo SET USERNAME = '%s' WHERE ROWID = %s";

	strcpy (func, "Cns_update_user_entry");
	sprintf (sql_stmt, update_stmt, user_entry->username, *rec_addr);

	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		Cns_mysql_error (func, "UPDATE", dbfd);
		return (-1);
	}
	return (0);
}
