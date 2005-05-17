/*
 * Copyright (C) 2002-2003 by CERN/IT/DS/HSM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgr_mysql_ifce.c,v $ $Revision: 1.2 $ $Date: 2005/05/17 15:03:24 $ CERN IT-DS/HSM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <mysqld_error.h>
#include "serrno.h"
#include "u64subr.h"
#include "vmgr.h"
#include "vmgr_server.h"

vmgr_init_dbpkg()
{
	int i;

	return (0);
}

vmgr_abort_tr(dbfd)
struct vmgr_dbfd *dbfd;
{
	(void) mysql_query (&dbfd->mysql, "ROLLBACK");
	dbfd->tr_started = 0;
	return (0);
}

vmgr_closedb(dbfd)
struct vmgr_dbfd *dbfd;
{
	mysql_close (&dbfd->mysql);
	return (0);
}

vmgr_delete_denmap_entry(dbfd, rec_addr)
struct vmgr_dbfd *dbfd;
vmgr_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM vmgr_tape_denmap WHERE ROWID = %s";
	char func[25];
	char sql_stmt[70];

	strcpy (func, "vmgr_delete_denmap_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		vmgrlogit (func, "DELETE error: %s\n",
		    mysql_error (&dbfd->mysql));
		serrno = SEINTERNAL;
		return (-1);
	}
	return (0);
}

vmgr_delete_dgnmap_entry(dbfd, rec_addr)
struct vmgr_dbfd *dbfd;
vmgr_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM vmgr_tape_dgnmap WHERE ROWID = %s";
	char func[25];
	char sql_stmt[70];

	strcpy (func, "vmgr_delete_dgnmap_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		vmgrlogit (func, "DELETE error: %s\n",
		    mysql_error (&dbfd->mysql));
		serrno = SEINTERNAL;
		return (-1);
	}
	return (0);
}

vmgr_delete_library_entry(dbfd, rec_addr)
struct vmgr_dbfd *dbfd;
vmgr_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM vmgr_tape_library WHERE ROWID = %s";
	char func[26];
	char sql_stmt[70];

	strcpy (func, "vmgr_delete_library_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		vmgrlogit (func, "DELETE error: %s\n",
		    mysql_error (&dbfd->mysql));
		serrno = SEINTERNAL;
		return (-1);
	}
	return (0);
}

vmgr_delete_model_entry(dbfd, rec_addr)
struct vmgr_dbfd *dbfd;
vmgr_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM vmgr_tape_media WHERE ROWID = %s";
	char func[24];
	char sql_stmt[70];

	strcpy (func, "vmgr_delete_model_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		vmgrlogit (func, "DELETE error: %s\n",
		    mysql_error (&dbfd->mysql));
		serrno = SEINTERNAL;
		return (-1);
	}
	return (0);
}

vmgr_delete_pool_entry(dbfd, rec_addr)
struct vmgr_dbfd *dbfd;
vmgr_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM vmgr_tape_pool WHERE ROWID = %s";
	char func[23];
	char sql_stmt[70];

	strcpy (func, "vmgr_delete_pool_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		vmgrlogit (func, "DELETE error: %s\n",
		    mysql_error (&dbfd->mysql));
		serrno = SEINTERNAL;
		return (-1);
	}
	return (0);
}

vmgr_delete_side_entry(dbfd, rec_addr)
struct vmgr_dbfd *dbfd;
vmgr_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM vmgr_tape_side WHERE ROWID = %s";
	char func[23];
	char sql_stmt[70];

	strcpy (func, "vmgr_delete_side_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		vmgrlogit (func, "DELETE error: %s\n",
		    mysql_error (&dbfd->mysql));
		serrno = SEINTERNAL;
		return (-1);
	}
	return (0);
}

vmgr_delete_tag_entry(dbfd, rec_addr)
struct vmgr_dbfd *dbfd;
vmgr_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM vmgr_tape_tag WHERE ROWID = %s";
	char func[22];
	char sql_stmt[70];

	strcpy (func, "vmgr_delete_tag_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		vmgrlogit (func, "DELETE error: %s\n",
		    mysql_error (&dbfd->mysql));
		serrno = SEINTERNAL;
		return (-1);
	}
	return (0);
}

vmgr_delete_tape_entry(dbfd, rec_addr)
struct vmgr_dbfd *dbfd;
vmgr_dbrec_addr *rec_addr;
{
	static char delete_stmt[] =
		"DELETE FROM vmgr_tape_info WHERE ROWID = %s";
	char func[23];
	char sql_stmt[70];

	strcpy (func, "vmgr_delete_tape_entry");
	sprintf (sql_stmt, delete_stmt, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		vmgrlogit (func, "DELETE error: %s\n",
		    mysql_error (&dbfd->mysql));
		serrno = SEINTERNAL;
		return (-1);
	}
	return (0);
}

vmgr_end_tr(dbfd)
struct vmgr_dbfd *dbfd;
{
	(void) mysql_query (&dbfd->mysql, "COMMIT");
	dbfd->tr_started = 0;
	return (0);
}

vmgr_exec_query(func, dbfd, sql_stmt, res)
char *func;
struct vmgr_dbfd *dbfd;
char *sql_stmt;
MYSQL_RES **res;
{
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		vmgrlogit (func, "mysql_query error: %s\n",
		    mysql_error (&dbfd->mysql));
		serrno = SEINTERNAL;
		return (-1);
	}
	if ((*res = mysql_store_result (&dbfd->mysql)) == NULL) {
		vmgrlogit (func, "mysql_store_res error: %s\n",
		    mysql_error (&dbfd->mysql));
		serrno = SEINTERNAL;
		return (-1);
	}
	return (0);
}

vmgr_get_denmap_entry(dbfd, model, media_letter, density, denmap_entry, lock, rec_addr)
struct vmgr_dbfd *dbfd;
char *model;
char *media_letter;
char *density;
struct vmgr_tape_denmap *denmap_entry;
int lock;
vmgr_dbrec_addr *rec_addr;
{
	char func[22];
	int i = 0;
	static char query[] =
		"SELECT \
		 MD_MODEL, MD_MEDIA_LETTER, MD_DENSITY, NATIVE_CAPACITY \
		FROM vmgr_tape_denmap \
		WHERE md_model = '%s' AND md_media_letter = '%s' AND \
		 md_density = '%s'";
	static char query4upd[] =
		"SELECT ROWID, \
		 MD_MODEL, MD_MEDIA_LETTER, MD_DENSITY, NATIVE_CAPACITY \
		FROM vmgr_tape_denmap \
		WHERE md_model = '%s' AND md_media_letter = '%s' AND \
		 md_density = '%s' \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "vmgr_get_denmap_entry");
	sprintf (sql_stmt, lock ? query4upd : query, model, media_letter,
	    density);
	if (vmgr_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}
	if (lock)
		strcpy (*rec_addr, row[i++]);
	strcpy (denmap_entry->md_model, row[i++]);
	strcpy (denmap_entry->md_media_letter, row[i++]);
	strcpy (denmap_entry->md_density, row[i++]);
	denmap_entry->native_capacity = atoi (row[i++]);
	mysql_free_result (res);
	return (0);
}

vmgr_get_dgnmap_entry(dbfd, model, library, dgnmap_entry, lock, rec_addr)
struct vmgr_dbfd *dbfd;
char *model;
char *library;
struct vmgr_tape_dgnmap *dgnmap_entry;
int lock;
vmgr_dbrec_addr *rec_addr;
{
	char func[22];
	int i = 0;
	static char query[] =
		"SELECT \
		 DGN, MODEL, LIBRARY \
		FROM vmgr_tape_dgnmap \
		WHERE model = '%s' AND library = '%s'";
	static char query4upd[] =
		"SELECT ROWID, \
		 DGN, MODEL, LIBRARY \
		FROM vmgr_tape_dgnmap \
		WHERE model = '%s' AND library = '%s' \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "vmgr_get_dgnmap_entry");
	sprintf (sql_stmt, lock ? query4upd : query, model, library);
	if (vmgr_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}

	if (lock)
		strcpy (*rec_addr, row[i++]);
	strcpy (dgnmap_entry->dgn, row[i++]);
	strcpy (dgnmap_entry->model, row[i++]);
	strcpy (dgnmap_entry->library, row[i++]);
	mysql_free_result (res);
	return (0);
}

vmgr_get_dgn_entry(dbfd, dgn, dgn_entry, lock, rec_addr)
struct vmgr_dbfd *dbfd;
char *dgn;
struct vmgr_tape_dgn *dgn_entry;
int lock;
vmgr_dbrec_addr *rec_addr;
{
	char func[22];
	int i = 0;
	static char query[] =
		"SELECT \
		 DGN, WEIGHT, DELTAWEIGHT\
		FROM vmgr_tape_dgn \
		WHERE dgn = '%s'";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "vmgr_get_dgnmap_entry");
	sprintf (sql_stmt, query, dgn);
	if (vmgr_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}

	if (lock)
		strcpy (*rec_addr, row[i++]);
	strcpy (dgn_entry->dgn, row[i++]);
	dgn_entry->weight = strtol(row[i++], NULL, 10);
	dgn_entry->deltaweight = strtol(row[i++], NULL, 10);
	mysql_free_result (res);
	return (0);
}

vmgr_update_weight(dbfd, dgn, weight, delta, valid)
struct vmgr_dbfd *dbfd;
char *dgn;
int weight;
int delta;
int valid;
{
        char func[22];
        char sql_stmt[1024];
        static char *update_stmt;
        static char update_weight_stmt[] =
                "UPDATE vmgr_tape_dgn \
                SET WEIGHT = '%i' \
                WHERE dgn = '%s'";
        static char update_delta_stmt[] =
           "UPDATE vmgr_tape_dgn \
                SET DELTAWEIGHT = '%i' \
                WHERE dgn = '%s'";
        static char update_weightdelta_stmt[] =
           "UPDATE vmgr_tape_dgn \
                SET WEIGHT = '%i', DELTAWEIGHT = '%i' \
                WHERE dgn = '%s'";

        if ((valid & OPT_VALID_WEIGHT) && (valid & OPT_VALID_DELTA)) {
           update_stmt = update_weightdelta_stmt;;
           sprintf (sql_stmt, update_stmt, weight, delta, dgn);
        } else {
           if (valid & OPT_VALID_WEIGHT) {
              update_stmt = update_weight_stmt;
              sprintf (sql_stmt, update_stmt, weight, dgn);
           } else { /* we now valid & OPT_VALID_DELTA is true */
              update_stmt = update_delta_stmt;
              sprintf (sql_stmt, update_stmt, delta, dgn);
           }
        }

        strcpy (func, "vmgr_update_weight");
        if (mysql_query (&dbfd->mysql, sql_stmt)) {
                vmgrlogit (func, "UPDATE error: %s\n",
                    mysql_error (&dbfd->mysql));
                serrno = SEINTERNAL;
                return (-1);
        }

        if (mysql_affected_rows(&dbfd->mysql) == 0) {
		/* No record updated -> That DGN does not exist */
		serrno = EVIDGNINVL;
		return (-1);
 	}

        return (0);
}

/* populate all missing dgns */
vmgr_populate_dgns(dbfd)
struct vmgr_dbfd *dbfd;
{

        char func[25];
        static char insert_stmt[] =
               "INSERT INTO vmgr_tape_dgn (DGN, WEIGHT, DELTAWEIGHT) \
                 SELECT DISTINCT d.dgn, 0, 1 FROM vmgr_tape_dgnmap d \
                 LEFT JOIN vmgr_tape_dgn dm ON d.dgn = dm.dgn \
                 WHERE dm.dgn IS NULL;";

        char sql_stmt[1024];
                                                                                
        strcpy (func, "vmgr_populate_dgns");
        if (mysql_query (&dbfd->mysql, insert_stmt)) {
                if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
                        serrno = EEXIST;
                else {
                        vmgrlogit (func, "INSERT error: %s\n",
                            mysql_error (&dbfd->mysql));
                        serrno = SEINTERNAL;
                }
                return (-1);
        }
        return (0);
}

/* sweep all dgns that do not have a mapping */
vmgr_sweep_dgns(dbfd)
struct vmgr_dbfd *dbfd;
{
        static char delete_stmt[] =
                "DELETE vmgr_tape_dgn FROM vmgr_tape_dgn d \
		 LEFT JOIN vmgr_tape_dgnmap dm ON d.dgn = dm.dgn \
		 WHERE dm.dgn IS NULL;";
        char func[25];
                                                                                
        strcpy (func, "vmgr_sweep_dgns");
        if (mysql_query (&dbfd->mysql, delete_stmt)) {
                vmgrlogit (func, "DELETE error: %s\n",
                    mysql_error (&dbfd->mysql));
                serrno = SEINTERNAL;
                return (-1);
        }
        return (0);

}


vmgr_get_library_entry(dbfd, library_name, library_entry, lock, rec_addr)
struct vmgr_dbfd *dbfd;
char *library_name;
struct vmgr_tape_library *library_entry;
int lock;
vmgr_dbrec_addr *rec_addr;
{
	char func[23];
	int i = 0;
	static char query[] =
		"SELECT \
		 NAME, CAPACITY, \
		 NB_FREE_SLOTS, STATUS \
		FROM vmgr_tape_library \
		WHERE name = '%s'";
	static char query4upd[] =
		"SELECT ROWID, \
		 NAME, CAPACITY, \
		 NB_FREE_SLOTS, STATUS \
		FROM vmgr_tape_library \
		WHERE name = '%s' \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "vmgr_get_library_entry");
	sprintf (sql_stmt, lock ? query4upd : query, library_name);
	if (vmgr_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}

	if (lock)
		strcpy (*rec_addr, row[i++]);
	strcpy (library_entry->name, row[i++]);
	library_entry->capacity = atoi (row[i++]);
	library_entry->nb_free_slots = atoi (row[i++]);
	library_entry->status = atoi (row[i++]);
	mysql_free_result (res);
	return (0);
}

vmgr_get_model_entry(dbfd, model, model_entry, lock, rec_addr)
struct vmgr_dbfd *dbfd;
char *model;
struct vmgr_tape_media *model_entry;
int lock;
vmgr_dbrec_addr *rec_addr;
{
	char func[21];
	int i = 0;
	static char query[] =
		"SELECT \
		 M_MODEL, M_MEDIA_LETTER, \
		 MEDIA_COST \
		FROM vmgr_tape_media \
		WHERE m_model = '%s'";
	static char query4upd[] =
		"SELECT ROWID, \
		 M_MODEL, M_MEDIA_LETTER, \
		 MEDIA_COST \
		FROM vmgr_tape_media \
		WHERE m_model = '%s' \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "vmgr_get_model_entry");
	sprintf (sql_stmt, lock ? query4upd : query, model);
	if (vmgr_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}

	if (lock)
		strcpy (*rec_addr, row[i++]);
	strcpy (model_entry->m_model, row[i++]);
	strcpy (model_entry->m_media_letter, row[i++]);
	model_entry->media_cost = atoi (row[i++]);
	mysql_free_result (res);
	return (0);
}

vmgr_get_pool_entry(dbfd, pool_name, pool_entry, lock, rec_addr)
struct vmgr_dbfd *dbfd;
char *pool_name;
struct vmgr_tape_pool *pool_entry;
int lock;
vmgr_dbrec_addr *rec_addr;
{
	char func[20];
	int i = 0;
	static char query[] =
		"SELECT \
		 NAME, OWNER_UID, GID, CAPACITY, TOT_FREE_SPACE \
		FROM vmgr_tape_pool \
		WHERE name = '%s'";
	static char query4upd[] =
		"SELECT ROWID, \
		 NAME, OWNER_UID, GID, CAPACITY, TOT_FREE_SPACE \
		FROM vmgr_tape_pool \
		WHERE name = '%s' \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "vmgr_get_pool_entry");
	sprintf (sql_stmt, lock ? query4upd : query, pool_name);
	if (vmgr_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}

	if (lock)
		strcpy (*rec_addr, row[i++]);
	strcpy (pool_entry->name, row[i++]);
	pool_entry->uid = atoi (row[i++]);
	pool_entry->gid = atoi (row[i++]);
	pool_entry->capacity = strtou64 (row[i++]);
	pool_entry->tot_free_space = strtou64 (row[i++]);
	mysql_free_result (res);
	return (0);
}

vmgr_get_side_by_fullid (dbfd, vid, side, side_entry, lock, rec_addr)
struct vmgr_dbfd *dbfd;
char *vid;
int side;
struct vmgr_tape_side *side_entry;
int lock;
vmgr_dbrec_addr *rec_addr;
{
	char func[24];
	int i = 0;
	static char query[] =
		"SELECT \
		 VID, SIDE, POOLNAME, STATUS, ESTIMATED_FREE_SPACE, NBFILES \
		FROM vmgr_tape_side \
		WHERE vid = '%s' AND side = %d";
	static char query4upd[] =
		"SELECT ROWID, \
		 VID, SIDE, POOLNAME, STATUS, ESTIMATED_FREE_SPACE, NBFILES \
		FROM vmgr_tape_side \
		WHERE vid = '%s' AND side = %d \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "vmgr_get_side_by_fullid");
	sprintf (sql_stmt, lock ? query4upd : query, vid, side);
	if (vmgr_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}

	if (lock)
		strcpy (*rec_addr, row[i++]);
	strcpy (side_entry->vid, row[i++]);
	side_entry->side = atoi (row[i++]);
	strcpy (side_entry->poolname, row[i++]);
	side_entry->status = atoi (row[i++]);
	side_entry->estimated_free_space = atoi (row[i++]);
	side_entry->nbfiles = atoi (row[i++]);
	mysql_free_result (res);
	return (0);
}

vmgr_get_tag_by_vid(dbfd, vid, tag_entry, lock, rec_addr)
struct vmgr_dbfd *dbfd;
char *vid;
struct vmgr_tape_tag *tag_entry;
int lock;
vmgr_dbrec_addr *rec_addr;
{
	char func[20];
	int i = 0;
	static char query[] =
		"SELECT \
		 VID, TEXT \
		FROM vmgr_tape_tag \
		WHERE vid = '%s'";
	static char query4upd[] =
		"SELECT ROWID, \
		 VID, TEXT \
		FROM vmgr_tape_tag \
		WHERE vid = '%s' \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "vmgr_get_tag_by_vid");
	sprintf (sql_stmt, lock ? query4upd : query, vid);
	if (vmgr_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}
	if (lock)
		strcpy (*rec_addr, row[i++]);
	strcpy (tag_entry->vid, row[i++]);
	strcpy (tag_entry->text, row[i++]);
	mysql_free_result (res);
	return (0);
}

vmgr_get_tape_by_vid(dbfd, vid, tape_entry, lock, rec_addr)
struct vmgr_dbfd *dbfd;
char *vid;
struct vmgr_tape_info *tape_entry;
int lock;
vmgr_dbrec_addr *rec_addr;
{
	char func[21];
	int i = 0;
	static char query[] =
		"SELECT \
		 VID, VSN, LIBRARY, DENSITY, LBLTYPE, MODEL, MEDIA_LETTER, \
		 MANUFACTURER, SN, NBSIDES, ETIME, RCOUNT, WCOUNT, RHOST, \
		 WHOST, RJID, WJID, RTIME, WTIME \
		FROM vmgr_tape_info \
		WHERE vid = '%s'";
	static char query4upd[] =
		"SELECT ROWID, \
		 VID, VSN, LIBRARY, DENSITY, LBLTYPE, MODEL, MEDIA_LETTER, \
		 MANUFACTURER, SN, NBSIDES, ETIME, RCOUNT, WCOUNT, RHOST, \
		 WHOST, RJID, WJID, RTIME, WTIME \
		FROM vmgr_tape_info \
		WHERE vid = '%s' \
		FOR UPDATE";
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "vmgr_get_tape_by_vid");
	sprintf (sql_stmt, lock ? query4upd : query, vid);
	if (vmgr_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		serrno = ENOENT;
		return (-1);
	}

	if (lock)
		strcpy (*rec_addr, row[i++]);
	strcpy (tape_entry->vid, row[i++]);
	strcpy (tape_entry->vsn, row[i++]);
	strcpy (tape_entry->library, row[i++]);
	strcpy (tape_entry->density, row[i++]);
	strcpy (tape_entry->lbltype, row[i++]);
	strcpy (tape_entry->model, row[i++]);
	strcpy (tape_entry->media_letter, row[i++]);
	strcpy (tape_entry->manufacturer, row[i++]);
	strcpy (tape_entry->sn, row[i++]);
	tape_entry->nbsides = atoi (row[i++]);
	tape_entry->etime = atoi (row[i++]);
	tape_entry->rcount = atoi (row[i++]);
	tape_entry->wcount = atoi (row[i++]);
	strcpy (tape_entry->rhost, row[i++]);
	strcpy (tape_entry->whost, row[i++]);
	tape_entry->rjid = atoi (row[i++]);
	tape_entry->wjid = atoi (row[i++]);
	tape_entry->rtime = atoi (row[i++]);
	tape_entry->wtime = atoi (row[i++]);
	mysql_free_result (res);
	return (0);
}

vmgr_get_side_by_weight(dbfd, poolname, Size, side_entry, lock, rec_addr)
struct vmgr_dbfd *dbfd;
char *poolname;
u_signed64 Size;
struct vmgr_tape_side *side_entry;
int lock;
vmgr_dbrec_addr *rec_addr;
{
        char func[22];
        int i = 0;
        static char querymin4upd[] =
                "SELECT s.ROWID, s.VID, s.SIDE, s.POOLNAME,\
                 s.STATUS, s.ESTIMATED_FREE_SPACE, s.NBFILES \
                FROM vmgr_tape_side s, vmgr_tape_info i, \
			vmgr_tape_dgnmap dm, vmgr_tape_dgn d \
                WHERE poolname = '%s' AND status = 0 \
                 AND estimated_free_space >= %d AND s.vid = i.vid \
		 AND i.library = dm.library AND i.model = dm.model \
		 AND dm.dgn = d.dgn \
                ORDER BY d.weight, s.estimated_free_space \
                LIMIT 1 \
                FOR UPDATE";
        /* If the former query returns no results, there is no tape with 
         * enought space. It's critical that this function returns something,
         * so we return the tape with the most space available */
        static char querymax4upd[] =
                "SELECT ROWID, \
                 VID, SIDE, POOLNAME, STATUS, ESTIMATED_FREE_SPACE, NBFILES \
                FROM vmgr_tape_side \
                WHERE poolname = '%s' AND status = 0 \
                ORDER BY estimated_free_space DESC \
                LIMIT 1 \
                FOR UPDATE";

        int reqsize = (Size + 1023) / 1024;
        MYSQL_RES *res;
        MYSQL_ROW row;
        char sql_stmt[2048];
                                                                                
        strcpy (func, "vmgr_get_side_by_weight");
        sprintf (sql_stmt, querymin4upd, poolname, reqsize);
        if (vmgr_exec_query (func, dbfd, sql_stmt, &res))
                return (-1);
        if ((row = mysql_fetch_row (res)) == NULL) {
                mysql_free_result (res);
                sprintf (sql_stmt, querymax4upd, poolname);
                if (vmgr_exec_query (func, dbfd, sql_stmt, &res))
                        return (-1);
                if ((row = mysql_fetch_row (res)) == NULL) {
                        mysql_free_result (res);
                        serrno = ENOENT;
                        return (-1);
                }
        }
        if (lock)
                strcpy (*rec_addr, row[i++]);
        strcpy (side_entry->vid, row[i++]);
        side_entry->side = atoi (row[i++]);
        strcpy (side_entry->poolname, row[i++]);
        side_entry->status = atoi (row[i++]);
        side_entry->estimated_free_space = atoi (row[i++]);
        side_entry->nbfiles = atoi (row[i++]);
        mysql_free_result (res);
        return (0);



}


vmgr_get_side_by_size(dbfd, poolname, Size, side_entry, lock, rec_addr)
struct vmgr_dbfd *dbfd;
char *poolname;
u_signed64 Size;
struct vmgr_tape_side *side_entry;
int lock;
vmgr_dbrec_addr *rec_addr;
{
	char func[22];
	int i = 0;
	static char querymin4upd[] =
		"SELECT ROWID, \
		 VID, SIDE, POOLNAME, STATUS, ESTIMATED_FREE_SPACE, NBFILES \
		FROM vmgr_tape_side \
		WHERE poolname = '%s' AND status = 0 AND \
		 estimated_free_space >= %d \
		ORDER BY estimated_free_space \
		LIMIT 1 \
		FOR UPDATE";
	static char querymax4upd[] =
		"SELECT ROWID, \
		 VID, SIDE, POOLNAME, STATUS, ESTIMATED_FREE_SPACE, NBFILES \
		FROM vmgr_tape_side \
		WHERE poolname = '%s' AND status = 0 \
		ORDER BY estimated_free_space DESC \
		LIMIT 1 \
		FOR UPDATE";
	int reqsize = (Size + 1023) / 1024;
	MYSQL_RES *res;
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "vmgr_get_side_by_size");
	sprintf (sql_stmt, querymin4upd, poolname, reqsize);
	if (vmgr_exec_query (func, dbfd, sql_stmt, &res))
		return (-1);
	if ((row = mysql_fetch_row (res)) == NULL) {
		mysql_free_result (res);
		sprintf (sql_stmt, querymax4upd, poolname);
		if (vmgr_exec_query (func, dbfd, sql_stmt, &res))
			return (-1);
		if ((row = mysql_fetch_row (res)) == NULL) {
			mysql_free_result (res);
			serrno = ENOENT;
			return (-1);
		}
	}
	if (lock)
		strcpy (*rec_addr, row[i++]);
	strcpy (side_entry->vid, row[i++]);
	side_entry->side = atoi (row[i++]);
	strcpy (side_entry->poolname, row[i++]);
	side_entry->status = atoi (row[i++]);
	side_entry->estimated_free_space = atoi (row[i++]);
	side_entry->nbfiles = atoi (row[i++]);
	mysql_free_result (res);
	return (0);
}

vmgr_insert_denmap_entry(dbfd, denmap_entry)
struct vmgr_dbfd *dbfd;
struct vmgr_tape_denmap *denmap_entry;
{
	char func[25];
	static char insert_stmt[] =
		"INSERT INTO vmgr_tape_denmap \
		(MD_MODEL, MD_MEDIA_LETTER, MD_DENSITY, NATIVE_CAPACITY) \
		VALUES \
		('%s', '%s', '%s', %d)";
	char sql_stmt[1024];

	strcpy (func, "vmgr_insert_denmap_entry");
	sprintf (sql_stmt, insert_stmt,
	    denmap_entry->md_model, denmap_entry->md_media_letter,
	    denmap_entry->md_density, denmap_entry->native_capacity);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			vmgrlogit (func, "INSERT error: %s\n",
			    mysql_error (&dbfd->mysql));
			serrno = SEINTERNAL;
		}
		return (-1);
	}
	return (0);
}

vmgr_insert_dgnmap_entry(dbfd, dgnmap_entry)
struct vmgr_dbfd *dbfd;
struct vmgr_tape_dgnmap *dgnmap_entry;
{
	char func[25];
	static char insert_stmt[] =
		"INSERT INTO vmgr_tape_dgnmap \
		(DGN, MODEL, LIBRARY) \
		VALUES \
		('%s', '%s', '%s')";
	char sql_stmt[1024];

	strcpy (func, "vmgr_insert_dgnmap_entry");
	sprintf (sql_stmt, insert_stmt,
	    dgnmap_entry->dgn, dgnmap_entry->model, dgnmap_entry->library);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			vmgrlogit (func, "INSERT error: %s\n",
			    mysql_error (&dbfd->mysql));
			serrno = SEINTERNAL;
		}
		return (-1);
	}
	return (0);
}

vmgr_insert_library_entry(dbfd, library_entry)
struct vmgr_dbfd *dbfd;
struct vmgr_tape_library *library_entry;
{
	char func[26];
	static char insert_stmt[] =
		"INSERT INTO vmgr_tape_library \
		(NAME, CAPACITY, NB_FREE_SLOTS, STATUS) \
		VALUES \
		('%s', %d, %d, %d)";
	char sql_stmt[1024];

	strcpy (func, "vmgr_insert_library_entry");
	sprintf (sql_stmt, insert_stmt,
	    library_entry->name, library_entry->capacity,
	    library_entry->nb_free_slots, library_entry->status);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			vmgrlogit (func, "INSERT error: %s\n",
			    mysql_error (&dbfd->mysql));
			serrno = SEINTERNAL;
		}
		return (-1);
	}
	return (0);
}

vmgr_insert_model_entry(dbfd, model_entry)
struct vmgr_dbfd *dbfd;
struct vmgr_tape_media *model_entry;
{
	char func[24];
	static char insert_stmt[] =
		"INSERT INTO vmgr_tape_media \
		(M_MODEL, M_MEDIA_LETTER, MEDIA_COST) \
		VALUES \
		('%s', '%s', %d)";
	char sql_stmt[1024];

	strcpy (func, "vmgr_insert_model_entry");
	sprintf (sql_stmt, insert_stmt,
	    model_entry->m_model, model_entry->m_media_letter,
	    model_entry->media_cost);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			vmgrlogit (func, "INSERT error: %s\n",
			    mysql_error (&dbfd->mysql));
			serrno = SEINTERNAL;
		}
		return (-1);
	}
	return (0);
}

vmgr_insert_pool_entry(dbfd, pool_entry)
struct vmgr_dbfd *dbfd;
struct vmgr_tape_pool *pool_entry;
{
	char capacity_str[21];
	char func[23];
	static char insert_stmt[] =
		"INSERT INTO vmgr_tape_pool \
		(NAME, OWNER_UID, GID, CAPACITY, TOT_FREE_SPACE) \
		VALUES \
		('%s', %d, %d, %s, %s)";
	char sql_stmt[1024];
	char tot_free_space_str[21];

	strcpy (func, "vmgr_insert_pool_entry");
	sprintf (sql_stmt, insert_stmt,
	    pool_entry->name, pool_entry->uid, pool_entry->gid,
	    u64tostr (pool_entry->capacity, capacity_str, -1),
	    u64tostr (pool_entry->tot_free_space, tot_free_space_str, -1));
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			vmgrlogit (func, "INSERT error: %s\n",
			    mysql_error (&dbfd->mysql));
			serrno = SEINTERNAL;
		}
		return (-1);
	}
	return (0);
}

vmgr_insert_side_entry(dbfd, side_entry)
struct vmgr_dbfd *dbfd;
struct vmgr_tape_side *side_entry;
{
	char func[23];
	static char insert_stmt[] =
		"INSERT INTO vmgr_tape_side \
		(VID, SIDE, POOLNAME, STATUS, ESTIMATED_FREE_SPACE, NBFILES) \
		VALUES \
		('%s', %d, '%s', %d, %d, %d)";
	char sql_stmt[1024];

	strcpy (func, "vmgr_insert_side_entry");
	sprintf (sql_stmt, insert_stmt,
	    side_entry->vid, side_entry->side, side_entry->poolname,
	    side_entry->status, side_entry->estimated_free_space,
	    side_entry->nbfiles);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			vmgrlogit (func, "INSERT error: %s\n",
			    mysql_error (&dbfd->mysql));
			serrno = SEINTERNAL;
		}
		return (-1);
	}
	return (0);
}

vmgr_insert_tag_entry(dbfd, tag_entry)
struct vmgr_dbfd *dbfd;
struct vmgr_tape_tag *tag_entry;
{
	char func[22];
	static char insert_stmt[] =
		"INSERT INTO vmgr_tape_tag \
		(VID, TEXT) \
		VALUES \
		('%s', '%s')";
	char sql_stmt[1024];

	strcpy (func, "vmgr_insert_tag_entry");
	sprintf (sql_stmt, insert_stmt,
	    tag_entry->vid, tag_entry->text);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			vmgrlogit (func, "INSERT error: %s\n",
			    mysql_error (&dbfd->mysql));
			serrno = SEINTERNAL;
		}
		return (-1);
	}
	return (0);
}

vmgr_insert_tape_entry(dbfd, tape_entry)
struct vmgr_dbfd *dbfd;
struct vmgr_tape_info *tape_entry;
{
	char func[23];
	static char insert_stmt[] =
		"INSERT INTO vmgr_tape_info \
		(VID, VSN, LIBRARY, DENSITY, LBLTYPE, MODEL, MEDIA_LETTER, \
		 MANUFACTURER, SN, NBSIDES, ETIME, RCOUNT, WCOUNT, RHOST, \
		 WHOST, RJID, WJID, RTIME, WTIME) \
		VALUES \
		('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d, %d, '%s', '%s', %d, %d, %d, %d)";
	char sql_stmt[1024];

	strcpy (func, "vmgr_insert_tape_entry");
	sprintf (sql_stmt, insert_stmt,
	    tape_entry->vid, tape_entry->vsn, tape_entry->library,
	    tape_entry->density, tape_entry->lbltype, tape_entry->model,
	    tape_entry->media_letter, tape_entry->manufacturer,
	    tape_entry->sn, tape_entry->nbsides, tape_entry->etime,
	    tape_entry->rcount, tape_entry->wcount, tape_entry->rhost,
	    tape_entry->whost, tape_entry->rjid, tape_entry->wjid,
	    tape_entry->rtime, tape_entry->wtime);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		if (mysql_errno (&dbfd->mysql) == ER_DUP_ENTRY)
			serrno = EEXIST;
		else {
			vmgrlogit (func, "INSERT error: %s\n",
			    mysql_error (&dbfd->mysql));
			serrno = SEINTERNAL;
		}
		return (-1);
	}
	return (0);
}

vmgr_list_denmap_entry(dbfd, bol, denmap_entry, endlist, dblistptr)
struct vmgr_dbfd *dbfd;
int bol;
struct vmgr_tape_denmap *denmap_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	char func[23];
	int i = 0;
	static char query[] =
		"SELECT \
		 MD_MODEL, MD_MEDIA_LETTER, MD_DENSITY, NATIVE_CAPACITY \
		FROM vmgr_tape_denmap \
		ORDER BY md_model, md_media_letter, native_capacity";
	MYSQL_ROW row;

	strcpy (func, "vmgr_list_denmap_entry");
	if (endlist) {
		if (*dblistptr)
			mysql_free_result (*dblistptr);
		return (1);
	}
	if (bol) {
		if (vmgr_exec_query (func, dbfd, query, dblistptr))
			return (-1);
	}
	if ((row = mysql_fetch_row (*dblistptr)) == NULL)
		return (1);
	strcpy (denmap_entry->md_model, row[i++]);
	strcpy (denmap_entry->md_media_letter, row[i++]);
	strcpy (denmap_entry->md_density, row[i++]);
	denmap_entry->native_capacity = atoi (row[i++]);
	return (0);
}

vmgr_list_dgnmap_entry(dbfd, bol, dgnmap_entry, endlist, dblistptr)
struct vmgr_dbfd *dbfd;
int bol;
struct vmgr_tape_dgnmap *dgnmap_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	char func[23];
	int i = 0;
	static char query[] =
		"SELECT \
		 DGN, MODEL, LIBRARY \
		FROM vmgr_tape_dgnmap \
		ORDER BY dgn";
	MYSQL_ROW row;

	strcpy (func, "vmgr_list_dgnmap_entry");
	if (endlist) {
		if (*dblistptr)
			mysql_free_result (*dblistptr);
		return (1);
	}
	if (bol) {
		if (vmgr_exec_query (func, dbfd, query, dblistptr))
			return (-1);
	}
	if ((row = mysql_fetch_row (*dblistptr)) == NULL)
		return (1);
	strcpy (dgnmap_entry->dgn, row[i++]);
	strcpy (dgnmap_entry->model, row[i++]);
	strcpy (dgnmap_entry->library, row[i++]);
	return (0);
}

/* bol indicates wheter the cursor needs to be open or not */

vmgr_list_dgn_entry(dbfd, bol, dgn_entry, endlist, dblistptr)
struct vmgr_dbfd *dbfd;
int bol;
struct vmgr_tape_dgn *dgn_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	char func[23];
	int i = 0;
	static char query[] =
		"SELECT \
		 DGN, WEIGHT \
		FROM vmgr_tape_dgn \
		ORDER BY dgn";
	MYSQL_ROW row;

	strcpy (func, "vmgr_list_dgn_entry");
	if (endlist) {
		if (*dblistptr)
			mysql_free_result (*dblistptr);
		return (1);
	}
	if (bol) {
		if (vmgr_exec_query (func, dbfd, query, dblistptr))
			return (-1);
	}
	if ((row = mysql_fetch_row (*dblistptr)) == NULL)
		return (1);
	strcpy (dgn_entry->dgn, row[i++]);
	dgn_entry->weight = strtol(row[i++], NULL, 10);
	dgn_entry->deltaweight = strtol(row[i++], NULL, 10);
	return (0);
}


vmgr_list_library_entry(dbfd, bol, library_entry, endlist, dblistptr)
struct vmgr_dbfd *dbfd;
int bol;
struct vmgr_tape_library *library_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	char func[24];
	int i = 0;
	static char query[] =
		"SELECT \
		 NAME, CAPACITY, NB_FREE_SLOTS, STATUS \
		FROM vmgr_tape_library \
		ORDER BY name";
	MYSQL_ROW row;

	strcpy (func, "vmgr_list_library_entry");
	if (endlist) {
		if (*dblistptr)
			mysql_free_result (*dblistptr);
		return (1);
	}
	if (bol) {
		if (vmgr_exec_query (func, dbfd, query, dblistptr))
			return (-1);
	}
	if ((row = mysql_fetch_row (*dblistptr)) == NULL)
		return (1);
	strcpy (library_entry->name, row[i++]);
	library_entry->capacity = atoi (row[i++]);
	library_entry->nb_free_slots = atoi (row[i++]);
	library_entry->status = atoi (row[i++]);
	return (0);
}

vmgr_list_model_entry(dbfd, bol, model_entry, endlist, dblistptr)
struct vmgr_dbfd *dbfd;
int bol;
struct vmgr_tape_media *model_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	char func[22];
	int i = 0;
	static char query[] =
		"SELECT \
		 M_MODEL, M_MEDIA_LETTER, MEDIA_COST \
		FROM vmgr_tape_media \
		ORDER BY m_model, m_media_letter";
	MYSQL_ROW row;

	strcpy (func, "vmgr_list_model_entry");
	if (endlist) {
		if (*dblistptr)
			mysql_free_result (*dblistptr);
		return (1);
	}
	if (bol) {
		if (vmgr_exec_query (func, dbfd, query, dblistptr))
			return (-1);
	}
	if ((row = mysql_fetch_row (*dblistptr)) == NULL)
		return (1);
	strcpy (model_entry->m_model, row[i++]);
	strcpy (model_entry->m_media_letter, row[i++]);
	model_entry->media_cost = atoi (row[i++]);
	return (0);
}

vmgr_list_pool_entry(dbfd, bol, pool_entry, endlist, dblistptr)
struct vmgr_dbfd *dbfd;
int bol;
struct vmgr_tape_pool *pool_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	char func[21];
	int i = 0;
	static char query[] =
		"SELECT \
		 NAME, OWNER_UID, GID, CAPACITY, TOT_FREE_SPACE \
		FROM vmgr_tape_pool \
		ORDER BY name";
	MYSQL_ROW row;

	strcpy (func, "vmgr_list_pool_entry");
	if (endlist) {
		if (*dblistptr)
			mysql_free_result (*dblistptr);
		return (1);
	}
	if (bol) {
		if (vmgr_exec_query (func, dbfd, query, dblistptr))
			return (-1);
	}
	if ((row = mysql_fetch_row (*dblistptr)) == NULL)
		return (1);
	strcpy (pool_entry->name, row[i++]);
	pool_entry->uid = atoi (row[i++]);
	pool_entry->gid = atoi (row[i++]);
	pool_entry->capacity = strtou64 (row[i++]);
	pool_entry->tot_free_space = strtou64 (row[i++]);
	return (0);
}

vmgr_list_side_entry(dbfd, bol, vid, pool_name, side_entry, endlist, dblistptr)
struct vmgr_dbfd *dbfd;
int bol;
char *vid;
char *pool_name;
struct vmgr_tape_side *side_entry;
int endlist;
DBLISTPTR *dblistptr;
{
	char func[21];
	int i = 0;
	static char query[] =
		"SELECT \
		 VID, SIDE, POOLNAME, STATUS, ESTIMATED_FREE_SPACE, NBFILES \
		FROM vmgr_tape_side \
		ORDER BY vid, side";
	static char query_p[] =
		"SELECT \
		 VID, SIDE, POOLNAME, STATUS, ESTIMATED_FREE_SPACE, NBFILES \
		FROM vmgr_tape_side \
		WHERE poolname = '%s' \
		ORDER BY vid, side";
	static char query_v[] =
		"SELECT \
		 VID, SIDE, POOLNAME, STATUS, ESTIMATED_FREE_SPACE, NBFILES \
		FROM vmgr_tape_side \
		WHERE vid = '%s' \
		ORDER BY side";
	MYSQL_ROW row;
	char sql_stmt[1024];

	strcpy (func, "vmgr_list_side_entry");
	if (endlist) {
		if (*dblistptr)
			mysql_free_result (*dblistptr);
		return (1);
	}
	if (bol) {
		if (*vid)
			sprintf (sql_stmt, query_v, vid);
		else if (*pool_name)
			sprintf (sql_stmt, query_p, pool_name);
		else
			strcpy (sql_stmt, query);
		if (vmgr_exec_query (func, dbfd, sql_stmt, dblistptr))
			return (-1);
	}
	if ((row = mysql_fetch_row (*dblistptr)) == NULL)
		if (*vid && bol) {
			mysql_free_result (*dblistptr);
			serrno = ENOENT;
			return (-1);
		} else
			return (1);
	strcpy (side_entry->vid, row[i++]);
	side_entry->side = atoi (row[i++]);
	strcpy (side_entry->poolname, row[i++]);
	side_entry->status = atoi (row[i++]);
	side_entry->estimated_free_space = atoi (row[i++]);
	side_entry->nbfiles = atoi (row[i++]);
	return (0);
}

vmgr_opendb(db_srvr, db_user, db_pwd, dbfd)
char *db_srvr;
char *db_user;
char *db_pwd;
struct vmgr_dbfd *dbfd;
{
	char func[16];
	int ntries;

	strcpy (func, "vmgr_opendb");
	(void) mysql_init (&dbfd->mysql);
	ntries = 0;
	while (1) {
		if (mysql_real_connect (&dbfd->mysql, db_srvr, db_user, db_pwd,
		    "vmgr_db", 0, NULL, 0)) return (0);
		if (ntries++ >= MAXRETRY) break;
		sleep (RETRYI);
	}
	vmgrlogit (func, "CONNECT error: %s\n",
	    mysql_error (&dbfd->mysql));
	serrno = SEINTERNAL;
	return (-1);
}

vmgr_start_tr(s, dbfd)
int s;
struct vmgr_dbfd *dbfd;
{
	(void) mysql_query (&dbfd->mysql, "BEGIN");
	dbfd->tr_started = 1;
	return (0);
}

vmgr_update_library_entry(dbfd, rec_addr, library_entry)
struct vmgr_dbfd *dbfd;
vmgr_dbrec_addr *rec_addr;
struct vmgr_tape_library *library_entry;
{
	char func[26];
	char sql_stmt[1024];
	static char update_stmt[] =
		"UPDATE vmgr_tape_library SET \
		CAPACITY = %d, NB_FREE_SLOTS = %d, STATUS = %d \
		WHERE ROWID = %s";

	strcpy (func, "vmgr_update_library_entry");
	sprintf (sql_stmt, update_stmt,
	    library_entry->capacity, library_entry->nb_free_slots,
	    library_entry->status, *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		vmgrlogit (func, "UPDATE error: %s\n",
		    mysql_error (&dbfd->mysql));
		serrno = SEINTERNAL;
		return (-1);
	}
	return (0);
}

vmgr_update_model_entry(dbfd, rec_addr, model_entry)
struct vmgr_dbfd *dbfd;
vmgr_dbrec_addr *rec_addr;
struct vmgr_tape_media *model_entry;
{
	char func[24];
	char sql_stmt[1024];
	static char update_stmt[] =
		"UPDATE vmgr_tape_media SET \
		MEDIA_COST = %d \
		WHERE ROWID = %s";

	strcpy (func, "vmgr_update_model_entry");
	sprintf (sql_stmt, update_stmt,
	    model_entry->media_cost,
	    *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		vmgrlogit (func, "UPDATE error: %s\n",
		    mysql_error (&dbfd->mysql));
		serrno = SEINTERNAL;
		return (-1);
	}
	return (0);
}

vmgr_update_pool_entry(dbfd, rec_addr, pool_entry)
struct vmgr_dbfd *dbfd;
vmgr_dbrec_addr *rec_addr;
struct vmgr_tape_pool *pool_entry;
{
	char capacity_str[21];
	char func[23];
	char sql_stmt[1024];
	char tot_free_space_str[21];
	static char update_stmt[] =
		"UPDATE vmgr_tape_pool SET \
		OWNER_UID = %d, GID = %d, CAPACITY = %s, TOT_FREE_SPACE = %s \
		WHERE ROWID = %s";

	strcpy (func, "vmgr_update_pool_entry");
	sprintf (sql_stmt, update_stmt,
	    pool_entry->uid, pool_entry->gid,
	    u64tostr (pool_entry->capacity, capacity_str, -1),
	    u64tostr (pool_entry->tot_free_space, tot_free_space_str, -1),
	    *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		vmgrlogit (func, "UPDATE error: %s\n",
		    mysql_error (&dbfd->mysql));
		serrno = SEINTERNAL;
		return (-1);
	}
	return (0);
}

vmgr_update_side_entry(dbfd, rec_addr, side_entry)
struct vmgr_dbfd *dbfd;
vmgr_dbrec_addr *rec_addr;
struct vmgr_tape_side *side_entry;
{
	char func[23];
	char sql_stmt[1024];
	static char update_stmt[] =
		"UPDATE vmgr_tape_side SET \
		POOLNAME = '%s', STATUS = %d, ESTIMATED_FREE_SPACE = %d, NBFILES = %d \
		WHERE ROWID = %s";

	strcpy (func, "vmgr_update_side_entry");
	sprintf (sql_stmt, update_stmt,
	    side_entry->poolname, side_entry->status,
	    side_entry->estimated_free_space, side_entry->nbfiles,
	    *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		vmgrlogit (func, "UPDATE error: %s\n",
		    mysql_error (&dbfd->mysql));
		serrno = SEINTERNAL;
		return (-1);
	}
	return (0);
}

vmgr_update_tag_entry(dbfd, rec_addr, tag_entry)
struct vmgr_dbfd *dbfd;
vmgr_dbrec_addr *rec_addr;
struct vmgr_tape_tag *tag_entry;
{
	char func[22];
	char sql_stmt[1024];
	static char update_stmt[] =
		"UPDATE vmgr_tape_tag SET \
		TEXT = '%s' \
		WHERE ROWID = %s";

	strcpy (func, "vmgr_update_tag_entry");
	sprintf (sql_stmt, update_stmt,
	    tag_entry->text,
	    *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		vmgrlogit (func, "UPDATE error: %s\n",
		    mysql_error (&dbfd->mysql));
		serrno = SEINTERNAL;
		return (-1);
	}
	return (0);
}

vmgr_update_tape_entry(dbfd, rec_addr, tape_entry)
struct vmgr_dbfd *dbfd;
vmgr_dbrec_addr *rec_addr;
struct vmgr_tape_info *tape_entry;
{
	char func[23];
	char sql_stmt[1024];
	static char update_stmt[] =
		"UPDATE vmgr_tape_info SET \
		VSN = '%s', LIBRARY = '%s', DENSITY = '%s', LBLTYPE = '%s', \
		MODEL = '%s', MEDIA_LETTER = '%s', MANUFACTURER = '%s', \
		SN = '%s', NBSIDES = %d, ETIME = %d, RCOUNT = %d, \
		WCOUNT = %d, RHOST = '%s', WHOST = '%s', \
		RJID = %d, WJID = %d, RTIME = %d, WTIME = %d \
		WHERE ROWID = %s";

	strcpy (func, "vmgr_update_tape_entry");
	sprintf (sql_stmt, update_stmt,
	    tape_entry->vsn, tape_entry->library, tape_entry->density,
	    tape_entry->lbltype, tape_entry->model, tape_entry->media_letter,
	    tape_entry->manufacturer, tape_entry->sn, tape_entry->nbsides,
	    tape_entry->etime, tape_entry->rcount, tape_entry->wcount,
	    tape_entry->rhost, tape_entry->whost, tape_entry->rjid,
	    tape_entry->wjid, tape_entry->rtime, tape_entry->wtime,
	    *rec_addr);
	if (mysql_query (&dbfd->mysql, sql_stmt)) {
		vmgrlogit (func, "UPDATE error: %s\n",
		    mysql_error (&dbfd->mysql));
		serrno = SEINTERNAL;
		return (-1);
	}
	return (0);
}
