/*
 *
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * @(#)$RCSfile: dlf_server.h,v $ $Revision: 1.1 $ $Date: 2003/08/20 13:43:50 $ CERN IT-ADC Vitaly Motyakov
 */
 
#ifndef _DLF_SERVER_H
#define _DLF_SERVER_H
#ifdef USE_ORACLE
#ifdef USE_OCI
#include <ocidfn.h>
#ifdef __STDC__
#include <ociapr.h>
#else
#include <ocikpr.h>
#endif
#if defined(__alpha) && defined(__osf__)
#define HDA_SIZE 512
#else
#define HDA_SIZE 256
#endif
#endif
#else
#ifdef USE_MYSQL
#include <mysql.h>
#else
#ifdef USE_RAIMA
#include <rds.h>
#else
#include "Cdb_api.h"
#endif
#endif
#endif
#include "dlf_struct.h"

                        /* dlf constants and macros */

#define CHECKI	5	/* max interval to check for work to be done */
#define DLF_NBTHREADS	6
#define RETURN(x) \
	{ \
/*	dlflogit (func, "returns %d\n", (x)); */ \
	if (thip->dbfd.tr_started) \
		if (x) \
			(void) dlf_abort_tr (&thip->dbfd); \
		else \
			(void) dlf_end_tr (&thip->dbfd); \
	return ((x)); \
	}

			/* dlf tables and structures */

struct dlf_dbfd {
	int		idx;		/* index in array of dlf_dbfd */
#ifdef USE_ORACLE
#ifdef USE_OCI
	Lda_Def		lda;		/* logon data area */
	ub1		hda[HDA_SIZE];	/* host data area */
#endif
#else
#ifdef USE_MYSQL
        MYSQL           mysql;
#else
#ifdef USE_RAIMA
	RDM_SESS	hSess;		/* session handle */
	RDM_DB		hDb;		/* database handle */
#else
	Cdb_sess_t	Cdb_sess;
	Cdb_db_t	Cdb_db;
#endif
#endif
#endif
	int		tr_started;
};

#ifdef USE_ORACLE
typedef char dlf_dbrec_addr[19];
typedef int DBLISTPTR;
#else
#ifdef USE_MYSQL
typedef char dlf_dbrec_addr[21];
typedef MYSQL_RES *DBLISTPTR;
#else
#ifdef USE_RAIMA
typedef DB_ADDR vmgr_dbrec_addr;
#else
typedef Cdb_off_t vmgr_dbrec_addr;
typedef Cdb_off_t DBLISTPTR;
#endif
#endif
#endif

struct dlf_srv_thread_info {
	int		s;		/* socket for communication with client */
	int		db_open_done;
	struct dlf_dbfd dbfd;
	char		errbuf[PRTBUFSZ];
};


/* DLF server function prototypes */

EXTERN_C void DLL_DECL dlf_logreq _PROTO((char*, char*));
EXTERN_C int DLL_DECL sendrep _PROTO((int, int, ...));
EXTERN_C int DLL_DECL dlf_srv_getmsgtexts _PROTO((int, char*, char*, struct dlf_srv_thread_info*));
EXTERN_C int DLL_DECL dlf_srv_getfacilites _PROTO((int, char*, char*, struct dlf_srv_thread_info*));
EXTERN_C int DLL_DECL dlf_srv_enterfacility _PROTO((int, char*, char*, struct dlf_srv_thread_info*));
EXTERN_C int DLL_DECL dlf_srv_entertext _PROTO((int, char*, char*, struct dlf_srv_thread_info*));
EXTERN_C int DLL_DECL dlf_srv_modfacility _PROTO((int, char*, char*, struct dlf_srv_thread_info*));
EXTERN_C int DLL_DECL dlf_srv_delfacility _PROTO((int, char*, char*, struct dlf_srv_thread_info*));
EXTERN_C int DLL_DECL dlf_srv_modtext _PROTO((int, char*, char*, struct dlf_srv_thread_info*));
EXTERN_C int DLL_DECL dlf_srv_deltext _PROTO((int, char*, char*, struct dlf_srv_thread_info*));
EXTERN_C int DLL_DECL dlf_srv_entermessage _PROTO((int, char*, int, char*, struct dlf_srv_thread_info*, int*));
EXTERN_C int DLL_DECL dlf_srv_shutdown _PROTO((int, char*, char*, struct dlf_srv_thread_info*));

EXTERN_C int DLL_DECL dlf_init_dbpkg _PROTO((void));
EXTERN_C int DLL_DECL dlf_opendb _PROTO((char*, char*, char*, struct dlf_dbfd*));
EXTERN_C int DLL_DECL dlf_abort_tr _PROTO((struct dlf_dbfd*));
EXTERN_C int DLL_DECL dlf_closedb _PROTO((struct dlf_dbfd*));
EXTERN_C int DLL_DECL dlf_end_tr _PROTO((struct dlf_dbfd*));

#ifdef USE_MYSQL
EXTERN_C int DLL_DECL dlf_exec_query _PROTO((char*, struct dlf_dbfd*, char*, MYSQL_RES**));
#endif

EXTERN_C int DLL_DECL dlf_insert_message_entry _PROTO((struct dlf_dbfd*, dlf_log_message_t*));
EXTERN_C int DLL_DECL dlf_get_host_id _PROTO((struct dlf_dbfd*, const char*, int*, DBLISTPTR*));
EXTERN_C int DLL_DECL dlf_insert_text_entry _PROTO((struct dlf_dbfd*, const char*, int, const char*));
EXTERN_C int DLL_DECL dlf_modify_text_entry _PROTO((struct dlf_dbfd*, const char*, int, const char*));
EXTERN_C int DLL_DECL dlf_delete_facility_entry _PROTO((struct dlf_dbfd*, const char*));
EXTERN_C int DLL_DECL dlf_modify_facility_entry _PROTO((struct dlf_dbfd*, const char*, int));
EXTERN_C int DLL_DECL dlf_delete_text_entry _PROTO((struct dlf_dbfd*, const char*, int));
EXTERN_C int DLL_DECL dlf_insert_facility_entry _PROTO((struct dlf_dbfd*, int, const char*));
EXTERN_C int DLL_DECL dlf_get_facility_no _PROTO((struct dlf_dbfd*, const char*, int*, DBLISTPTR*));
EXTERN_C int DLL_DECL dlf_get_facility_entry _PROTO((struct dlf_dbfd*, int, int*, char*, int, DBLISTPTR*));
EXTERN_C int DLL_DECL dlf_get_text_entry _PROTO((struct dlf_dbfd*, int, int, int*, char*, int, DBLISTPTR*));
EXTERN_C int DLL_DECL dlf_start_tr _PROTO((int, struct dlf_dbfd*));

#ifdef USE_ORACLE
EXTERN_C int DLL_DECL dlf_get_curr_host_sequence _PROTO((struct dlf_dbfd*, int*));
#endif

#endif
