/*
 * Cupv_server.h,v 1.5 2002/06/06 14:18:39 bcouturi Exp
 */

/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)Cupv_server.h,v 1.5 2002/06/06 14:18:39 CERN IT-PDP/DM Ben Couturier
 */
 
#ifndef _CUPV_SERVER_H
#define _CUPV_SERVER_H
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

#ifdef CSEC
#include "Csec_api.h"
#endif


#include "Cupv_struct.h"
#include "Cupv_util.h" 
                        /* UPV constants and macros */

#define CHECKI	5	/* max interval to check for work to be done */
#define CUPV_NBTHREADS	6

#define RETURN(x) \
        { \
    	Cupvlogit (func, "returns %d\n", (x)); \
    	if (thip->dbfd.tr_started) \
    		if (x) \
    			(void) Cupv_abort_tr (&thip->dbfd); \
    		else \
    			(void) Cupv_end_tr (&thip->dbfd); \
    	return ((x)); \
    	}

			/* UPV tables and structures */

struct Cupv_dbfd {
	int		idx;		/* index in array of Cupv_dbfd */
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
typedef char Cupv_dbrec_addr[19];
typedef int DBLISTPTR;
#else
#ifdef USE_MYSQL
typedef char Cupv_dbrec_addr[21];
typedef MYSQL_RES *DBLISTPTR;
#else
#ifdef USE_RAIMA
typedef DB_ADDR Cupv_dbrec_addr;
#else
typedef Cdb_off_t Cupv_dbrec_addr;
typedef Cdb_off_t DBLISTPTR;
#endif
#endif
#endif

struct Cupv_srv_thread_info {
        int		s;		/* socket for communication with client */
        int		db_open_done;
        struct Cupv_dbfd dbfd;
#ifdef CSEC
        Csec_context_t sec_ctx;
        uid_t Csec_uid;
        gid_t Csec_gid;
        int Csec_service_type;    /* Type of the service if client is another Castor server */
#endif
};

EXTERN_C int Cupv_util_check _PROTO ((struct Cupv_userpriv *, struct Cupv_srv_thread_info *thip));
EXTERN_C int Cupv_check_domain _PROTO((char *));
EXTERN_C int Cupv_check_regexp _PROTO((char *));
EXTERN_C int Cupv_check_regexp_syntax _PROTO((char *));
EXTERN_C int Cupv_compare_priv _PROTO((struct Cupv_userpriv *, struct Cupv_userpriv *));
EXTERN_C int Cupv_opendb _PROTO((char *, char *, char *, struct Cupv_dbfd *));
EXTERN_C int Cupv_start_tr _PROTO((int, struct Cupv_dbfd *));
EXTERN_C int Cupv_end_tr _PROTO((struct Cupv_dbfd *)); 
EXTERN_C int Cupv_init_dbpkg _PROTO (());
EXTERN_C int Cupv_abort_tr _PROTO ((struct Cupv_dbfd *));
EXTERN_C int Cupv_closedb  _PROTO ((struct Cupv_dbfd *));

/*  EXTERN_C int Cupv_add_privilege _PROTO ((struct Cupv_dbfd *, int, int, char *, char *, int)); */
/*  EXTERN_C int Cupv_delete_privilege _PROTO ((struct Cupv_dbfd *, int, int, char *, char *)); */
/*  EXTERN_C int Cupv_list_privilege  _PROTO((struct Cupv_dbfd *, int, struct Cupv_userpriv *)); */

EXTERN_C int Cupv_get_privilege_entry _PROTO ((struct Cupv_dbfd *, struct Cupv_userpriv *, int, Cupv_dbrec_addr *));
EXTERN_C int Cupv_delete_privilege_entry _PROTO ((struct Cupv_dbfd *, Cupv_dbrec_addr *));
EXTERN_C int Cupv_insert_privilege_entry _PROTO ((struct Cupv_dbfd *, struct Cupv_userpriv *));
EXTERN_C int Cupv_update_privilege_entry _PROTO ((struct Cupv_dbfd *, Cupv_dbrec_addr *, struct Cupv_userpriv *));
EXTERN_C int Cupv_list_privilege_entry   _PROTO ((struct Cupv_dbfd *, int, 
						  struct Cupv_userpriv *,
						  struct Cupv_userpriv *,
						  int, DBLISTPTR *));

#endif









