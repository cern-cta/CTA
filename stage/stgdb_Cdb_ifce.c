/*
 * $Id: stgdb_Cdb_ifce.c,v 1.23 2001/12/04 10:33:23 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <stdio.h>              /* Contains BUFSIZ */
#include <stdlib.h>             /* Contains qsort */
#include <unistd.h>             /* Contains sleep */
#include <serrno.h>

#include "osdep.h"
#include "stgdb_Cdb_ifce.h"
#include "Cstage_db.h"
#include "Cstage_ifce.h"

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stgdb_Cdb_ifce.c,v $ $Revision: 1.23 $ $Date: 2001/12/04 10:33:23 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

int stgdb_stcpcmp _PROTO((CONST void *, CONST void *));
int stgdb_stppcmp _PROTO((CONST void *, CONST void *));
extern int stglogit _PROTO(());
extern char func[];
#ifndef USECDB
u_signed64 local_uniqueid = 0;
#endif

#ifdef STGDB_CONRETRY
#undef STGDB_CONRETRY
#endif
#define STGDB_CONRETRY 6

#ifdef STGDB_CONRETRYINT
#undef STGDB_CONRETRYINT
#endif
#define STGDB_CONRETRYINT 10

#ifdef PING
#undef PING
#endif

#define PING {																				\
	if (Cdb_ping(&(dbfd->Cdb_sess)) != 0) {													\
		stglogit("stgdb_Cdb_ifce","### Error at %s:%d: Cannot ping to Cdb (%s)\n",			\
						 __FILE__,__LINE__, sstrerror(serrno));								\
		if ((serrno == EDB_A_ESESSION) || (serrno == SECOMERR) || (serrno == SECONNDROP)) {	\
			int retry_status;																\
			if (stgdb_logout(dbfd) != 0) {												\
				stglogit("stgdb_Cdb_ifce","### Error at %s:%d: Cannot logout from Cdb (%s)\n",	\
								 __FILE__,__LINE__, sstrerror(serrno));					\
			}																			\
			RECONNECT(retry_status);														\
			if (retry_status != 0) {														\
				stglogit("stgdb_Cdb_ifce","### Error at %s:%d: Cannot reconnect to Cdb (%s)\n",	\
								 __FILE__,__LINE__, sstrerror(serrno));						\
				return(-1);																	\
			}																				\
			if (stgdb_open(dbfd,"stage") != 0) {											\
				stglogit("stgdb_Cdb_ifce","### Error at %s:%d: Cannot reopen database (%s)\n",	\
								 __FILE__,__LINE__, sstrerror(serrno));						\
				if (stgdb_logout(dbfd) != 0) {												\
					stglogit("stgdb_Cdb_ifce","### Error at %s:%d: Cannot logout from Cdb (%s)\n",	\
									 __FILE__,__LINE__, sstrerror(serrno));					\
				}																			\
				return(-1);																	\
			}																				\
			stglogit("stgdb_Cdb_ifce","--> Reconnected to Database\n");						\
		} else {																			\
			stglogit("stgdb_Cdb_ifce","### Non-recoverable ping error at %s:%d (%s)\n",		\
								 __FILE__,__LINE__,sstrerror(serrno));						\
		}																					\
	}																						\
}

#ifdef RECONNECT
#undef RECONNECT
#endif

#define RECONNECT(retry_status) {															\
	int iretry;																				\
	stglogit("stgdb_Cdb_ifce","### Warning at %s:%d: Connection with Cdb is lost. "			\
					 "Retry to connect %d times (%d seconds between each retry).\n",		\
					 __FILE__,__LINE__,STGDB_CONRETRY,STGDB_CONRETRYINT);					\
	retry_status = -1;																		\
	for (iretry = 0; iretry < STGDB_CONRETRY; iretry++) {									\
		if (stgdb_login(dbfd) == 0) {														\
			retry_status = 0;																\
			break;																			\
		}																					\
		stglogit("stgdb_Cdb_ifce","### Warning at %s:%d: Cannot login to Cdb at retry "		\
						 "No %d (%s)\n",													\
						 __FILE__,__LINE__,iretry,sstrerror(serrno));						\
		sleep(STGDB_CONRETRYINT);															\
	}																						\
}

#ifdef RETURN
#undef RETURN
#endif

#define RETURN(a) {																			\
	if (a != 0) {																			\
		stglogit("stgdb_Cdb_ifce","### Error at %s:%d: return code is %d (%s)\n",			\
						 __FILE__,__LINE__, a, sstrerror(serrno));							\
		if ((serrno == EDB_A_ESESSION) || (serrno == SECOMERR) || (serrno == SECONNDROP)) {	\
			int retry_status;																\
			if (stgdb_logout(dbfd) != 0) {													\
				stglogit("stgdb_Cdb_ifce","### Error at %s:%d: Cannot logout from Cdb (%s)\n",	\
						__FILE__,__LINE__, sstrerror(serrno));								\
			}																				\
			RECONNECT(retry_status);														\
			if (retry_status != 0) {														\
				stglogit("stgdb_Cdb_ifce","### Error at %s:%d: Cannot reconnect to Cdb (%s).\n",	\
								 __FILE__,__LINE__, sstrerror(serrno));						\
				return(a);																	\
			}																				\
			if (stgdb_open(dbfd,"stage") != 0) {											\
				stglogit("stgdb_Cdb_ifce","### Error at %s:%d: Cannot reopen database (%s)\n",	\
								 __FILE__,__LINE__,sstrerror(serrno));						\
				if (stgdb_logout(dbfd) != 0) {												\
					stglogit("stgdb_Cdb_ifce","### Error at %s:%d: Cannot logout from Cdb (%s)\n",	\
									 __FILE__,__LINE__, sstrerror(serrno));					\
				}																			\
				return(a);																	\
			}																				\
			stglogit("stgdb_Cdb_ifce","--> Reconnected to Database\n");						\
		} else {																			\
			stglogit("stgdb_Cdb_ifce","### Non-recoverable dial error at %s:%d: %s\n",		\
								 __FILE__,__LINE__,sstrerror(serrno));						\
		}																					\
	}																						\
	return(a);																				\
}

int DLL_DECL Stgdb_login(dbfd,file,line)
		 stgdb_fd_t *dbfd;
		 char *file;
		 int line;
{
#ifdef USECDB
	if (Cdb_login(dbfd->username,dbfd->password,&(dbfd->Cdb_sess)) != 0) {
		return(-1);
	}
#endif

	return(0);
}

int DLL_DECL Stgdb_logout(dbfd,file,line)
		 stgdb_fd_t *dbfd;
		 char *file;
		 int line;
{
#ifdef USECDB
	return(Cdb_logout(&(dbfd->Cdb_sess)));
#else
	return(0);
#endif
}

int DLL_DECL Stgdb_open(dbfd,dbname,file,line)
		 stgdb_fd_t *dbfd;
		 char *dbname;
		 char *file;
		 int line;
{
#ifdef USECDB
	if (Cdb_open(&(dbfd->Cdb_sess),dbname,&Cdb_stage_interface,&(dbfd->Cdb_db)) != 0) {
		return(-1);
	}
#endif

	return(0);
}

int DLL_DECL Stgdb_close(dbfd,file,line)
		 stgdb_fd_t *dbfd;
		 char *file;
		 int line;
{
#ifdef USECDB
	return(Cdb_close(&(dbfd->Cdb_db)));
#else
	return(0);
#endif
}

int DLL_DECL Stgdb_load(dbfd,stcsp,stcep,stgcat_bufsz,stpsp,stpep,stgpath_bufsz,file,line)
		 stgdb_fd_t *dbfd;
		 struct stgcat_entry **stcsp;
		 struct stgcat_entry **stcep;
		 size_t *stgcat_bufsz;
		 struct stgpath_entry **stpsp;
		 struct stgpath_entry **stpep;
		 size_t *stgpath_bufsz;
		 char *file;
		 int line;
{
#ifdef USECDB
	struct stgcat_entry *stcp;
	struct stgpath_entry *stpp;
	struct stgcat_tape tape;
	struct stgcat_disk disk;
	struct stgcat_hpss hsm;
	struct stgcat_hsm castor;
	struct stgcat_link link;
	struct stgcat_alloc alloc;

	/* stglogit(func, "In stgdb_load called at %s:%d\n",file,line); */

	PING;

	/* We init the variable */
	*stcsp = NULL;
	*stpsp = NULL;

	/* We allocate blocks of stcp and stpp */
	*stgcat_bufsz = BUFSIZ;
	*stgpath_bufsz = BUFSIZ;

	if ((*stcsp = (struct stgcat_entry *) malloc(*stgcat_bufsz)) == NULL) {
		goto _stgdb_load_error;
	}
	*stcep = *stcsp + (*stgcat_bufsz/sizeof(struct stgcat_entry));

	/* We makes sure that the critical variable, stcp->reqid, is initalized */
	for (stcp = *stcsp; stcp < *stcep; stcp++) {
		stcp->reqid = 0;
	}

	/* We say where to start */
	stcp = *stcsp;

	if ((*stpsp = (struct stgpath_entry *) calloc(1, *stgpath_bufsz)) == NULL) {
		goto _stgdb_load_error;
	}
	*stpep = *stpsp + (*stgpath_bufsz/sizeof(struct stgpath_entry));

	/* We makes sure that the critical variable, stpp->reqid, is initalized */
	for (stpp = *stpsp; stpp < *stpep; stpp++) {
		stpp->reqid = 0;
	}

	/* We say where to start */
	stpp = *stpsp;

	/* We ask for a dump of tape table from Cdb */
	/* ---------------------------------------- */
	if (Cdb_dump_start(&(dbfd->Cdb_db),"stgcat_tape") != 0) {
		goto _stgdb_load_error;
	}
	while (Cdb_dump(&(dbfd->Cdb_db),"stgcat_tape",NULL,&tape) == 0) {
		struct stgcat_entry thisstcp;

		if (Cdb2stcp(&thisstcp,&tape,NULL,NULL,NULL,NULL) != 0) {
			continue;
		}
		if (stcp == *stcep) {
			struct stgcat_entry *newstcs;
			/* The calloced area is not enough */
			*stgcat_bufsz += BUFSIZ;
			if ((newstcs = (struct stgcat_entry *) realloc(*stcsp,*stgcat_bufsz)) == NULL) {
				Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_tape");
				goto _stgdb_load_error;
			}
			*stcsp = newstcs;
			*stcep = *stcsp + (*stgcat_bufsz/sizeof(struct stgcat_entry));

			/* We makes sure that critical variable, stcp->reqid, is initalized */
			for (stcp = *stcsp + ((*stgcat_bufsz - BUFSIZ)/sizeof(struct stgcat_entry));
					 stcp < *stcep; stcp++) {
				stcp->reqid = 0;
			}

			/* We say where to continue */
			stcp = *stcsp + ((*stgcat_bufsz - BUFSIZ)/sizeof(struct stgcat_entry));

		}
		*stcp++ = thisstcp;
	}
	if (serrno != EDB_D_DUMPEND) {
		int save_serrno = serrno;
		Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_tape");
		serrno = save_serrno;
		goto _stgdb_load_error;
    }
 	Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_tape");

	/* We ask for a dump of disk table from Cdb */
	/* ---------------------------------------- */
	if (Cdb_dump_start(&(dbfd->Cdb_db),"stgcat_disk") != 0) {
		goto _stgdb_load_error;
	}
	while (Cdb_dump(&(dbfd->Cdb_db),"stgcat_disk",NULL,&disk) == 0) {
		struct stgcat_entry thisstcp;
		if (Cdb2stcp(&thisstcp,NULL,&disk,NULL,NULL,NULL) != 0) {
			continue;
		}
		if (stcp == *stcep) {
			struct stgcat_entry *newstcs;
			/* The calloced area is not enough */
			*stgcat_bufsz += BUFSIZ;
			if ((newstcs = (struct stgcat_entry *) realloc(*stcsp,*stgcat_bufsz)) == NULL) {
				Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_disk");
				goto _stgdb_load_error;
			}
			*stcsp = newstcs;
			*stcep = *stcsp + (*stgcat_bufsz/sizeof(struct stgcat_entry));

			/* We makes sure that critical variable, stcp->reqid, is initalized */
			for (stcp = *stcsp + ((*stgcat_bufsz - BUFSIZ)/sizeof(struct stgcat_entry));
					 stcp < *stcep; stcp++) {
				stcp->reqid = 0;
			}

			/* We say where to continue */
			stcp = *stcsp + ((*stgcat_bufsz - BUFSIZ)/sizeof(struct stgcat_entry));

		}
		*stcp++ = thisstcp;
	}
	if (serrno != EDB_D_DUMPEND) {
		int save_serrno = serrno;
		Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_disk");
		serrno = save_serrno;
		goto _stgdb_load_error;
    }
	Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_disk");

	/* We ask for a dump of hsm table from Cdb */
	/* --------------------------------------- */
	if (Cdb_dump_start(&(dbfd->Cdb_db),"stgcat_hpss") != 0) {
		goto _stgdb_load_error;
	}
	while (Cdb_dump(&(dbfd->Cdb_db),"stgcat_hpss",NULL,&hsm) == 0) {
		struct stgcat_entry thisstcp;
		if (Cdb2stcp(&thisstcp,NULL,NULL,&hsm,NULL,NULL) != 0) {
			continue;
		}
		if (stcp == *stcep) {
			struct stgcat_entry *newstcs;
			/* The calloced area is not enough */
			*stgcat_bufsz += BUFSIZ;
			if ((newstcs = (struct stgcat_entry *) realloc(*stcsp,*stgcat_bufsz)) == NULL) {
				Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_hpss");
				goto _stgdb_load_error;
			}
			*stcsp = newstcs;
			*stcep = *stcsp + (*stgcat_bufsz/sizeof(struct stgcat_entry));

			/* We makes sure that critical variable, stcp->reqid, is initalized */
			for (stcp = *stcsp + ((*stgcat_bufsz - BUFSIZ)/sizeof(struct stgcat_entry));
					 stcp < *stcep; stcp++) {
				stcp->reqid = 0;
			}

			/* We say where to continue */
			stcp = *stcsp + ((*stgcat_bufsz - BUFSIZ)/sizeof(struct stgcat_entry));

		}
		*stcp++ = thisstcp;
	}
	if (serrno != EDB_D_DUMPEND) {
		int save_serrno = serrno;
		Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_hpss");
		serrno = save_serrno;
		goto _stgdb_load_error;
    }
	Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_hpss");

	/* We ask for a dump of castor table from Cdb */
	/* ------------------------------------------ */
	if (Cdb_dump_start(&(dbfd->Cdb_db),"stgcat_hsm") != 0) {
		goto _stgdb_load_error;
	}
	while (Cdb_dump(&(dbfd->Cdb_db),"stgcat_hsm",NULL,&castor) == 0) {
		struct stgcat_entry thisstcp;
		if (Cdb2stcp(&thisstcp,NULL,NULL,NULL,&castor,NULL) != 0) {
			continue;
		}
		if (stcp == *stcep) {
			struct stgcat_entry *newstcs;
			/* The calloced area is not enough */
			*stgcat_bufsz += BUFSIZ;
			if ((newstcs = (struct stgcat_entry *) realloc(*stcsp,*stgcat_bufsz)) == NULL) {
				Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_hsm");
				goto _stgdb_load_error;
			}
			*stcsp = newstcs;
			*stcep = *stcsp + (*stgcat_bufsz/sizeof(struct stgcat_entry));

			/* We makes sure that critical variable, stcp->reqid, is initalized */
			for (stcp = *stcsp + ((*stgcat_bufsz - BUFSIZ)/sizeof(struct stgcat_entry));
					 stcp < *stcep; stcp++) {
				stcp->reqid = 0;
			}

			/* We say where to continue */
			stcp = *stcsp + ((*stgcat_bufsz - BUFSIZ)/sizeof(struct stgcat_entry));

		}
		*stcp++ = thisstcp;
	}
	if (serrno != EDB_D_DUMPEND) {
		int save_serrno = serrno;
		Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_hsm");
		serrno = save_serrno;
		goto _stgdb_load_error;
    }
	Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_hsm");

	/* We ask for a dump of alloc table from Cdb */
	/* --------------------------------------- */
	if (Cdb_dump_start(&(dbfd->Cdb_db),"stgcat_alloc") != 0) {
		goto _stgdb_load_error;
	}
	while (Cdb_dump(&(dbfd->Cdb_db),"stgcat_alloc",NULL,&alloc) == 0) {
		struct stgcat_entry thisstcp;
		if (Cdb2stcp(&thisstcp,NULL,NULL,NULL,NULL,&alloc) != 0) {
			continue;
		}
		if (stcp == *stcep) {
			struct stgcat_entry *newstcs;
			/* The calloced area is not enough */
			*stgcat_bufsz += BUFSIZ;
			if ((newstcs = (struct stgcat_entry *) realloc(*stcsp,*stgcat_bufsz)) == NULL) {
				Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_alloc");
				goto _stgdb_load_error;
			}
			*stcsp = newstcs;
			*stcep = *stcsp + (*stgcat_bufsz/sizeof(struct stgcat_entry));

			/* We makes sure that critical variable, stcp->reqid, is initalized */
			for (stcp = *stcsp + ((*stgcat_bufsz - BUFSIZ)/sizeof(struct stgcat_entry));
					 stcp < *stcep; stcp++) {
				stcp->reqid = 0;
			}

			/* We say where to continue */
			stcp = *stcsp + ((*stgcat_bufsz - BUFSIZ)/sizeof(struct stgcat_entry));

		}
		*stcp++ = thisstcp;
	}
	if (serrno != EDB_D_DUMPEND) {
		int save_serrno = serrno;
		Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_alloc");
		serrno = save_serrno;
		goto _stgdb_load_error;
    }
	Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_alloc");

	/* We ask for a dump of link table from Cdb */
	/* --------------------------------------- */
	if (Cdb_dump_start(&(dbfd->Cdb_db),"stgcat_link") != 0) {
		goto _stgdb_load_error;
	}
	while (Cdb_dump(&(dbfd->Cdb_db),"stgcat_link",NULL,&link) == 0) {
		struct stgpath_entry thisstpp;
		if (Cdb2stpp(&thisstpp,&link) != 0) {
			continue;
		}
		if (stpp == *stpep) {
			struct stgpath_entry *newstps;
			/* The calloced area is not enough */
			*stgpath_bufsz += BUFSIZ;
			if ((newstps = (struct stgpath_entry *) realloc(*stpsp,*stgpath_bufsz)) == NULL) {
				Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_link");
				goto _stgdb_load_error;
			}
			*stpsp = newstps;
			*stpep = *stpsp + (*stgpath_bufsz/sizeof(struct stgpath_entry));

			/* We makes sure that critical variable, stpp->reqid, is initalized */
			for (stpp = *stpsp + ((*stgpath_bufsz - BUFSIZ)/sizeof(struct stgpath_entry));
					 stpp < *stpep; stpp++) {
				stpp->reqid = 0;
			}

			/* We say where to continue */
			stpp = *stpsp + ((*stgpath_bufsz - BUFSIZ)/sizeof(struct stgpath_entry));

		}
		*stpp++ = thisstpp;
	}
	if (serrno != EDB_D_DUMPEND) {
		int save_serrno = serrno;
		Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_link");
		serrno = save_serrno;
		goto _stgdb_load_error;
    }
	Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_link");

	/*
		{
		struct stgcat_entry *dummy;

		for (dummy = *stcsp; dummy < stcp; dummy++) {
		stglogit("stgdb_load","Got (unsorted) stcp entry reqid %d\n",dummy->reqid);
		}
		}
		stglogit("sgdb_load","[1] stcp=0x%lx, *stcsp=0x%lx, calling qsort\n",(unsigned long) stcp, (unsigned long) *stcsp);
	*/

	/* We sort the stgcat entries per c_time, and per reqid as secondary key */
    /* Warning : arithmetic operation between stcp and *stcsp will return the number */
    /* of the same entities, so no need to divide by sizeof(struct stgcat_entry) */
	if (stcp > *stcsp) {
		qsort(*stcsp, (stcp - *stcsp), sizeof(struct stgcat_entry), &stgdb_stcpcmp);
	}

	/*
		{
		struct stgpath_entry *dummy;

		for (dummy = *stpsp; dummy < stpp; dummy++) {
		stglogit("stgdb_load","Got (unsorted) stpp entry reqid %d (%s)\n",dummy->reqid,dummy->upath);
		}
		}
		stglogit("sgdb_load","[1] stpp=0x%lx, *stpsp=0x%lx, calling qsort\n",(unsigned long) stpp, (unsigned long) *stpsp);
	*/

	/* We sort the stgpath entries per reqid */
    /* Warning : arithmetic operation between stpp and *stpsp will return the number */
    /* of the same entities, so no need to divide by sizeof(struct stgpath_entry) */
	if (stpp > *stpsp) {
		qsort(*stpsp, (stpp - *stpsp), sizeof(struct stgpath_entry), &stgdb_stppcmp);
	}

	return(0);

 _stgdb_load_error:
	if (*stcsp != NULL) {
		free(*stcsp);
	}
	if (*stpsp != NULL) {
		free(*stpsp);
	}
	return(-1);
#else
	return(0);
#endif
}

int stgdb_stcpcmp(p1,p2)
		 CONST void *p1;
		 CONST void *p2;
{
#ifdef USECDB
	struct stgcat_entry *stcp1 = (struct stgcat_entry *) p1;
	struct stgcat_entry *stcp2 = (struct stgcat_entry *) p2;

	if (stcp1->c_time < stcp2->c_time) {
		return(-1);
	} else if (stcp1->c_time > stcp2->c_time) {
		return(1);
	} else {
		if (stcp1->reqid < stcp2->reqid) {
			return(-1);
		} else if (stcp1->reqid > stcp2->reqid) {
			return(1);
		} else {
			/*
				stglogit("stgdb_stcpcmp",
				"### Warning[%s:%d] : two elements in stgcat have same c_time (%d) and reqid (%d)\n",
				__FILE__,__LINE__,(int) stcp1->c_time, (int) stcp1->reqid);
			*/
			return(0);
		}
	}
#else
	return(0);
#endif
}

int stgdb_stppcmp(p1,p2)
		 CONST void *p1;
		 CONST void *p2;
{
#ifdef USECDB
	struct stgpath_entry *stpp1 = (struct stgpath_entry *) p1;
	struct stgpath_entry *stpp2 = (struct stgpath_entry *) p2;

	if (stpp1->reqid < stpp2->reqid) {
		return(-1);
	} else if (stpp1->reqid > stpp2->reqid) {
		return(1);
	} else {
		/*
			stglogit("stgdb_stppcmp",
			"### Warning[%s:%d] : two elements (at 0x%lx and 0x%lx) in stgpath have same reqid (%d)\n",
			__FILE__,__LINE__,(unsigned long) stpp1,(unsigned long) stpp2,(int) stpp1->reqid);
			stglogit("stgdb_stppcmp",
			"### .......[%s:%d] : stpp1 (0x%lx) = [%d,%s] , stpp2 (0x%lx) = [%d,%s]\n"
			__FILE__,__LINE__,
			(unsigned long) stpp1,stpp1->reqid,stpp1->upath,
			(unsigned long) stpp2,stpp2->reqid,stpp2->upath);
		*/
		return(0);
	}
#else
	return(0);
#endif
}

int DLL_DECL Stgdb_upd_stgcat(dbfd,stcp,file,line)
		 stgdb_fd_t *dbfd;
		 struct stgcat_entry *stcp;
		 char *file;
		 int line;
{
#ifdef USECDB
	int find_status = -1;
	int update_status;
	Cdb_off_t Cdb_offset;
	struct stgcat_tape tape;
	struct stgcat_disk disk;
	struct stgcat_hpss hsm;
	struct stgcat_hsm castor;
	struct stgcat_alloc alloc;

	PING;

	/* stglogit(func, "In stgdb_upd_stgcat called at %s:%d\n",file,line); */

	if (stcp2Cdb(stcp,&tape,&disk,&hsm,&castor,&alloc) != 0) {
		stglogit("stgdb_upd_stgcat",
						 "### Warning[%s:%d] : stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d called at %s:%d\n",
						 __FILE__,__LINE__,stcp->reqid,file,line);
		return(-1);
	}

	switch (stcp->t_or_d) {
	case 't':
		find_status = Cdb_keyfind(&(dbfd->Cdb_db),
															"stgcat_tape","stgcat_tape_per_reqid","w",
															(void *) &tape,&Cdb_offset);
		break;
	case 'd':
		find_status = Cdb_keyfind(&(dbfd->Cdb_db),
															"stgcat_disk","stgcat_disk_per_reqid","w",
															(void *) &disk,&Cdb_offset);
		break;
	case 'm':
		find_status = Cdb_keyfind(&(dbfd->Cdb_db),
															"stgcat_hpss","stgcat_hpss_per_reqid","w",
															(void *) &hsm,&Cdb_offset);
		break;
	case 'h':
		find_status = Cdb_keyfind(&(dbfd->Cdb_db),
															"stgcat_hsm","stgcat_hsm_per_reqid","w",
															(void *) &castor,&Cdb_offset);
		break;
	case 'a':
		find_status = Cdb_keyfind(&(dbfd->Cdb_db),
															"stgcat_alloc","stgcat_alloc_per_reqid","w",
															(void *) &alloc,&Cdb_offset);
		break;
	default:
		stglogit("stgdb_upd_stgcat",
						 "### Warning[%s:%d] : unknown t_or_d element ('%c') for reqid %d called at %s:%d\n",
						 __FILE__,__LINE__,(char) stcp->t_or_d, (int) stcp->reqid,file,line);
		return(-1);
	}

	if (find_status != 0) {
		stglogit("stgdb_upd_stgcat",
						 "### Warning[%s:%d] : unknown record to update for reqid %d called at %s:%d (%s)\n",
						 __FILE__,__LINE__,(int) stcp->reqid,file,line,sstrerror(serrno));
		if (serrno == EDB_D_ENOENT) {
			stglogit("stgdb_upd_stgcat",
							 "### Warning[%s:%d] : Try to insert, instead of update, reqid %d called at %s:%d\n",
							 __FILE__,__LINE__,(int) stcp->reqid,file,line);
			return(stgdb_ins_stgcat(dbfd,stcp));
		}
		RETURN(-1);
	}

	switch (stcp->t_or_d) {
	case 't':
		update_status = Cdb_update(&(dbfd->Cdb_db),"stgcat_tape",
															 (void *) &tape,&Cdb_offset,&Cdb_offset);
		break;
	case 'd':
		update_status = Cdb_update(&(dbfd->Cdb_db),"stgcat_disk",
															 (void *) &disk,&Cdb_offset,&Cdb_offset);
		break;
	case 'm':
		update_status = Cdb_update(&(dbfd->Cdb_db),"stgcat_hpss",
															 (void *) &hsm,&Cdb_offset,&Cdb_offset);
		break;
	case 'h':
		update_status = Cdb_update(&(dbfd->Cdb_db),"stgcat_hsm",
															 (void *) &castor,&Cdb_offset,&Cdb_offset);
		break;
	case 'a':
		update_status = Cdb_update(&(dbfd->Cdb_db),"stgcat_alloc",
															 (void *) &alloc,&Cdb_offset,&Cdb_offset);
		break;
	}

	if (update_status != 0) {
		stglogit("stgdb_upd_stgcat",
						 "### Warning[%s:%d] : Cdb_update error for reqid %d called at %s:%d (%s)\n",
						 __FILE__,__LINE__,(int) stcp->reqid,file,line,sstrerror(serrno));
	}

	switch (stcp->t_or_d) {
	case 't':
		if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_tape",&Cdb_offset) != 0) {
			stglogit("stgdb_upd_stgcat",
							 "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d (%s)\n",
							 __FILE__,__LINE__,(int) stcp->reqid,file,line,sstrerror(serrno));
		}
		break;
	case 'd':
		if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_disk",&Cdb_offset) != 0) {
			stglogit("stgdb_upd_stgcat",
							 "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d (%s)\n",
							 __FILE__,__LINE__,(int) stcp->reqid,file,line,sstrerror(serrno));
		}
		break;
	case 'm':
		if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_hpss",&Cdb_offset) != 0) {
			stglogit("stgdb_upd_stgcat",
							 "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d (%s)\n",
							 __FILE__,__LINE__,(int) stcp->reqid,file,line,sstrerror(serrno));
		}
		break;
	case 'h':
		if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_hsm",&Cdb_offset) != 0) {
			stglogit("stgdb_upd_stgcat",
							 "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d (%s)\n",
							 __FILE__,__LINE__,(int) stcp->reqid,file,line,sstrerror(serrno));
		}
		break;
	case 'a':
		if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_alloc",&Cdb_offset) != 0) {
			stglogit("stgdb_upd_stgcat",
							 "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d (%s)\n",
							 __FILE__,__LINE__,(int) stcp->reqid,file,line,sstrerror(serrno));
		}
		break;
	}

	RETURN(update_status);
#else
	return(0);
#endif
}

int DLL_DECL Stgdb_upd_stgpath(dbfd,stpp,file,line)
		 stgdb_fd_t *dbfd;
		 struct stgpath_entry *stpp;
		 char *file;
		 int line;
{
#ifdef USECDB
	Cdb_off_t Cdb_offset;
	int update_status;
	struct stgcat_link link;

	PING;

	/* stglogit(func, "In stgdb_upd_stgpath called at %s:%d\n",file,line); */

	if (stpp2Cdb(stpp,&link) != 0) {
		stglogit("stgdb_upd_stgpath",
						 "### Warning[%s:%d] : stpp2Cdb (eg. stgpath -> Cdb on-the-fly) conversion error for reqid = %d called at %s:%d\n",
						 __FILE__,__LINE__,stpp->reqid,file,line);
	}

	if (Cdb_keyfind(&(dbfd->Cdb_db), "stgcat_link","stgcat_link_per_upath","w",
									(void *) &link,&Cdb_offset) != 0) {
		stglogit("stgdb_upd_stgpath",
						 "### Warning[%s:%d] : unknown record to update for reqid %d called at %s:%d (%s)\n",
						 __FILE__,__LINE__,(int) stpp->reqid,file,line,sstrerror(serrno));
		if (serrno == EDB_D_ENOENT) {
			stglogit("stgdb_upd_stgpath",
							 "### Warning[%s:%d] : Try to insert, instead of update, reqid %d called at %s:%d\n",
							 __FILE__,__LINE__,(int) stpp->reqid,file,line);
			return(stgdb_ins_stgpath(dbfd,stpp));
		}
		RETURN(-1);
	}

	if ((update_status = Cdb_update(&(dbfd->Cdb_db), "stgcat_link",
									(void *) &link,&Cdb_offset,&Cdb_offset)) != 0) {
		stglogit("stgdb_upd_stgpath",
						 "### Warning[%s:%d] : Cdb_update error for reqid %d called at %s:%d (%s)\n",
						 __FILE__,__LINE__,(int) stpp->reqid,file,line,sstrerror(serrno));
	}

	if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_link",&Cdb_offset) != 0) {
		stglogit("stgdb_upd_stgpath",
						 "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d (%s)\n",
						 __FILE__,__LINE__,(int) stpp->reqid,file,line,sstrerror(serrno));
	}

	RETURN(update_status);
#else
	return(0);
#endif
}

int DLL_DECL Stgdb_del_stgcat(dbfd,stcp,file,line)
		 stgdb_fd_t *dbfd;
		 struct stgcat_entry *stcp;
		 char *file;
		 int line;
{
#ifdef USECDB
	int find_status;
	int delete_status;
	Cdb_off_t Cdb_offset;
	struct stgcat_tape tape;
	struct stgcat_disk disk;
	struct stgcat_hpss hsm;
	struct stgcat_hsm castor;
	struct stgcat_alloc alloc;

	PING;

	/* stglogit(func, "In stgdb_del_stgcat called at %s:%d\n",file,line); */

	if (stcp2Cdb(stcp,&tape,&disk,&hsm,&castor,&alloc) != 0) {
		stglogit("stgdb_del_stgcat",
				 "### Warning[%s:%d] : stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d called at %s:%d\n",
						 __FILE__,__LINE__,stcp->reqid,file,line);
		return(-1);
	}

	switch (stcp->t_or_d) {
	case 't':
		find_status = Cdb_keyfind(&(dbfd->Cdb_db),
															"stgcat_tape","stgcat_tape_per_reqid","w",
															(void *) &tape,&Cdb_offset);
		break;
	case 'd':
		find_status = Cdb_keyfind(&(dbfd->Cdb_db),
															"stgcat_disk","stgcat_disk_per_reqid","w",
															(void *) &disk,&Cdb_offset);
		break;
	case 'm':
		find_status = Cdb_keyfind(&(dbfd->Cdb_db),
															"stgcat_hpss","stgcat_hpss_per_reqid","w",
															(void *) &hsm,&Cdb_offset);
		break;
	case 'h':
		find_status = Cdb_keyfind(&(dbfd->Cdb_db),
															"stgcat_hsm","stgcat_hsm_per_reqid","w",
															(void *) &castor,&Cdb_offset);
		break;
	case 'a':
		find_status = Cdb_keyfind(&(dbfd->Cdb_db),
															"stgcat_alloc","stgcat_alloc_per_reqid","w",
															(void *) &alloc,&Cdb_offset);
		break;
	default:
		stglogit("stgdb_del_stgcat",
						 "### Warning[%s:%d] : unknown t_or_d element ('%c') for reqid %d called at %s:%d\n",
						 __FILE__,__LINE__,(char) stcp->t_or_d, (int) stcp->reqid,file,line);
		return(-1);
	}

	if (find_status != 0) {
		stglogit("stgdb_del_stgcat",
						 "### Warning[%s:%d] : unknown record to delete for reqid %d called at %s:%d (%s)\n",
						 __FILE__,__LINE__,(int) stcp->reqid,file,line,sstrerror(serrno));
		RETURN(-1);
	}

	switch (stcp->t_or_d) {
	case 't':
		delete_status = Cdb_delete(&(dbfd->Cdb_db),"stgcat_tape",&Cdb_offset);
		break;
	case 'd':
		delete_status = Cdb_delete(&(dbfd->Cdb_db),"stgcat_disk",&Cdb_offset);
		break;
	case 'm':
		delete_status = Cdb_delete(&(dbfd->Cdb_db),"stgcat_hpss",&Cdb_offset);
		break;
	case 'h':
		delete_status = Cdb_delete(&(dbfd->Cdb_db),"stgcat_hsm",&Cdb_offset);
		break;
	case 'a':
		delete_status = Cdb_delete(&(dbfd->Cdb_db),"stgcat_alloc",&Cdb_offset);
		break;
	}

	if (delete_status != 0) {
		stglogit("stgdb_del_stgcat",
						 "### Warning[%s:%d] : Cdb_delete error for reqid %d called at %s:%d (%s)\n",
						 __FILE__,__LINE__,(int) stcp->reqid,file,line,sstrerror(serrno));
	}

	switch (stcp->t_or_d) {
	case 't':
		if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_tape",&Cdb_offset) != 0) {
			stglogit("stgdb_del_stgcat",
							 "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d (%s)\n",
							 __FILE__,__LINE__,(int) stcp->reqid,file,line,sstrerror(serrno));
		}
		break;
	case 'd':
		if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_disk",&Cdb_offset) != 0) {
			stglogit("stgdb_del_stgcat",
							 "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d (%s)\n",
							 __FILE__,__LINE__,(int) stcp->reqid,file,line,sstrerror(serrno));
		}
		break;
	case 'm':
		if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_hpss",&Cdb_offset) != 0) {
			stglogit("stgdb_del_stgcat",
							 "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d (%s)\n",
							 __FILE__,__LINE__,(int) stcp->reqid,file,line,sstrerror(serrno));
		}
		break;
	case 'h':
		if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_hsm",&Cdb_offset) != 0) {
			stglogit("stgdb_del_stgcat",
							 "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d (%s)\n",
							 __FILE__,__LINE__,(int) stcp->reqid,file,line,sstrerror(serrno));
		}
		break;
	case 'a':
		if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_alloc",&Cdb_offset) != 0) {
			stglogit("stgdb_del_stgcat",
							 "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d (%s)\n",
							 __FILE__,__LINE__,(int) stcp->reqid,file,line,sstrerror(serrno));
		}
		break;
	}

	RETURN(delete_status);
#else
	return(0);
#endif
}

int DLL_DECL Stgdb_del_stgpath(dbfd,stpp,file,line)
		 stgdb_fd_t *dbfd;
		 struct stgpath_entry *stpp;
		 char *file;
		 int line;
{
#ifdef USECDB
	Cdb_off_t Cdb_offset;
	int delete_status;
	struct stgcat_link link;

	PING;

	/* stglogit(func, "In stgdb_del_stgpath called at %s:%d\n",file,line); */

	if (stpp2Cdb(stpp,&link) != 0) {
		stglogit("stgdb_del_stgpath",
						 "### Warning[%s:%d] : stpp2Cdb (eg. stgpath -> Cdb on-the-fly) conversion error for reqid = %d called at %s:%d\n",
						 __FILE__,__LINE__,stpp->reqid,file,line);
	}

	if (Cdb_keyfind(&(dbfd->Cdb_db), "stgcat_link","stgcat_link_per_upath","w",
									(void *) &link,&Cdb_offset) != 0) {
		stglogit("stgdb_del_stgpath",
						 "### Warning[%s:%d] : unknown record to delete for reqid %d called at %s:%d (%s)\n",
						 __FILE__,__LINE__,(int) stpp->reqid,file,line,sstrerror(serrno));
		RETURN(-1);
	}

	if ((delete_status = Cdb_delete(&(dbfd->Cdb_db),"stgcat_link",&Cdb_offset)) != 0) {
		stglogit("stgdb_del_stgpath",
						 "### Warning[%s:%d] : Cdb_delete error for reqid %d called at %s:%d (%s)\n",
						 __FILE__,__LINE__,(int) stpp->reqid,file,line,sstrerror(serrno));
	}

	if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_link",&Cdb_offset) != 0) {
		stglogit("stgdb_del_stgpath",
						 "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d (%s)\n",
						 __FILE__,__LINE__,(int) stpp->reqid,file,line,sstrerror(serrno));
	}

	RETURN(delete_status);
#else
	return(0);
#endif
}

int DLL_DECL Stgdb_ins_stgcat(dbfd,stcp,file,line)
		 stgdb_fd_t *dbfd;
		 struct stgcat_entry *stcp;
		 char *file;
		 int line;
{
#ifdef USECDB
	int insert_status;
	struct stgcat_tape tape;
	struct stgcat_disk disk;
	struct stgcat_hpss hsm;
	struct stgcat_hsm castor;
	struct stgcat_alloc alloc;

	PING;

	/* stglogit(func, "In stgdb_ins_stgcat called at %s:%d\n",file,line); */

	if (stcp2Cdb(stcp,&tape,&disk,&hsm,&castor,&alloc) != 0) {
		stglogit("stgdb_ins_stgcat",
						 "### Warning[%s:%d] : stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d called at %s:%d\n",
						 __FILE__,__LINE__,stcp->reqid,file,line);
		return(-1);
	}

	switch (stcp->t_or_d) {
	case 't':
		insert_status = Cdb_insert(&(dbfd->Cdb_db),"stgcat_tape",NULL,(void *) &tape,NULL);
		break;
	case 'd':
		insert_status = Cdb_insert(&(dbfd->Cdb_db),"stgcat_disk",NULL,(void *) &disk,NULL);
		break;
	case 'm':
		insert_status = Cdb_insert(&(dbfd->Cdb_db),"stgcat_hpss",NULL,(void *) &hsm,NULL);
		break;
	case 'h':
		insert_status = Cdb_insert(&(dbfd->Cdb_db),"stgcat_hsm",NULL,(void *) &castor,NULL);
		break;
	case 'a':
		insert_status = Cdb_insert(&(dbfd->Cdb_db),"stgcat_alloc",NULL,(void *) &alloc,NULL);
		break;
	default:
		stglogit("stgdb_ins_stgcat",
						 "### Warning[%s:%d] : unknown t_or_d element ('%c') for reqid %d called at %s:%d\n",
						 __FILE__,__LINE__,(char) stcp->t_or_d, (int) stcp->reqid,file,line);
		return(-1);
	}

	if (insert_status != 0) {
		stglogit("stgdb_ins_stgcat",
						 "### Warning[%s:%d] : cannot insert record for reqid %d called at %s:%d (%s)\n",
						 __FILE__,__LINE__,(int) stcp->reqid,file,line,sstrerror(serrno));
		RETURN(-1);
	}
#endif
	return(0);
}

int DLL_DECL Stgdb_ins_stgpath(dbfd,stpp,file,line)
		 stgdb_fd_t *dbfd;
		 struct stgpath_entry *stpp;
		 char *file;
		 int line;
{
#ifdef USECDB
	int insert_status;
	struct stgcat_link link;

	PING;

	if (stpp2Cdb(stpp,&link) != 0) {
		stglogit("stgdb_ins_stgpath",
						 "### Warning[%s:%d] : stpp2Cdb (eg. stgpath -> Cdb on-the-fly) conversion error for reqid = %d called at %s:%d\n",
						 __FILE__,__LINE__,stpp->reqid,file,line);
	}
	
	insert_status = Cdb_insert(&(dbfd->Cdb_db),"stgcat_link",NULL,(void *) &link,NULL);
	
	if (insert_status != 0) {
		stglogit("stgdb_ins_stgpath",
						 "### Warning[%s:%d] : cannot insert record for reqid %d called at %s:%d (%s)\n",
						 __FILE__,__LINE__,(int) stpp->reqid,file,line,sstrerror(serrno));
		RETURN(-1);
	}
	return(0);
#else
	return(0);
#endif
}

int DLL_DECL Stgdb_uniqueid(dbfd,uniqueid,file,line)
		 stgdb_fd_t *dbfd;
		 u_signed64 *uniqueid;
		 char *file;
		 int line;
{
#ifdef USECDB
	/* The trick is to always a generated uniqueid from the SAME table */
	/* so that we guarantee its uniqueness. */
	return(Cdb_uniqueid(&(dbfd->Cdb_db),
						"stgcat_link",
						0,                         /* Start value */
						1,                         /* Step value */
						uniqueid));
#else
	*uniqueid = ++local_uniqueid;
	return(0);
#endif
}

