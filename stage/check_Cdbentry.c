/*
	$Id: check_Cdbentry.c,v 1.3 2000/03/23 17:14:58 jdurand Exp $
*/

#include "Cstage_db.h"
#include "stage.h"

/* ============== */
/* System headers */
/* ============== */
#ifndef _WIN32
#include <unistd.h>
#endif
#include <sys/types.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>

/* =============================================================== */
/* Local headers for threads : to be included before ANYTHING else */
/* =============================================================== */
#include "osdep.h"
#include "Cdb_api.h"                /* CASTOR Cdb Interface */
#include "Cstage_db.h"              /* Generated STAGE/Cdb header */
#include "stage.h"                  /* CASTOR's STAGE header */
#include "serrno.h"                 /* CASTOR's serrno */
#include "Cstage_ifce.h"            /* Conversion routines */
#include "u64subr.h"

/* ====== */
/* Macros */
/* ====== */
#define CDB_USERNAME "Cdb_Stage_User"
#define CDB_PASSWORD "Cdb_Stage_Password"

#ifdef __STDC__
#define NAMEOFVAR(x) #x
#else
#define NAMEOFVAR(x) "x"
#endif

#define DUMP_VAL(st,member) {					\
	printf("%10s : %10d\n", NAMEOFVAR(member) ,	\
				 (int) st->member);				\
}

#define DUMP_CHAR(st,member) {										\
	printf("%10s : %10c\n", NAMEOFVAR(member) ,						\
				 st->member != '\0' ? st->member : ' ');			\
}

#define DUMP_STRING(st,member) {				\
	printf("%10s : %10s\n", NAMEOFVAR(member) ,	\
				 st->member);					\
}


/* =================== */
/* Internal prototypes */
/* =================== */
void check_Cdbentry_usage _PROTO(());
void stppprint _PROTO((struct stgpath_entry *));
void stcpprint _PROTO((struct stgcat_entry *));

/* ================ */
/* Static variables */
/* ================ */
int reqid;

int main(argc,argv)
		 int argc;
		 char **argv;
{
	/* For options */
	extern char *optarg;
	extern int optind, opterr, optopt;
	int errflg = 0;
	int c;
	int help = 0;
	char *Cdb_username = NULL;
	char *Cdb_password = NULL;
	Cdb_db_t Cdb_db;
	Cdb_sess_t Cdb_session;
	int Cdb_session_opened = 0;
	int Cdb_db_opened = 0;
	char *db[] = {
		"stgcat_tape",
		"stgcat_disk",
		"stgcat_hsm",
		"stgcat_alloc",
		"stgcat_link",
	};
	char *key[] = {
		"stgcat_tape_per_reqid",
		"stgcat_disk_per_reqid",
		"stgcat_hsm_per_reqid",
		"stgcat_alloc_per_reqid",
		"stgcat_link_per_reqid",
	};
	int ndb = 5;
	int idb;
	struct stgcat_tape tape;
	struct stgcat_disk disk;
	struct stgcat_hsm hsm;
	struct stgcat_alloc alloc;
	struct stgcat_link link;
	struct stgcat_entry stcp;
	struct stgpath_entry stpp;
	Cdb_off_t found_offset;
	char tmpbuf[21];
	char *error = NULL;
	int rc;
	int iargv;

	while ((c = getopt(argc,argv,"hu:p:")) != EOF) {
		switch (c) {
		case 'h':
			help = 1;
			break;
		case 'u':
			Cdb_username = optarg;
			break;
		case 'p':
			Cdb_password = optarg;
			break;
		case '?':
			++errflg;
			printf("Unknown option\n");
			break;
		default:
			++errflg;
			printf("?? getopt returned character code 0%o (octal) 0x%lx (hex) %d (int) '%c' (char) ?\n"
						 ,c,(unsigned long) c,c,(char) c);
			break;
		}
	}

	if (errflg != 0) {
		printf("### getopt error\n");
		check_Cdbentry_usage();
		return(EXIT_FAILURE);
	}

	if (help != 0) {
		check_Cdbentry_usage();
		return(EXIT_SUCCESS);
	}

	if (optind >= argc) {
		printf("?? getopt parsing error\n");
		check_Cdbentry_usage();
		return(EXIT_FAILURE);
	}

	/* Check the arguments */
	for (iargv = optind; iargv < argc; iargv++) {
		if ((reqid = atoi(argv[iargv])) <= 0) {
			printf("### one of the argument is <= 0\n");
			check_Cdbentry_usage();
			return(EXIT_FAILURE);
		}
	}

	/* We open a connection to Cdb */
	if (Cdb_username == NULL) {
		Cdb_username = CDB_USERNAME;
	}
	if (Cdb_password == NULL) {
		Cdb_password = CDB_PASSWORD;
	}

	if (Cdb_login(Cdb_username,Cdb_password,&Cdb_session) != 0) {
		printf("### Cdb_login(\"%s\",\"%s\",&Cdb_session) error, %s - %s\n"
					 ,Cdb_username
					 ,Cdb_password
					 ,sstrerror(serrno)
					 ,strerror(errno));
		if (Cdb_api_error(&Cdb_session,&error) == 0) {
			printf("--> more info:\n%s",error);
		}
		rc = EXIT_FAILURE;
		goto check_Cdbentry_return;
	}

	Cdb_session_opened = -1;

	/* We open the database */
	if (Cdb_open(&Cdb_session,"stage",&Cdb_stage_interface,&Cdb_db) != 0) {
		printf("### Cdb_open(&Cdb_session,\"stage\",&Cdb_stage_interface,&Cdb_db) error, %s\n"
					 ,sstrerror(serrno));
		if (Cdb_api_error(&Cdb_session,&error) == 0) {
			printf("--> more info:\n%s",error);
		}
		printf("### PLEASE SUBMIT THE STAGER DATABASE WITH THE TOOL Cdb_create IF NOT ALREADY DONE.\n");
		rc = EXIT_FAILURE;
		goto check_Cdbentry_return;
	}

	Cdb_db_opened = -1;

	/* Loop on the arguments */
	for (iargv = optind; iargv < argc; iargv++) {
		int global_find_status = 0;

		reqid = atoi(argv[iargv]);

		/* Search for the corresponding record in all databases */
		for (idb = 0; idb < ndb; idb++) {
			int find_status;
			
			switch (idb) {
			case 0:
				memset(&tape,0,sizeof(struct stgcat_tape));
				tape.reqid = reqid;
				if ((find_status = Cdb_keyfind_fetch(&Cdb_db,db[idb],key[idb],NULL,&tape,&found_offset,&tape)) == 0) {
					Cdb2stcp(&stcp,&tape,NULL,NULL,NULL);
					++global_find_status;
					printf("--> Reqid %d found at offset %s in database \"%s\":\n",
								 reqid, u64tostr((u_signed64) found_offset, tmpbuf, 0),
								 db[idb]);
					stcpprint(&stcp);
				}
				break;
			case 1:
				memset(&disk,0,sizeof(struct stgcat_disk));
				disk.reqid = reqid;
				if ((find_status = Cdb_keyfind_fetch(&Cdb_db,db[idb],key[idb],NULL,&disk,&found_offset,&disk)) == 0) {
					Cdb2stcp(&stcp,NULL,&disk,NULL,NULL);
					++global_find_status;
					printf("--> Reqid %d found at offset %s in database \"%s\":\n",
								 reqid, u64tostr((u_signed64) found_offset, tmpbuf, 0),
								 db[idb]);
					stcpprint(&stcp);
				}
				break;
			case 2:
				memset(&hsm,0,sizeof(struct stgcat_hsm));
				hsm.reqid = reqid;
				if ((find_status = Cdb_keyfind_fetch(&Cdb_db,db[idb],key[idb],NULL,&hsm,&found_offset,&hsm)) == 0) {
					Cdb2stcp(&stcp,NULL,NULL,&hsm,NULL);
					++global_find_status;
					printf("--> Reqid %d found at offset %s in database \"%s\":\n",
								 reqid, u64tostr((u_signed64) found_offset, tmpbuf, 0),
								 db[idb]);
					stcpprint(&stcp);
				}
				break;
			case 3:
				memset(&alloc,0,sizeof(struct stgcat_alloc));
				alloc.reqid = reqid;
				if ((find_status = Cdb_keyfind_fetch(&Cdb_db,db[idb],key[idb],NULL,&alloc,&found_offset,&alloc)) == 0) {
					Cdb2stcp(&stcp,NULL,NULL,NULL,&alloc);
					++global_find_status;
					printf("--> Reqid %d found at offset %s in database \"%s\":\n",
								 reqid, u64tostr((u_signed64) found_offset, tmpbuf, 0),
								 db[idb]);
					stcpprint(&stcp);
				}
				break;
			case 4:
				memset(&link,0,sizeof(struct stgcat_link));
				link.reqid = reqid;
				if ((find_status = Cdb_pkeyfind_fetch(&Cdb_db,db[idb],key[idb],1,NULL,&link,&found_offset,&link)) == 0) {
					Cdb2stpp(&stpp,&link);
					++global_find_status;
					printf("--> Reqid %d found at offset %s in database \"%s\":\n",
								 reqid, u64tostr((u_signed64) found_offset, tmpbuf, 0),
								 db[idb]);
					stppprint(&stpp);
					while (Cdb_pkeynext_fetch(&Cdb_db,db[idb],key[idb],1,NULL,&found_offset,&link) == 0) {
						Cdb2stpp(&stpp,&link);
						++global_find_status;
						printf("--> Reqid %d found again at offset %s in database \"%s\":\n",
									 reqid, u64tostr((u_signed64) found_offset, tmpbuf, 0),
									 db[idb]);
						stppprint(&stpp);
					}
				}
				break;
			default:
				printf("### Internal error\n");
				rc = EXIT_FAILURE;
				goto check_Cdbentry_return;
			}
		}
		if (global_find_status == 0) {
			printf("--> Reqid %d not found\n",reqid);
		} else {
			printf("==> Reqid %d found %d time%s\n",
						 reqid,
						 global_find_status,
						 global_find_status > 1 ? "s" : ""
						 );
		}
	}


 check_Cdbentry_return:
	if (Cdb_db_opened != 0) {
		Cdb_close(&Cdb_db);
	}
	if (Cdb_session_opened != 0) {
		Cdb_logout(&Cdb_session);
	}
	return(rc);
}

void check_Cdbentry_usage() {
	printf(
				 "\n"
				 "Usage : check_Cdbentry [-h] [-u username] [-p password] reqid [reqid]\n"
				 "\n"
				 "  This program will load entries with reqid (>0) from Cdb and print in human format\n"
				 "\n"
				 "  -h                Print this help and exit\n"
				 "  -u                Cdb username\n"
				 "  -p                Cdb password\n"
				 "\n");
}

void stppprint(stpp)
		 struct stgpath_entry *stpp;
{
	if (stpp == NULL) {
		return;
	}

	printf("-----------------------\n");
	printf("%10s : %10s\n","What","Value");
	printf("-----------------------\n");
	DUMP_VAL(stpp,reqid);
	DUMP_STRING(stpp,upath);
}

void stcpprint(stcp)
		 struct stgcat_entry *stcp;
{
	int i;

	if (stcp == NULL) {
		return;
	}

	printf("-----------------------\n");
	printf("%10s : %10s\n","What","Value");
	printf("-----------------------\n");
	DUMP_VAL(stcp,blksize);
	DUMP_STRING(stcp,filler);
	DUMP_CHAR(stcp,charconv);
	DUMP_CHAR(stcp,keep);
	DUMP_VAL(stcp,lrecl);
	DUMP_VAL(stcp,nread);
	DUMP_STRING(stcp,poolname);
	DUMP_STRING(stcp,recfm);
	DUMP_VAL(stcp,size);
	DUMP_STRING(stcp,ipath);
	DUMP_CHAR(stcp,t_or_d);
	DUMP_STRING(stcp,group);
	DUMP_STRING(stcp,user);
	DUMP_VAL(stcp,uid);
	DUMP_VAL(stcp,gid);
	DUMP_VAL(stcp,mask);
	DUMP_VAL(stcp,reqid);
	DUMP_VAL(stcp,status);
	DUMP_VAL(stcp,actual_size);
	DUMP_VAL(stcp,c_time);
	DUMP_VAL(stcp,a_time);
	DUMP_VAL(stcp,nbaccesses);
	switch (stcp->t_or_d) {
	case 't':
		DUMP_STRING(stcp,u1.t.den);
		DUMP_STRING(stcp,u1.t.dgn);
		DUMP_STRING(stcp,u1.t.fid);
		DUMP_CHAR(stcp,u1.t.filstat);
		DUMP_STRING(stcp,u1.t.fseq);
		DUMP_STRING(stcp,u1.t.lbl);
		DUMP_VAL(stcp,u1.t.retentd);
		DUMP_STRING(stcp,u1.t.tapesrvr);
		DUMP_CHAR(stcp,u1.t.E_Tflags);
		for (i = 0; i < MAXVSN; i++) {
			DUMP_STRING(stcp,u1.t.vid[i]);
			DUMP_STRING(stcp,u1.t.vsn[i]);
		}
		break;
	case 'd':
	case 'a':
		DUMP_STRING(stcp,u1.d.xfile);
		DUMP_STRING(stcp,u1.d.Xparm);
		break;
	case 'm':
		DUMP_STRING(stcp,u1.m.xfile);
		break;
	}
}



