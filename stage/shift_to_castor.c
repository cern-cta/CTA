/*
 * $Id: shift_to_castor.c,v 1.11 2001/11/30 12:04:42 jdurand Exp $
 */

/* ============== */
/* System headers */
/* ============== */
#include <stdio.h>            /* All I/O defs */
#include <sys/stat.h>         /* fstat etc... */
#include <sys/types.h>        /* For all types  */
#include <fcntl.h>            /* RDWR etc... */
#include <unistd.h>           /* Getopt etc... */
#include <stdlib.h>           /* atoi etc... */
#include <errno.h>            /* errno etc... */
#include <string.h>           /* memset etc... */

/* ============= */
/* Local headers */
/* ============= */
#include "osdep.h"            /* For _PROTO */
#include "stage_api.h"
#include "stage_shift.h"
#include "u64subr.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "serrno.h"

/* ====== */
/* Macros */
/* ====== */
#ifdef _WIN32
#define FILE_OFLAG ( O_RDONLY | O_BINARY )
#else
#define FILE_OFLAG O_RDONLY
#endif
#ifdef _WIN32
#define FILE_MODE ( _S_IREAD | _S_IWRITE )
#else
#define FILE_MODE ( S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH )
#endif
#define FREQUENCY 1000

#ifdef __STDC__
#define NAMEOFVAR(x) #x
#else
#define NAMEOFVAR(x) "x"
#endif

#define CHECK_RAW_SIZE(member) {												\
	if (sizeof(stcp_castor.member) < sizeof(stcp_old.member)) {					\
		int this_answer;														\
		printf("### Warning : data may be lost in %s at reqid = %d\n"			\
					 "... (Castor's sizeof (%d) < Shift's sizeof (%d))\n"		\
					 "... Do you really wish to convert this entry ? (y/n)\n",	\
					 NAMEOFVAR(member),stcp_old.reqid,							\
					 sizeof(stcp_castor.member),sizeof(stcp_old.member));		\
		this_answer = getchar();												\
		fflush(stdin);															\
		if (this_answer != 'y' && this_answer != 'Y') {							\
			continue;															\
		}																		\
	}																			\
}

#define CHECK_VAR_SIZE(member) CHECK_RAW_SIZE(member)

#define CHECK_STRING_SIZE(member) {												\
	if (sizeof(stcp_castor.member) <  (strlen(stcp_old.member) + 1)) {			\
		int this_answer;														\
		printf("### Warning : data may be lost in %s at reqid = %d\n"			\
					 "... (Castor's sizeof (%d) < Shift's tot length (%d))\n"	\
					 "... Do you wish to convert this entry ? (y/n)\n",			\
						NAMEOFVAR(member),stcp_old.reqid,						\
					 sizeof(stcp_castor.member),strlen(stcp_old.member) + 1);	\
		this_answer = getchar();												\
		fflush(stdin);															\
		if (this_answer != 'y' && this_answer != 'Y') {							\
			continue;															\
		}																		\
	}																			\
}

/* =============== */
/* Local variables */
/* =============== */
char *stgcat_in, *stgpath_in, *stgcat_out, *stgpath_out;
int stgcat_in_fd, stgpath_in_fd, stgcat_out_fd, stgpath_out_fd;
struct stat stgcat_in_statbuff, stgpath_in_statbuff, stgcat_out_statbuff, stgpath_out_statbuff;
int frequency = FREQUENCY;

/* ========== */
/* Prototypes */
/* ========== */
void shift_to_castor_usage _PROTO(());
int convert_stgcat _PROTO(());
int convert_stgpath _PROTO(());

int main(argc, argv)
		 int argc;
		 char **argv;
{
	int c;
	int help = 0;
	int skip_stgcat = 0;
	int skip_stgpath = 0;
	int errflg = 0;
	char tmpbuf[21];
	int answer;

	Coptind = 1;
	Copterr = 1;
	while ((c = Cgetopt(argc,argv,"hCLn:")) != -1) {
		switch (c) {
		case 'h':
			help = 1;
			break;
		case 'C':
			skip_stgcat = 1;
			break;
		case 'L':
			skip_stgpath = 1;
			break;
		case 'n':
			frequency = atoi(Coptarg);
			break;
		case '?':
			++errflg;
			printf("Unknown option\n");
			break;
		default:
			++errflg;
			printf("?? Cgetopt returned character code 0%o (octal) 0x%lx (hex) %d (int) '%c' (char) ?\n"
						 ,c,(unsigned long) c,c,(char) c);
			break;
		}
	}

	if (errflg != 0) {
		printf("### Cgetopt error\n");
		shift_to_castor_usage();
		return(EXIT_FAILURE);
	}

	if (help != 0) {
		shift_to_castor_usage();
		return(EXIT_SUCCESS);
	}

	if (Coptind >= argc || Coptind > (argc - 4)) {
		printf("?? Exactly four parameters are requested\n");
		shift_to_castor_usage();
		return(EXIT_FAILURE);
	}

	stgcat_in = argv[Coptind];
	stgpath_in = argv[Coptind+1];
	stgcat_out = argv[Coptind+2];
	stgpath_out = argv[Coptind+3];

	if (skip_stgcat == 0) {
		if ((stgcat_in_fd = open(stgcat_in, FILE_OFLAG, FILE_MODE)) < 0) {
			printf("### open of %s error, %s\n",stgcat_in,strerror(errno));
			return(EXIT_FAILURE);      
		}
		if ((stgcat_out_fd = open(stgcat_out, FILE_OFLAG | O_RDWR | O_CREAT, FILE_MODE)) < 0) {
			printf("### open of %s error, %s\n",stgcat_out,strerror(errno));
			return(EXIT_FAILURE);      
		}
		/* Check the sizes */
		if (fstat(stgcat_in_fd, &stgcat_in_statbuff) < 0) {
			printf("### fstat on %s error, %s\n"
						 ,stgcat_in
						 ,strerror(errno));
			return(EXIT_FAILURE);
		}
		/* - stgcat_in has to be length > 0 */
		if (stgcat_in_statbuff.st_size <= 0) {
			printf("### %s is of zero size !\n"
						 ,stgcat_in);
			return(EXIT_FAILURE);
		}
		if (fstat(stgcat_out_fd, &stgcat_out_statbuff) < 0) {
			printf("### fstat on %s error, %s\n"
						 ,stgcat_out
						 ,strerror(errno));
			return(EXIT_FAILURE);
		}
		/* - stgcat_out has to be length <= 0 (asking to proceed anyway otherwise) */
		if (stgcat_out_statbuff.st_size > 0) {
			printf("### Current %s length : %s\n"
						 ,stgcat_out,
						 u64tostr((u_signed64) stgcat_out_statbuff.st_size, tmpbuf, 0));
			printf("### Do you really want to proceed (y/n) ? ");
			answer = getchar();
			fflush(stdin);
			if (answer != 'y' && answer != 'Y') {
				return(EXIT_FAILURE);
			}
			if (ftruncate(stgcat_out_fd, (size_t) 0) != 0) {
				printf("### ftruncate on %s error, %s\n"
							 ,stgcat_out
							 ,strerror(errno));
				return(EXIT_FAILURE);
			}
		}
	}
	if (skip_stgpath == 0) {
		if ((stgpath_in_fd = open(stgpath_in, FILE_OFLAG, FILE_MODE)) < 0) {
			printf("### open of %s error, %s\n",stgpath_in,strerror(errno));
			return(EXIT_FAILURE);      
		}
		if ((stgpath_out_fd = open(stgpath_out, FILE_OFLAG | O_RDWR | O_CREAT, FILE_MODE)) < 0) {
			printf("### open of %s error, %s\n",stgpath_out,strerror(errno));
			return(EXIT_FAILURE);      
		}
		/* Check the sizes */
		if (fstat(stgpath_in_fd, &stgpath_in_statbuff) < 0) {
			printf("### fstat on %s error, %s\n"
						 ,stgpath_in
						 ,strerror(errno));
			return(EXIT_FAILURE);
		}
		/* - stgpath_in has to be length > 0 */
		if (stgpath_in_statbuff.st_size <= 0) {
			printf("### %s is of zero size !\n"
						 ,stgpath_in);
			return(EXIT_FAILURE);
		}
		if (fstat(stgpath_out_fd, &stgpath_out_statbuff) < 0) {
			printf("### fstat on %s error, %s\n"
						 ,stgpath_out
						 ,strerror(errno));
			return(EXIT_FAILURE);
		}
		/* - stgpath_out has to be length <= 0 (asking to proceed anyway otherwise) */
		if (stgpath_out_statbuff.st_size > 0) {
			printf("### Current %s length : %s\n"
						 ,stgpath_out,
						 u64tostr((u_signed64) stgpath_out_statbuff.st_size, tmpbuf, 0));
			printf("### Do you really want to proceed (y/n) ? ");
			answer = getchar();
			fflush(stdin);
			if (answer != 'y' && answer != 'Y') {
				return(EXIT_FAILURE);
			}
			if (ftruncate(stgpath_out_fd, (size_t) 0) != 0) {
				printf("### ftruncate on %s error, %s\n"
							 ,stgpath_out
							 ,strerror(errno));
				return(EXIT_FAILURE);
			}
		}
	}

	if (skip_stgcat == 0) {
		printf("\n*** Conversion SHIFT's %s to CASTOR's %s\n\n",stgcat_in,stgcat_out);
		if (convert_stgcat() != 0) {
			return(EXIT_FAILURE);
		}
		close(stgcat_in_fd);
		close(stgcat_out_fd);
	}
	if (skip_stgpath == 0) {
		printf("\n*** Conversion SHIFT's %s to CASTOR's %s\n\n",stgpath_in,stgpath_out);
		if (convert_stgpath() != 0) {
			return(EXIT_FAILURE);
		}
		close(stgpath_in_fd);
		close(stgpath_out_fd);
	}

	return(EXIT_SUCCESS);
}

int convert_stgcat() {
	int answer;
	struct stgcat_entry_old stcp_old;
	struct stgcat_entry     stcp_castor;
	int i;
	int imax;
	int j;

	/* We check in the size of the in-file module SHIFT stager structure matches... */
	if (stgcat_in_statbuff.st_size % sizeof(struct stgcat_entry_old) != 0) {
		printf("### Current %s length %% sizeof(old SHIFT structure) is NOT zero (%d)...\n"
					 ".... I believe that either this file is truncated or currupted, either\n"
					 ".... you did not gave a read old SHIFT stager stgcat file as input\n"
					 ,stgcat_in,
					 (int) (stgcat_in_statbuff.st_size % sizeof(struct stgcat_entry_old)));
		printf("### Do you really want to proceed (y/n) ? ");
		answer = getchar();
		fflush(stdin);
		if (answer != 'y' && answer != 'Y') {
			return(-1);
		}
	}

	/* We convert all the entries */
	imax = stgcat_in_statbuff.st_size / sizeof(struct stgcat_entry_old) - 1;
	for (i = 0; i <= imax; i++) {
		int status;

		status = 0;

		if (read(stgcat_in_fd, &stcp_old, sizeof(struct stgcat_entry_old)) != 
				sizeof(struct stgcat_entry_old)) {
			printf("### read error on %s (%s) at %s:%d\n",stgcat_in,
						 strerror(errno),__FILE__,__LINE__);
			return(-1);
		}
		/* Convert the SHIFT entry to a CASTOR one */
		if (i == 0 || i % frequency == 0 || i == imax) {
			printf("--> (%5d/%5d) Doing reqid = %d\n",i+1,imax+1,stcp_old.reqid);
		}
		memset((char *) &stcp_castor, 0, sizeof(struct stgcat_entry));
		CHECK_VAR_SIZE(blksize);
		stcp_castor.blksize        = stcp_old.blksize;
		CHECK_RAW_SIZE(filler);
		memcpy(stcp_castor.filler  , stcp_old.filler,sizeof(stcp_castor.filler));
		CHECK_RAW_SIZE(charconv);
		stcp_castor.charconv       = stcp_old.charconv;
		CHECK_RAW_SIZE(keep);
		stcp_castor.keep           = stcp_old.keep;
		CHECK_VAR_SIZE(lrecl);
		stcp_castor.lrecl          = stcp_old.lrecl;
		CHECK_VAR_SIZE(nread);
		stcp_castor.nread          = stcp_old.nread;
		CHECK_STRING_SIZE(poolname);
		strcpy(stcp_castor.poolname, stcp_old.poolname);
		CHECK_STRING_SIZE(recfm);
		strcpy(stcp_castor.recfm,    stcp_old.recfm);
		CHECK_VAR_SIZE(size);
		stcp_castor.size           = stcp_old.size;
		CHECK_STRING_SIZE(ipath);
		strcpy(stcp_castor.ipath,    stcp_old.ipath);
		CHECK_RAW_SIZE(t_or_d);
		stcp_castor.t_or_d         = stcp_old.t_or_d;
		CHECK_STRING_SIZE(group);
		strcpy(stcp_castor.group,    stcp_old.group);
		CHECK_STRING_SIZE(user);
		strcpy(stcp_castor.user,     stcp_old.user);
		CHECK_VAR_SIZE(uid);
		stcp_castor.uid            = stcp_old.uid;
		CHECK_VAR_SIZE(gid);
		stcp_castor.gid            = stcp_old.gid;
		CHECK_VAR_SIZE(mask);
		stcp_castor.mask           = stcp_old.mask;
		CHECK_VAR_SIZE(reqid);
		stcp_castor.reqid          = stcp_old.reqid;
		CHECK_VAR_SIZE(status);
		stcp_castor.status         = stcp_old.status;
		CHECK_VAR_SIZE(actual_size);
		stcp_castor.actual_size    = stcp_old.actual_size;
		CHECK_VAR_SIZE(c_time);
		stcp_castor.c_time         = stcp_old.c_time;
		CHECK_VAR_SIZE(a_time);
		stcp_castor.a_time         = stcp_old.a_time;
		CHECK_VAR_SIZE(nbaccesses);
		stcp_castor.nbaccesses     = stcp_old.nbaccesses;
		switch (stcp_castor.t_or_d) {
		case 't':
			CHECK_STRING_SIZE(u1.t.den);
			strcpy(stcp_castor.u1.t.den,    stcp_old.u1.t.den);
			CHECK_STRING_SIZE(u1.t.dgn);
			strcpy(stcp_castor.u1.t.dgn,    stcp_old.u1.t.dgn);
			CHECK_STRING_SIZE(u1.t.fid);
			strcpy(stcp_castor.u1.t.fid,    stcp_old.u1.t.fid);
			CHECK_RAW_SIZE(u1.t.filstat);
			stcp_castor.u1.t.filstat      = stcp_old.u1.t.filstat;
			CHECK_STRING_SIZE(u1.t.fseq);
			strcpy(stcp_castor.u1.t.fseq,   stcp_old.u1.t.fseq);
			CHECK_STRING_SIZE(u1.t.lbl);
			strcpy(stcp_castor.u1.t.lbl,   stcp_old.u1.t.lbl);
			CHECK_VAR_SIZE(u1.t.retentd);
			stcp_castor.u1.t.retentd      = stcp_old.u1.t.retentd;
			CHECK_STRING_SIZE(u1.t.tapesrvr);
			strcpy(stcp_castor.u1.t.tapesrvr,   stcp_old.u1.t.tapesrvr);
			CHECK_RAW_SIZE(u1.t.E_Tflags);
			stcp_castor.u1.t.E_Tflags     = stcp_old.u1.t.E_Tflags;
			for (j = 0; j < MAXVSN; j++) {
				CHECK_STRING_SIZE(u1.t.vid[j]);
				strcpy(stcp_castor.u1.t.vid[j],   stcp_old.u1.t.vid[j]);
				CHECK_STRING_SIZE(u1.t.vsn[j]);
				strcpy(stcp_castor.u1.t.vsn[j],   stcp_old.u1.t.vsn[j]);
			}
			break;
		case 'd':
		case 'a':
			CHECK_STRING_SIZE(u1.d.xfile);
			strcpy(stcp_castor.u1.d.xfile,      stcp_old.u1.d.xfile);
			CHECK_STRING_SIZE(u1.d.Xparm);
			strcpy(stcp_castor.u1.d.Xparm,      stcp_old.u1.d.Xparm);
			break;
		case 'm':
			if (ISCASTOR(stcp_old.u1.m.xfile)) {
				struct Cns_fileid Cnsfileid;
				struct Cns_filestat Cnsfilestat;

				memset(&Cnsfileid, 0, sizeof(struct Cns_fileid));

				stcp_old.t_or_d = stcp_castor.t_or_d = 'h';

				if (ISCASTORHOST(stcp_old.u1.m.xfile)) {
					if (Cns_statx(stcp_old.u1.m.xfile,&Cnsfileid,&Cnsfilestat) != 0) {
						printf("### Cns_statx on %s : %s\n",stcp_old.u1.m.xfile,sstrerror(serrno));
						status = 1;
					} else {
						/* Note that u1.h.xfile and u1.m.xfile starts are the same address by definition */
						strcpy(stcp_castor.u1.h.server,Cnsfileid.server);
						stcp_castor.u1.h.fileid = Cnsfileid.fileid;
						stcp_castor.u1.h.fileclass = Cnsfilestat.fileclass;
						stcp_castor.u1.h.tppool[0] = '\0';
					}
				} else if (! ISHPSSHOST(stcp_old.u1.m.xfile)) {
					/* Try neverthless to stat */
					if (Cns_statx(stcp_old.u1.m.xfile,&Cnsfileid,&Cnsfilestat) != 0) {
						printf("### Cns_statx on %s : %s\n",stcp_old.u1.m.xfile,sstrerror(serrno));
						status = 1;
					} else {
						/* Note that u1.h.xfile and u1.m.xfile starts are the same address by definition */
						strcpy(stcp_castor.u1.h.server,Cnsfileid.server);
						stcp_castor.u1.h.fileid = Cnsfileid.fileid;
						stcp_castor.u1.h.fileclass = Cnsfilestat.fileclass;
						stcp_castor.u1.h.tppool[0] = '\0';
					}
				} else {
					printf("### %s : not a CASTOR host\n",stcp_old.u1.m.xfile);
					status = 1;
				}
			} else {
				CHECK_STRING_SIZE(u1.m.xfile);
				strcpy(stcp_castor.u1.m.xfile,      stcp_old.u1.m.xfile);
			}
			break;
		default:
			printf("### Unknown t_or_d type at reqid = %d\n",stcp_castor.reqid);
			status = 1;
			break;
		}
		if (status != 1) {
			if (write(stgcat_out_fd, &stcp_castor, sizeof(struct stgcat_entry)) != 
					sizeof(struct stgcat_entry)) {
				printf("### write error on %s (%s) at %s:%d\n",stgcat_out,
							 strerror(errno),__FILE__,__LINE__);
				return(-1);
			}
		}
	}
	return(0);
}

int convert_stgpath() {
	int answer;
	struct stgpath_entry_old stcp_old;
	struct stgpath_entry     stcp_castor;
	int i;
	int imax;

	/* We check in the size of the in-file module SHIFT stager structure matches... */
	if (stgpath_in_statbuff.st_size % sizeof(struct stgpath_entry_old) != 0) {
		printf("### Current %s length %% sizeof(old SHIFT structure) is NOT zero (%d)...\n"
					 ".... I believe that either this file is truncated or currupted, either\n"
					 ".... you did not gave a read old SHIFT stager stgpath file as input\n"
					 ,stgpath_in,
					 (int) (stgpath_in_statbuff.st_size % sizeof(struct stgpath_entry_old)));
		printf("### Do you really want to proceed (y/n) ? ");
		answer = getchar();
		fflush(stdin);
		if (answer != 'y' && answer != 'Y') {
			return(-1);
		}
	}

	/* We convert all the entries */
	imax = stgpath_in_statbuff.st_size / sizeof(struct stgpath_entry_old) - 1;
	for (i = 0; i <= imax; i++) {
		if (read(stgpath_in_fd, &stcp_old, sizeof(struct stgpath_entry_old)) != 
				sizeof(struct stgpath_entry_old)) {
			printf("### read error on %s (%s) at %s:%d\n",stgpath_in,
						 strerror(errno),__FILE__,__LINE__);
			return(-1);
		}
		/* Convert the SHIFT entry to a CASTOR one */
		if (i == 0 || i % frequency == 0 || i == imax) {
			printf("--> (%5d/%5d) Doing reqid = %d\n",i+1,imax+1,stcp_old.reqid);
		}
		memset((char *) &stcp_castor, 0, sizeof(struct stgpath_entry));
		CHECK_VAR_SIZE(reqid);
		stcp_castor.reqid          = stcp_old.reqid;
		CHECK_STRING_SIZE(upath);
		strcpy(stcp_castor.upath,    stcp_old.upath);
		if (write(stgpath_out_fd, &stcp_castor, sizeof(struct stgpath_entry)) != 
				sizeof(struct stgpath_entry)) {
			printf("### write error on %s (%s) at %s:%d\n",stgpath_out,
						 strerror(errno),__FILE__,__LINE__);
			return(-1);
		}
	}
	return(0);
}

void shift_to_castor_usage() {
	printf("Usage : stgshift_to_castor [options] <stgcat_in> <stgpath_in> <stgcat_out> <stgpath_out>\n"
				 "\n"
				 "  where options can be:\n"
				 "  -h           This help\n"
				 "  -C           Skip stgcat conversion\n"
				 "  -L           Skip stgpath conversion\n"
				 "  -n <number>  Print output every <number> iterations. Default is %d.\n"
				 "\n"
				 "  This program will convert SHIFT stager catalogs to CASTOR ones. The SHIFT stager catalogs are typically <stgcat_in> == /usr/spool/stage/stgcat and <stgpath_in> == /usr/spool/stage/stgpath\n"
				 "\n"
				 "  Example:\n"
				 "stgshift_to_castor /usr/spool/stage/stgcat /usr/spool/stage/stgpath ./stgcat ./stgpath\n"
				 "Comments to castor-support@listbox.cern.ch\n"
				 "\n",
				 FREQUENCY
				 );
}

