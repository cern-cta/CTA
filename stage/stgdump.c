/*
 * $Id: stgdump.c,v 1.7 2003/01/16 13:13:24 jdurand Exp $
 */

/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stgdump.c,v $ $Revision: 1.7 $ $Date: 2003/01/16 13:13:24 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <string.h>
#include <errno.h>
#include "stage_api.h"
#include "osdep.h"
#include "Cgetopt.h"
#include "serrno.h"
#include "rfio_api.h"
#include "u64subr.h"

void log_callback _PROTO((int, char *));
void qry_callback _PROTO((struct stgcat_entry *, struct stgpath_entry *));
void usage _PROTO((char *));

int fd_stcp;
int fd_stpp;
int reqid;
int istcp_output = 0;
int istpp_output = 0;

char *stcp_output = NULL;
char *stpp_output = NULL;
char *stghost_input = NULL;
char *stghost_output = NULL;
static int stcp_output_flag = 0;
static int stpp_output_flag = 0;
static int stghost_input_flag = 0;
static int stghost_output_flag = 0;

int main(argc,argv)
	int argc;
	char **argv;
{
	struct stgcat_entry stcp_dummy;
	int nstcp_output = 0;
	int nstpp_output = 0;
	int rc;
	char *func = "stgdump";
	int errflg = 0;
	int c;
	char *stghost = NULL;
	int v;

	static struct Coptions longopts[] =
	{
		{"stcp_output",        REQUIRED_ARGUMENT,  &stcp_output_flag,  1},
		{"stpp_output",        REQUIRED_ARGUMENT,  &stpp_output_flag,  1},
		{"stghost_input",      REQUIRED_ARGUMENT,  &stghost_input_flag,  1},
		{"stghost_output",     REQUIRED_ARGUMENT,  &stghost_output_flag,  1},
		{NULL,                 0,                  NULL,               0}
	};

	if (stage_setlog(&log_callback) != 0) {
		stage_errmsg(func,"startup : Cannot set log callback (%s)\n", sstrerror(serrno));
	}

	Coptind = 1;
	Copterr = 1;

	stcp_output_flag = 0;
	stpp_output_flag = 0;

	while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != -1) {
		switch (c) {
		case 'h':
			stghost = Coptarg;
			break;
		case 0:
			/* Long option without short option correspondance */
			if (stcp_output_flag && ! stcp_output) {
				/* --stcp_output */
				stcp_output = Coptarg;
			}
			if (stpp_output_flag && ! stpp_output) {
				/* --stpp_output */
				stpp_output = Coptarg;
			}
			if (stghost_input_flag && ! stghost_input) {
				/* --stghost_input */
				stghost_input = Coptarg;
			}
			if (stghost_output_flag && ! stghost_output) {
				/* --stghost_output */
				stghost_output = Coptarg;
			}
			break;
		case '?':
			errflg++;
			break;
		default:
			errflg++;
			break;
		}
        if (errflg != 0) break;
	}

	if (errflg != 0) {
		usage(argv[0]);
		exit(EXIT_FAILURE);
	}
	if ((stcp_output == NULL) && (stpp_output == NULL)) {
		stage_errmsg(func,"arguments : --stcp_output or --stpp_output are mandatory\n");
		usage(argv[0]);
		exit(EXIT_FAILURE);
	}
	if (((stghost_input != NULL) && (stghost_output == NULL)) ||
		((stghost_input == NULL) && (stghost_output != NULL))) {
		stage_errmsg(func,"arguments : --stghost_input and --stghost_output must be set all together\n");
		usage(argv[0]);
		exit(EXIT_FAILURE);
	}
	
	if ((stcp_output != NULL) && (stpp_output != NULL)) {
		if (strcmp(stcp_output,stpp_output) == 0) {
			stage_errmsg(func,"arguments : --stcp_output and --stpp_output are equivalent\n");
			exit(EXIT_FAILURE);
		}
	}
	
	/* We will not seek etc... so we can work in V3 mode */
	v = RFIO_STREAM;
	rfiosetopt(RFIO_READOPT,&v,4); 

	if (stghost_output != NULL) {
		char myenv[1024];

		sprintf(myenv,"STAGE_HOST=%s",stghost_output);
		if (putenv(myenv) != 0) {
			stage_errmsg(func,"Cannot putenv(\"%s\"), %s\n", myenv, strerror(errno));
		}
	}

	/* Open output files */
	if (stcp_output != NULL) {
		PRE_RFIO;
		fprintf(stdout, "... Opening %s\n", stcp_output);
		if ((fd_stcp = rfio_open64(stcp_output, O_CREAT|O_WRONLY|O_TRUNC, 0644)) < 0) {
			stage_errmsg(func,"%s rfio_open64 error : %s\n", stcp_output, rfio_serror());
			exit(EXIT_FAILURE);
		}
	}
	if (stpp_output != NULL) {
		PRE_RFIO;
		fprintf(stdout, "... Opening %s\n", stpp_output);
		if ((fd_stpp = rfio_open64(stpp_output, O_CREAT|O_WRONLY|O_TRUNC, 0644)) < 0) {
			stage_errmsg(func,"%s rfio_open64 error : %s\n", stpp_output, rfio_serror());
			rfio_close(fd_stcp);
			exit(EXIT_FAILURE);
		}
	}

	if (stage_setcallback(&qry_callback) != 0) {
		stage_errmsg(func,"startup : Cannot set callback (%s)\n", sstrerror(serrno));
	}

	memset(&stcp_dummy, 0, sizeof(struct stgcat_entry));
	if (stcp_output != NULL) {
		if (stghost_input != NULL) {
			char myenv[1024];
			
			sprintf(myenv,"STAGE_HOST=%s",stghost_input);
			if (putenv(myenv) != 0) {
				stage_errmsg(func,"Cannot putenv(\"%s\"), %s\n", myenv, strerror(errno));
			}
		}

		fprintf(stdout,"\n... Querying   stgcat (main catalog)\n");
		if ((rc = stage_qry('t',                    /* t_or_d */
							(u_signed64) STAGE_ALL, /* Flags */
							stghost,                /* Hostname */
							1,                      /* nstcp_input */
							&stcp_dummy,            /* stcp_input */
							&nstcp_output,          /* nstcp_output */
							NULL,                   /* stcp_output */
							&nstpp_output,          /* nstpp_output */
							NULL                    /* stpp_output */
			)) < 0) {
			if (serrno == ENOENT) {
				/* Okay, that means the list is empty */
				goto stgcat_ok;
			} else {
				stage_errmsg(func,"stgcat stage_qry error : %s\n", sstrerror(serrno));
			}
		} else {
		  stgcat_ok:
			fprintf(stdout,"==> Got %6d stgcat (main catalog) entr%s\n", nstcp_output, (nstcp_output <= 1) ? "y" : "ies");
		}
	}
	if (stpp_output != NULL) {
		if (stghost_input != NULL) {
			char myenv[1024];
			
			sprintf(myenv,"STAGE_HOST=%s",stghost_input);
			if (putenv(myenv) != 0) {
				stage_errmsg(func,"Cannot putenv(\"%s\"), %s\n", myenv, strerror(errno));
			}
		}
		fprintf(stdout,"\n... Querying   stgpath (link catalog)\n");
		if ((rc = stage_qry('t',                    /* t_or_d */
							(u_signed64) STAGE_ALL|STAGE_LINKNAME, /* Flags */
							stghost,                /* Hostname */
							1,                      /* nstcp_input */
							&stcp_dummy,            /* stcp_input */
							&nstcp_output,          /* nstcp_output */
							NULL,                   /* stcp_output */
							&nstpp_output,          /* nstpp_output */
							NULL                    /* stpp_output */
			)) < 0) {
			if (serrno == ENOENT) {
				/* Okay, that means the list is empty */
				goto stgpath_ok;
			} else {
				stage_errmsg(func,"stgpath stage_qry error : %s\n", sstrerror(serrno));
			}
		} else {
		  stgpath_ok:
			fprintf(stdout,"==> Got %6d stgpath (link catalog) entr%s\n", nstpp_output, (nstpp_output <= 1) ? "y" : "ies");
		}
	}

	if (stghost_output != NULL) {
		char myenv[1024];

		sprintf(myenv,"STAGE_HOST=%s",stghost_output);
		if (putenv(myenv) != 0) {
			stage_errmsg(func,"Cannot putenv(\"%s\"), %s\n", myenv, strerror(errno));
		}
	}

	if (stcp_output != NULL) {
		PRE_RFIO;
		fprintf(stdout, "\n... Closing %s ", stcp_output);
		if ((rc = rfio_close(fd_stcp)) < 0) {
			stage_errmsg(func,"%s rfio_close error : %s\n", stcp_output, rfio_serror());
		} else {
			struct stat64 statbuf;
			if (rfio_stat64(stcp_output,&statbuf) != 0) {
				stage_errmsg(func,"%s rfio_stat error : %s\n", stcp_output, rfio_serror());
			} else {
				char tmpbuf[21];
				fprintf(stdout,"(%s byte%s)\n", u64tostr((u_signed64) statbuf.st_size, tmpbuf, 0), (statbuf.st_size > 1) ? "s" : "");
			}
		}
	}
	if (stpp_output != NULL) {
		PRE_RFIO;
		if (stcp_output == NULL) {
			fprintf(stdout,"\n");
		}
		fprintf(stdout, "... Closing %s ", stpp_output);
		if ((rc = rfio_close(fd_stpp)) < 0) {
			stage_errmsg(func,"%s rfio_close error : %s\n", stpp_output, rfio_serror());
		} else {
			struct stat64 statbuf;
			if (rfio_stat64(stpp_output,&statbuf) != 0) {
				stage_errmsg(func,"%s rfio_stat error : %s\n", stpp_output, rfio_serror());
			} else {
				char tmpbuf[21];
				fprintf(stdout,"(%s byte%s)\n", u64tostr((u_signed64) statbuf.st_size, tmpbuf, 0), (statbuf.st_size > 1) ? "s" : "");
			}
		}
	}

	exit(rc ? EXIT_FAILURE : EXIT_SUCCESS);

}

void log_callback(level,message)
	int level;
	char *message;
{
	if (level == MSG_ERR) {
		fprintf(stderr,"%s",message);
	}
	return;
}

void qry_callback(stcp,stpp)
	struct stgcat_entry *stcp;
	struct stgpath_entry *stpp;
{
	char *func = "qry_callback";
	if ((stcp == NULL) && (stpp == NULL)) {
		stage_errmsg(func,"(stcp == NULL) && (stpp == NULL) !?\n");
		return;
	}
	if ((stcp != NULL) && (stcp_output != NULL)) {
		/* stcp record */
		PRE_RFIO;
		if (rfio_write(fd_stcp, stcp, (int) sizeof(struct stgcat_entry)) != sizeof(struct stgcat_entry)) {
			stage_errmsg(func,"rfio_write of stcp error : %s\n", rfio_serror());
			exit(EXIT_FAILURE);
		} else {
			if (++istcp_output % 1000 == 0) fprintf(stdout,"...     %6d\n", istcp_output);
		}
		
	}
	if ((stpp != NULL) && (stpp_output != NULL)) {
		/* stpp record */
		PRE_RFIO;
		if (rfio_write(fd_stpp, stpp, (int) sizeof(struct stgpath_entry)) != sizeof(struct stgpath_entry)) {
			stage_errmsg(func,"rfio_write of stpp error : %s\n", rfio_serror());
			exit(EXIT_FAILURE);
		} else {
			if (++istpp_output % 1000 == 0) fprintf(stdout,"...     %6d\n", istpp_output);
		}
		
	}
}

void usage(prog)
	char *prog;
{
	fprintf(stderr,"Usage: %s [--stghost_input hostname] [--stghost_output hostname] --stcp_output <filename> --stpp_output <filename>\n", prog);
}
