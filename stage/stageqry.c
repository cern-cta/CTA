/*
 * $Id: stageqry.c,v 1.39 2003/11/17 13:11:17 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stageqry.c,v $ $Revision: 1.39 $ $Date: 2003/11/17 13:11:17 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/types.h>
#include <fcntl.h>
#include <grp.h>
#include <pwd.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "stage_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "Cgetopt.h"
#include "serrno.h"
#include "u64subr.h"

#ifndef _WIN32
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */
#endif

#define THEMAX(a,b) ((a) > (b) ? (a) : (b))

extern int getlist_of_vid _PROTO((char *, char[MAXVSN][7], int *));
#if (defined(IRIX5) || defined(IRIX6) || defined(IRIX64))
/* Surpringly, on Silicon Graphics, strdup declaration depends on non-obvious macros */
extern char *strdup _PROTO((CONST char *));
#endif

void usage _PROTO((char *));
void log_callback _PROTO((int, char *));
void record_callback _PROTO((struct stgcat_entry *, struct stgpath_entry *));
void cleanup _PROTO((int));
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
int funcrep _PROTO((int *, int, ...));
#else
int funcrep _PROTO(());
#endif

int noregexp_flag = 0;
int reqid_flag = 0;
int dump_flag = 0;
int migrator_flag = 0;
int class_flag = 0;
int queue_flag = 0;
int counters_flag = 0;
int retenp_flag = 0;
int mintime_flag = 0;
int side_flag = 0;
int display_side_flag = 0;
int output_flag = 0;
int format_flag = 0;
FILE *output_fd = NULL;
int output_fd_toclean = 0;
char *format = NULL;
int noheader_flag = 0;
int doneheader = 0;
int withna_flag = 0;
int withunit_flag = 0;
int withstatusnumber_flag = 0;
int withtimenumber_flag = 0;
int check_format_string = 0;
u_signed64 flags = 0;
int noretry_flag = 0;
int delimiter_flag = 0;
char *delimiter = NULL;

int main(argc, argv)
	int	argc;
	char	**argv;
{
	int c;
	int errflg = 0;
	int numvid;
	char *stghost = NULL;
	struct stgcat_entry stcp;
    int nstcp_output = 0;
    struct stgcat_entry *stcp_output = NULL;
    int nstpp_output = 0;
	int have_parsed_side = 0;
    struct stgpath_entry *stpp_output = NULL;
	static struct Coptions longopts[] =
	{
		{"allocated",          REQUIRED_ARGUMENT,  NULL,      'A'},
		{"allgroups",          NO_ARGUMENT,        NULL,      'a'},
		{"full",               NO_ARGUMENT,        NULL,      'f'},
		{"grpuser",            NO_ARGUMENT,        NULL,      'G'},
		{"host",               REQUIRED_ARGUMENT,  NULL,      'h'},
		{"external_filename",  REQUIRED_ARGUMENT,  NULL,      'I'},
		{"link",               NO_ARGUMENT,        NULL,      'L'},
		{"long",               NO_ARGUMENT,        NULL,      'l'},
		{"migration_filename", REQUIRED_ARGUMENT,  NULL,      'M'},
		{"path",               NO_ARGUMENT,        NULL,      'P'},
		{"poolname",           REQUIRED_ARGUMENT,  NULL,      'p'},
		{"file_sequence",      REQUIRED_ARGUMENT,  NULL,      'q'},
		{"file_range",         REQUIRED_ARGUMENT,  NULL,      'Q'},
		{"sort",               NO_ARGUMENT,        NULL,      'S'},
		{"statistic",          NO_ARGUMENT,        NULL,      's'},
		{"tape_info",          NO_ARGUMENT,        NULL,      'T'},
		{"user",               NO_ARGUMENT,        NULL,      'u'},
		{"vid",                REQUIRED_ARGUMENT,  NULL,      'V'},
		{"extended",           NO_ARGUMENT,        NULL,      'x'},
		{"migration_rules",    NO_ARGUMENT,        NULL,      'X'},
		{"noregexp",           NO_ARGUMENT,  &noregexp_flag,    1},
		{"reqid",              REQUIRED_ARGUMENT,  &reqid_flag, 1},
		{"dump",               NO_ARGUMENT,  &dump_flag,        1},
		{"migrator",           NO_ARGUMENT,  &migrator_flag,    1},
		{"class",              NO_ARGUMENT,  &class_flag,       1},
		{"queue",              NO_ARGUMENT,  &queue_flag,       1},
		{"counters",           NO_ARGUMENT,  &counters_flag,    1},
		{"retenp",             NO_ARGUMENT,  &retenp_flag,      1},
		{"side",               REQUIRED_ARGUMENT, &side_flag,   1},
		{"display_side",       NO_ARGUMENT, &display_side_flag, 1},
		{"ds",                 NO_ARGUMENT, &display_side_flag, 1},
		{"mintime",            NO_ARGUMENT,  &mintime_flag,     1},
        {"output",             REQUIRED_ARGUMENT, &output_flag, 1},
        {"format",             REQUIRED_ARGUMENT, &format_flag, 1},
        {"noheader",           NO_ARGUMENT, &noheader_flag,     1},
        {"withna",             NO_ARGUMENT, &withna_flag,       1},
        {"withunit",           NO_ARGUMENT, &withunit_flag,     1},
        {"withstatusnumber",   NO_ARGUMENT, &withstatusnumber_flag, 1},
        {"withtimenumber",     NO_ARGUMENT, &withtimenumber_flag, 1},
		{"noretry",            NO_ARGUMENT,     &noretry_flag,  1},
		{"delimiter",          REQUIRED_ARGUMENT,  &delimiter_flag,  1},
		{NULL,                 0,                  NULL,        0}
	};

	memset(&stcp,0,sizeof(struct stgcat_entry));
    /* In case it is an HSM request we make sure retenp_on_disk and mintime_before_migr are not logged for nothing */
	numvid = 0;
    Coptind = 1;
	Copterr = 1;
	while ((c = Cgetopt_long (argc, argv, "A:afGh:I:LlM:Pp:q:Q:SsTuV:x", longopts, NULL)) != -1) {
		switch (c) {
		case 'A':
			if (stcp.t_or_d != '\0' && stcp.t_or_d != 'a') {
				++errflg;
			} else {
				stcp.t_or_d = 'a';
				strncpy(stcp.u1.d.xfile,Coptarg,(CA_MAXHOSTNAMELEN+MAXPATH));
			}
			flags |= STAGE_ALLOCED;
			break;
		case 'a':
			flags |= STAGE_ALL;
			break;
		case 'f':
			flags |= STAGE_FILENAME;
			break;
		case 'G':
			flags |= STAGE_GRPUSER;
			break;
		case 'h':
			stghost = Coptarg;
			break;
		case 'I':
			flags |= STAGE_EXTERNAL;
			break;
		case 'L':
			flags |= STAGE_LINKNAME;
			break;
		case 'l':
			flags |= STAGE_LONG;
			break;
		case 'M':
			if (stcp.t_or_d != '\0' && stcp.t_or_d != 'm') {
				++errflg;
			} else {
				stcp.t_or_d = 'm';
				strncpy(stcp.u1.m.xfile,Coptarg,STAGE_MAX_HSMLENGTH);
				/* In case it is an HSM request we make sure retenp_on_disk and mintime_before_migr are not logged for nothing */
				/* Check how is defined the structure stgcat_entry and you will see that there is no problem to initialize */
				/* the following values even if they are not in same union... */
				stcp.u1.h.retenp_on_disk = -1;
				stcp.u1.h.mintime_beforemigr = -1;
			}
			break;
		case 'P':
			flags |= STAGE_PATHNAME;
			break;
		case 'p':
			strncpy(stcp.poolname,Coptarg,CA_MAXPOOLNAMELEN); /* Next characters guaranteed '\0' because of the memset */
			break;
		case 'Q':
			flags |= STAGE_MULTIFSEQ;
			/* There is intentionnaly no break here */
		case 'q':
			if (stcp.t_or_d != '\0' && stcp.t_or_d != 't') {
				++errflg;
			} else {
				stcp.t_or_d = 't';
				strncpy(stcp.u1.t.fseq, Coptarg, CA_MAXFSEQLEN);
			}
			break;
		case 'S':
			flags |= STAGE_SORTED;
			break;
		case 's':
			flags |= STAGE_STATPOOL;
			break;
		case 'T':
			flags |= STAGE_TAPEINFO;
			break;
		case 'u':
			flags |= STAGE_USER;
			break;
		case 'V':
			if (stcp.t_or_d != '\0' && stcp.t_or_d != 't') {
				++errflg;
			} else {
				stcp.t_or_d = 't';
				errflg += getlist_of_vid ("-V", stcp.u1.t.vid, &numvid);
			}
			break;
		case 'x':
			flags |= STAGE_EXTENDED;
			break;
		case 0:
			/* Here are the long options */
			if (reqid_flag && stcp.reqid == 0) {
				if ((stcp.reqid = atoi(Coptarg)) < 0) {
					stcp.reqid = 0;
				} else {
					flags |= STAGE_REQID;					
				}
			}
			if (side_flag && ! have_parsed_side) {
				if ((stcp.t_or_d != '\0' && stcp.t_or_d != 't')) {
					++errflg;
				} else {
					stcp.t_or_d = 't';
					stcp.u1.t.side = atoi(Coptarg);
				}
				have_parsed_side++;
			}
			if (output_flag && ! output_fd) {
				if (strcmp(Coptarg,"stdout") == 0) {
					output_fd = stdout;
				} else if (strcmp(Coptarg, "stderr") == 0) {
					output_fd = stderr;
				} else if ((output_fd = fopen(Coptarg,"w")) == NULL) {
					fprintf(stderr,"%s: fopen error: %s\n", Coptarg, strerror(errno));
					exit(USERR);
					output_fd_toclean = 1;
				}
			}
			if (format_flag && ! format) {
				format = Coptarg;
			}
			if (delimiter_flag && ! delimiter) {
				delimiter = Coptarg;
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

    if (reqid_flag) {
		flags |= STAGE_REQID;
		if (! stcp.t_or_d) {
			stcp.t_or_d = 'm'; /* Needed to pass reqid in the protocol */
			stcp.u1.h.retenp_on_disk = -1;
			stcp.u1.h.mintime_beforemigr = -1;
		}
	}
	if (stcp.poolname[0] != '\0' && ! stcp.t_or_d) {
		/* Use user's -p option and nothing else */
		stcp.t_or_d = 'm'; /* Needed to pass reqid in the protocol */
		stcp.u1.h.retenp_on_disk = -1;
		stcp.u1.h.mintime_beforemigr = -1;
	}
    if (noregexp_flag) flags |= STAGE_NOREGEXP;
    if (dump_flag) flags |= STAGE_DUMP;
    if (migrator_flag) flags |= STAGE_MIGRULES;
    if (class_flag) flags |= STAGE_CLASS;
    if (queue_flag) flags |= STAGE_QUEUE;
    if (counters_flag) flags |= STAGE_COUNTERS;
    if (retenp_flag) flags |= STAGE_RETENP;
    if (display_side_flag) flags |= STAGE_DISPLAY_SIDE;
    if (mintime_flag) flags |= STAGE_MINTIME;
    if (side_flag) flags |= STAGE_SIDE;
    if (noretry_flag) flags |= STAGE_NORETRY;

	if (argc > Coptind) {
		fprintf (stderr, STG16);
		errflg++;
	}

	if (errflg != 0) {
		usage (argv[0]);
        if (output_fd_toclean) fclose(output_fd);
		exit (USERR);	
    }

    if ((flags & STAGE_STATPOOL) == STAGE_STATPOOL) {
		/* When querying the pool statistics --format and al. options are meaningless */
		format = NULL;
		flags &= ~STAGE_FORMAT;
    }

    if (format) {
		check_format_string = 1;
		record_callback(NULL,NULL);
		if (check_format_string != 0) {
			usage (argv[0]);
			if (output_fd_toclean) fclose(output_fd);
			exit (USERR);
		}
		flags |= STAGE_FORMAT; /* hint for the stgdaemon */
    }

    if (stage_setlog((void (*) _PROTO((int, char *))) &log_callback) != 0) {
		stage_errmsg(NULL, STG02, "stageqry", "stage_setlog", sstrerror(serrno));
		if (output_fd_toclean) fclose(output_fd);
		exit(rc_castor2shift(serrno));
    }

    if (stage_setcallback(&record_callback) != 0) {
		stage_errmsg(NULL, STG02, "stageqry", "stage_setcallback", sstrerror(serrno));
		if (output_fd_toclean) fclose(output_fd);
		exit(rc_castor2shift(serrno));
    }

#if !defined(_WIN32)
    signal (SIGHUP, cleanup);
#endif
    signal (SIGINT, cleanup);
#if !defined(_WIN32)
    signal (SIGQUIT, cleanup);
#endif
    signal (SIGTERM, cleanup);

    if ((c = stage_qry(stcp.t_or_d,
                       flags,
                       stghost,
                       stcp.t_or_d ? 1 : 0,
                       stcp.t_or_d ? &stcp : NULL,
                       &nstcp_output, &stcp_output,
                       &nstpp_output, &stpp_output)) != 0) {
		/* Unless ENOENT, there is a real error */
		/* For backward compatibility ENOENT is not considered a failure in the command-line */
		if (serrno == ENOENT) {
			c = 0;
		} else {
			stage_errmsg(NULL, STG02, "stageqry", "stage_qry", sstrerror(serrno));
		}
    }
    if (stcp_output != NULL) free(stcp_output);
    if (stpp_output != NULL) free(stpp_output);

    if (output_fd_toclean) fclose(output_fd);
    exit((c == 0) ? 0 : rc_castor2shift(serrno));
}

void cleanup(sig)
	int sig;
{
	signal (sig, SIG_IGN);
        
#if defined(_WIN32)
	WSACleanup();
#endif
	if (output_fd_toclean) fclose(output_fd);
	exit (USERR);
}

void usage(cmd)
	char *cmd;
{
	fprintf (stderr, "usage: %s [options] where options are:\n%s",
			 cmd,
			 "[-A pattern | -M pattern] [-a] [-f] [-G] [-h stage_host]\n"
			 "[-I external_filename] [-L] [-l] [-P] [-p pool]\n"
			 "[-q file_sequence_number(s)] [-Q file_sequence_range]\n"
			 "[-S] [-s] [-T] [-u] [-V visual_identifier(s)] [-x]\n"
			 "[--class] [--counters] [--display_side or --ds] [--dump]\n"
			 "[--format formatstring] [--migrator] [--mintime]\n"
			 "[--noheader] [--noregexp] [--output filename]\n"
			 "[--queue] [--reqid reqid] [--retenp] [--side sidenumber]\n"
			 "[--withna] [--withstatusnumber] [--withtimenumber] [--withunit]\n"
		);
}

void log_callback(level,message)
	int level;
	char *message;
{
	if (format == NULL) {
		if (level == MSG_ERR) {
			fprintf(output_fd != NULL ? output_fd : stderr, "%s", message);
		} else {
			fprintf(output_fd != NULL ? output_fd : stdout, "%s", message);
		}
	} else {
		if (level == MSG_ERR) {
			fprintf(output_fd != NULL ? output_fd : stderr, "%s", message);
		}
	}
}

void record_callback(stcp,stpp)
	struct stgcat_entry *stcp;
	struct stgpath_entry *stpp;
{
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
	char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */
	char *p, *q, *qnext;
	FILE *fd = output_fd != NULL ? output_fd : stdout;
	int dospace = 0;
	char *thespace = (delimiter != NULL) ? delimiter : " ";
	char *thena = withna_flag ? "N/A" : "";

	if ((stcp == NULL) && (stpp == NULL)) {
		if (! check_format_string) {
			return;
		}
		/* We are running in the check_format_string mode */
	}

	if (! format) {
		return;
	}

	if (stcp != NULL || (check_format_string && ((flags & STAGE_LINKNAME) != STAGE_LINKNAME))) {
    
		/* format contain the list of members to print out */
		/* format is like: member1,member2,member3,...,membern */
		/* format can contain: */
	  redo_parse_stcp:
		if ((p = strdup(format)) == NULL) {
			stage_errmsg(NULL, STG02, "stageqry", "strdup", strerror(errno));
			if (output_fd_toclean) fclose(output_fd);
			exit(USERR);
		}
    
		q = strtok (p,",");
		while (q != NULL) {
			qnext = strtok(NULL, ",");
			if (dospace == 1 && ! check_format_string) fprintf(fd,thespace);
			/* ----------------------------------- */
			if (strcmp (q, "blksize") == 0) {
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,"%7s","blksize");
					goto next_token_stcp;
				}
				if (stcp->t_or_d == 't') {
					fprintf(fd,"%7d",stcp->blksize);
				} else {
					fprintf(fd,"%7s",thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "charconv") == 0) {
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,qnext ? "%-8s" : "%s","charconv");
					goto next_token_stcp;
				}
				if (stcp->t_or_d == 't') {
					char charconvstring[1024];
					fprintf(fd,qnext ? "%-8s" : "%s",stage_util_charconv2string((char *) charconvstring, (int) 1024, (int) stcp->charconv) == 0 ? charconvstring : thena);
				} else {
					fprintf(fd,qnext ? "%-8s" : "%s",thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "keep") == 0) {
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,qnext ? "%-4s" : "%s","keep");
					goto next_token_stcp;
				}
				fprintf(fd,qnext ? "%-4s" : "%s",stcp->keep ? "K" : thena);
				/* ----------------------------------- */
			} else if (strcmp (q, "lrecl") == 0) {
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,"%7s","lrecl");
					goto next_token_stcp;
				}
				if (stcp->t_or_d == 't') {
					fprintf(fd,"%7d",stcp->lrecl);
				} else {
					fprintf(fd,"%7s",thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "nread") == 0) {
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,"%7s","nread");
					goto next_token_stcp;
				}
				if (stcp->t_or_d == 't') {
					fprintf(fd,"%7d",stcp->nread);
				} else {
					fprintf(fd,"%7s",thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "poolname") == 0) {
				int themax;
				if (check_format_string) goto next_token_stcp;
				themax = THEMAX(CA_MAXPOOLNAMELEN,THEMAX(strlen("poolname"),strlen(thena)));
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					/* Unless last token we have to reserve space */
					if (qnext) {
						fprintf(fd,"%-*s",themax,"poolname");
					} else {
						fprintf(fd,"%-s","poolname");
					}
					goto next_token_stcp;
				}
				if (qnext) {
					fprintf(fd,"%-*s",themax,stcp->poolname[0] != '\0' ? stcp->poolname : thena);
				} else {
					fprintf(fd,"%-s",stcp->poolname[0] != '\0' ? stcp->poolname : thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "recfm") == 0) {
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,qnext ? "%-5s" : "%s","recfm");
					goto next_token_stcp;
				}
				if (stcp->t_or_d == 't') {
					fprintf(fd,qnext ? "%-5s" : "%s",stcp->recfm[0] != '\0' ? stcp->recfm : thena);
				} else {
					fprintf(fd,qnext ? "%-5s" : "%s",thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "size") == 0) {
				char tmpbuf[21];
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					if (withunit_flag) {
						fprintf(fd,"%8s","size");
					} else {
						fprintf(fd,"%20s","size");
					}
					goto next_token_stcp;
				}
				if (withunit_flag) {
					fprintf(fd,"%8s",u64tostru(stcp->size,tmpbuf,8));
				} else {
					fprintf(fd,"%20s",u64tostr(stcp->size,tmpbuf,20));
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "ipath") == 0) {
				int themax;
				if (check_format_string) goto next_token_stcp;
				themax = THEMAX((CA_MAXHOSTNAMELEN+MAXPATH),THEMAX(strlen("ipath"),strlen(thena)));
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					if (qnext) {
						fprintf(fd,"%-*s",themax,"ipath");
					} else {
						fprintf(fd,"%s","ipath");
					}
					goto next_token_stcp;
				}
				if (qnext) {
					fprintf(fd,"%-*s",themax,stcp->ipath[0] != '\0' ? stcp->ipath : thena);
				} else {
					fprintf(fd,"%s",stcp->ipath[0] != '\0' ? stcp->ipath : thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "t_or_d") == 0) {
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,qnext ? "%-6s" : "%s","t_or_d");
					goto next_token_stcp;
				}
				if (stcp->t_or_d) {
					fprintf(fd,qnext ? "%-6c" : "%c",stcp->t_or_d);
				} else {
					fprintf(fd,qnext ? "%-6s" : "%s",thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "group") == 0) {
				int themax;
				if (check_format_string) goto next_token_stcp;
				themax = THEMAX(CA_MAXGRPNAMELEN,THEMAX(strlen("group"),strlen(thena)));
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					/* Unless last token we have to reserve space */
					if (qnext) {
						fprintf(fd,"%-*s",themax,"group");
					} else {
						fprintf(fd,"%s","group");
					}
					goto next_token_stcp;
				}
				if (qnext) {
					fprintf(fd,"%-*s",themax,stcp->group[0] != '\0' ? stcp->group : thena);
				} else {
					fprintf(fd,"%s",stcp->group[0] != '\0' ? stcp->group : thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "user") == 0) {
				int themax;
				if (check_format_string) goto next_token_stcp;
				themax = THEMAX(CA_MAXUSRNAMELEN,THEMAX(strlen("user"),strlen(thena)));
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					/* Unless last token we have to reserve space */
					if (qnext) {
						fprintf(fd,"%-*s",themax,"user");
					} else {
						fprintf(fd,"%s","user");
					}
					goto next_token_stcp;
				}
				if (qnext) {
					fprintf(fd,"%-*s",themax,stcp->user[0] != '\0' ? stcp->user : thena);
				} else {
					fprintf(fd,"%s",stcp->user[0] != '\0' ? stcp->user : thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "uid") == 0) {
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,"%10s","uid");
					goto next_token_stcp;
				}
				if (stcp->uid) {
					fprintf(fd,"%10d",stcp->uid);
				} else {
					fprintf(fd,"%10s",thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "gid") == 0) {
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,"%10s","gid");
					goto next_token_stcp;
				}
				if (stcp->gid) {
					fprintf(fd,"%10d",stcp->gid);
				} else {
					fprintf(fd,"%10s",thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "mask") == 0) {
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,"%6s","mask");
					goto next_token_stcp;
				}
				if (stcp->mask) {
					char dummy[12];
					sprintf(dummy, "0%o", stcp->mask);
					fprintf(fd,"%6s",dummy);
				} else {
					fprintf(fd,"%6s",thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "reqid") == 0) {
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,"%6s","reqid");
					goto next_token_stcp;
				}
				if (stcp->reqid) {
					fprintf(fd,"%6d",stcp->reqid);
				} else {
					fprintf(fd,"%6s",thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "status") == 0) {
				if (check_format_string) goto next_token_stcp;
				if (withstatusnumber_flag) {
					if ((! noheader_flag) && (! doneheader)) {
						/* Do first the header */
						fprintf(fd,"%10s","status");
						goto next_token_stcp;
					}
					if (stcp->reqid) {
						fprintf(fd,"%10d",stcp->status);
					} else {
						fprintf(fd,"%10s",thena);
					}
				} else {
					/* Short version of status user-friendly */
					char p_stat[13];
					char s_stat[5][9] = {"", "STAGEIN", "STAGEOUT", "STAGEWRT", "STAGEPUT"};
					char l_stat[5][12] = {"", "STAGED_LSZ", "STAGED_TPE", "", "CAN_BE_MIGR"};
					char x_stat[7][12] = {"", "WAITING_SPC", "WAITING_REQ", "STAGED","KILLED", "FAILED", "PUT_FAILED"};
					int themax;

					if (stcp->status & 0xF0)
						if (((stcp->status & STAGED_LSZ) == STAGED_LSZ) ||
							((stcp->status & STAGED_TPE) == STAGED_TPE))
							strcpy (p_stat, l_stat[(stcp->status & 0xF00) >> 8]);
						else
							strcpy (p_stat, x_stat[(stcp->status & 0xF0) >> 4]);
					else if (stcp->status & 0xF00)
						if (ISCASTORBEINGMIG(stcp))
							strcpy( p_stat, "BEING_MIGR");
						else if (ISCASTORWAITINGMIG(stcp))
							strcpy( p_stat, "WAITING_MIGR");
						else
							strcpy (p_stat, l_stat[(stcp->status & 0xF00) >> 8]);
					else if (stcp->status == STAGEALLOC)
						strcpy (p_stat, "STAGEALLOC");
					else if (ISCASTORWAITINGNS(stcp))
						strcpy( p_stat, "WAITING_NS");
					else
						strcpy (p_stat, s_stat[stcp->status]);

					themax = THEMAX(12,THEMAX(strlen("status"),strlen(thena)));

					if ((! noheader_flag) && (! doneheader)) {
						/* Do first the header */
						/* Unless last token we have to reserve space */
						if (qnext) {
							fprintf(fd,"%-*s",themax,"status");
						} else {
							fprintf(fd,"%s","status");
						}
						goto next_token_stcp;
					}
					if (qnext) {
						fprintf(fd,"%-*s",themax,p_stat);
					} else {
						fprintf(fd,"%s",p_stat);
					}
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "actual_size") == 0) {
				char tmpbuf[21];
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					if (withunit_flag) {
						fprintf(fd,"%11s","actual_size");
					} else {
						fprintf(fd,"%20s","actual_size");
					}
					goto next_token_stcp;
				}
				if (withunit_flag) {
					fprintf(fd,"%11s",u64tostru(stcp->actual_size,tmpbuf,8));
				} else {
					fprintf(fd,"%20s",u64tostr(stcp->actual_size,tmpbuf,20));
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "c_time") == 0) {
				if (check_format_string) goto next_token_stcp;
				if (withtimenumber_flag) {
					char tmpbuf[21];
          
					if ((! noheader_flag) && (! doneheader)) {
						/* Do first the header */
						fprintf(fd,"%20s","c_time");
						goto next_token_stcp;
					}
					fprintf(fd,"%20s",u64tostr((u_signed64) stcp->c_time,tmpbuf,20));
				} else {
					/* In practice it will not exceed 15 characters */
					char timestr[64];

					stage_util_time((time_t) stcp->c_time, timestr);
					if ((! noheader_flag) && (! doneheader)) {
						/* Do first the header */
						fprintf(fd,"%-15s","c_time");
						goto next_token_stcp;
					}
					fprintf(fd,"%-15s",timestr);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "a_time") == 0) {
				if (check_format_string) goto next_token_stcp;
				if (withtimenumber_flag) {
					char tmpbuf[21];
          
					if ((! noheader_flag) && (! doneheader)) {
						/* Do first the header */
						fprintf(fd,"%20s","a_time");
						goto next_token_stcp;
					}
					fprintf(fd,"%20s",u64tostr((u_signed64) stcp->a_time,tmpbuf,20));
				} else {
					/* In practice it will not exceed 15 characters */
					char timestr[64];

					stage_util_time((time_t) stcp->a_time, timestr);
					if ((! noheader_flag) && (! doneheader)) {
						/* Do first the header */
						fprintf(fd,"%-15s","a_time");
						goto next_token_stcp;
					}
					fprintf(fd,"%-15s",timestr);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "nbaccesses") == 0) {
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,qnext ? "%10s" : "%s","nbaccesses");
					goto next_token_stcp;
				}
				fprintf(fd,qnext ? "%10d" : "%d",stcp->nbaccesses);
				/* ----------------------------------- */
			} else if (strcmp (q, "den") == 0) {
				int themax;
				if (check_format_string) goto next_token_stcp;
				themax = THEMAX(CA_MAXDENLEN,THEMAX(strlen("den"),strlen(thena)));
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					/* Unless last token we have to reserve space */
					if (qnext) {
						fprintf(fd,"%-*s",themax,"den");
					} else {
						fprintf(fd,"%s","den");
					}
					goto next_token_stcp;
				}
				if (qnext) {
					fprintf(fd,"%-*s",themax,stcp->t_or_d == 't' && stcp->u1.t.den[0] != '\0' ? stcp->u1.t.den : thena);
				} else {
					fprintf(fd,"%s",stcp->t_or_d == 't' && stcp->u1.t.den[0] != '\0' ? stcp->u1.t.den : thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "dgn") == 0) {
				int themax;
				if (check_format_string) goto next_token_stcp;
				themax = THEMAX(CA_MAXDGNLEN,THEMAX(strlen("dgn"),strlen(thena)));
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					/* Unless last token we have to reserve space */
					if (qnext) {
						fprintf(fd,"%-*s",themax,"dgn");
					} else {
						fprintf(fd,"%s","dgn");
					}
					goto next_token_stcp;
				}
				if (qnext) {
					fprintf(fd,"%-*s",themax,stcp->t_or_d == 't' && stcp->u1.t.dgn[0] != '\0' ? stcp->u1.t.dgn : thena);
				} else {
					fprintf(fd,"%s",stcp->t_or_d == 't' && stcp->u1.t.dgn[0] != '\0' ? stcp->u1.t.dgn : thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "fid") == 0) {
				int themax;
				if (check_format_string) goto next_token_stcp;
				themax = THEMAX(CA_MAXFIDLEN,THEMAX(strlen("fid"),strlen(thena)));
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					/* Unless last token we have to reserve space */
					if (qnext) {
						fprintf(fd,"%-*s",themax,"fid");
					} else {
						fprintf(fd,"%s","fid");
					}
					goto next_token_stcp;
				}
				if (qnext) {
					fprintf(fd,"%-*s",themax,stcp->t_or_d == 't' && stcp->u1.t.fid[0] != '\0' ? stcp->u1.t.fid : thena);
				} else {
					fprintf(fd,"%s",stcp->t_or_d == 't' && stcp->u1.t.fid[0] != '\0' ? stcp->u1.t.fid : thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "filstat") == 0) {
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,qnext ? "%-7s" : "%s","filstat");
					goto next_token_stcp;
				}
				if (stcp->t_or_d == 't' && stcp->u1.t.filstat != '\0') {
					fprintf(fd,qnext ? "%-7c" : "%c",stcp->u1.t.filstat);
				} else {
					fprintf(fd,qnext ? "%-7s" : "%s",thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "fseq") == 0) {
				int themax;
				if (check_format_string) goto next_token_stcp;
				themax = THEMAX(CA_MAXFSEQLEN,THEMAX(strlen("fseq"),strlen(thena)));
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					/* Unless last token we have to reserve space */
					fprintf(fd,"%*s",themax,"fseq");
					goto next_token_stcp;
				}
				fprintf(fd,"%*s",themax,stcp->t_or_d == 't' && stcp->u1.t.fseq[0] != '\0' ? stcp->u1.t.fseq : thena);
				/* ----------------------------------- */
			} else if (strcmp (q, "lbl") == 0) {
				int themax;
				if (check_format_string) goto next_token_stcp;
				themax = THEMAX(CA_MAXLBLTYPLEN,THEMAX(strlen("lbl"),strlen(thena)));
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					/* Unless last token we have to reserve space */
					if (qnext) {
						fprintf(fd,"%-*s",themax,"lbl");
					} else {
						fprintf(fd,"%s","lbl");
					}
					goto next_token_stcp;
				}
				if (qnext) {
					fprintf(fd,"%-*s",themax,stcp->t_or_d == 't' && stcp->u1.t.lbl[0] != '\0' ? stcp->u1.t.lbl : thena);
				} else {
					fprintf(fd,"%s",stcp->t_or_d == 't' && stcp->u1.t.lbl[0] != '\0' ? stcp->u1.t.lbl : thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "retentd") == 0) {
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,qnext ? "%10s" : "%s","retentd");
					goto next_token_stcp;
				}
				if (stcp->t_or_d == 't' && stcp->u1.t.retentd != 0) {
					fprintf(fd,qnext ? "%10d" : "%d",stcp->u1.t.retentd);
				} else {
					fprintf(fd,qnext ? "%10s" : "%s",thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "side") == 0) {
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,qnext ? "%2s" : "%s","side");
					goto next_token_stcp;
				}
				if (stcp->t_or_d == 't') {
					fprintf(fd,qnext ? "%2d" : "%d",stcp->u1.t.side);
				} else {
					fprintf(fd,qnext ? "%2s" : "%s",thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "tapesrvr") == 0) {
				int themax;
				if (check_format_string) goto next_token_stcp;
				themax = THEMAX(CA_MAXHOSTNAMELEN,THEMAX(strlen("tapesrvr"),strlen(thena)));
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					/* Unless last token we have to reserve space */
					if (qnext) {
						fprintf(fd,"%-*s",themax,"tapesrvr");
					} else {
						fprintf(fd,"%s","tapesrvr");
					}
					goto next_token_stcp;
				}
				if (qnext) {
					fprintf(fd,"%-*s",themax,stcp->t_or_d == 't' && stcp->u1.t.tapesrvr[0] != '\0' ? stcp->u1.t.tapesrvr : thena);
				} else {
					fprintf(fd,"%s",stcp->t_or_d == 't' && stcp->u1.t.tapesrvr[0] != '\0' ? stcp->u1.t.tapesrvr : thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "E_Tflags") == 0) {
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,qnext ? "%-16s" : "%s","E_Tflags");
					goto next_token_stcp;
				}
				if (stcp->t_or_d == 't') {
					char E_Tflagsstring[1024];
					fprintf(fd,qnext ? "%-16s" : "%s",stage_util_E_Tflags2string((char *) E_Tflagsstring, (int) 1024, (int) stcp->u1.t.E_Tflags) == 0 ? E_Tflagsstring : thena);
				} else {
					fprintf(fd,qnext ? "%-16s" : "%s",thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "vid") == 0) {
				int themax;
				if (check_format_string) goto next_token_stcp;
				/* In practice there is never more than one VID, so we say CA_MAXVIDLEN instead of MAXVSN*CA_MAXVIDLEN */
				themax = THEMAX(CA_MAXVIDLEN,THEMAX(strlen("vid"),strlen(thena)));
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					/* Unless last token we have to reserve space */
					if (qnext) {
						fprintf(fd,"%-*s",themax,"vid");
					} else {
						fprintf(fd,"%s","vid");
					}
					goto next_token_stcp;
				}
				if (qnext) {
					fprintf(fd,"%-*s",themax,stcp->t_or_d == 't' && stcp->u1.t.vid[0][0] != '\0' ? stcp->u1.t.vid[0] : thena);
				} else {
					fprintf(fd,"%s",stcp->t_or_d == 't' && stcp->u1.t.vid[0][0] != '\0' ? stcp->u1.t.vid[0] : thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "vsn") == 0) {
				int themax;
				if (check_format_string) goto next_token_stcp;
				/* In practice there is never more than one VSN, so we say CA_MAXVSNLEN instead of MAXVSN*CA_MAXVSNLEN */
				themax = THEMAX(CA_MAXVSNLEN,THEMAX(strlen("vsn"),strlen(thena)));
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					/* Unless last token we have to reserve space */
					if (qnext) {
						fprintf(fd,"%-*s",themax,"vsn");
					} else {
						fprintf(fd,"%s","vsn");
					}
					goto next_token_stcp;
				}
				if (qnext) {
					fprintf(fd,"%-*s",themax,stcp->t_or_d == 't' && stcp->u1.t.vsn[0][0] != '\0' ? stcp->u1.t.vsn[0] : thena);
				} else {
					fprintf(fd,"%s",stcp->t_or_d == 't' && stcp->u1.t.vsn[0][0] != '\0' ? stcp->u1.t.vsn[0] : thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "xfile") == 0) {
				int themax;
				if (check_format_string) goto next_token_stcp;
				themax = THEMAX(STAGE_MAX_HSMLENGTH,THEMAX(strlen("xfile"),strlen(thena)));
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					/* Unless last token we have to reserve space */
					if (qnext) {
						fprintf(fd,"%-*s",themax,"xfile");
					} else {
						fprintf(fd,"%s","xfile");
					}
					goto next_token_stcp;
				}
				/* Formally stcp->u1.d.xfile, stcp->u1.h.xfile and stcp->u1.m.xfile points to exactly the same address */
				/* In fact the xfile member have no meaning only when we talk about tape files */
				if (qnext) {
					fprintf(fd,"%-*s",themax,stcp->t_or_d != 't' && stcp->u1.d.xfile[0] != '\0' ? stcp->u1.d.xfile : thena);
				} else {
					fprintf(fd,"%s",stcp->t_or_d != 't' && stcp->u1.d.xfile[0] != '\0' ? stcp->u1.d.xfile : thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "server") == 0) {
				int themax;
				if (check_format_string) goto next_token_stcp;
				themax = THEMAX(CA_MAXHOSTNAMELEN,THEMAX(strlen("server"),strlen(thena)));
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					/* Unless last token we have to reserve space */
					if (qnext) {
						fprintf(fd,"%-*s",themax,"server");
					} else {
						fprintf(fd,"%s","server");
					}
					goto next_token_stcp;
				}
				if (qnext) {
					fprintf(fd,"%-*s",themax,stcp->t_or_d == 'h' && stcp->u1.h.server[0] != '\0' ? stcp->u1.h.server : thena);
				} else {
					fprintf(fd,"%s",stcp->t_or_d == 'h' && stcp->u1.h.server[0] != '\0' ? stcp->u1.h.server : thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "fileid") == 0) {
				char tmpbuf[21];
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,"%20s","fileid");
					goto next_token_stcp;
				}
				fprintf(fd,"%20s",stcp->t_or_d == 'h' && stcp->u1.h.fileid != '\0' ? u64tostr(stcp->u1.h.fileid,tmpbuf,20) : thena);
				/* ----------------------------------- */
			} else if (strcmp (q, "fileclass") == 0) {
				/* We always force number of characters - more pretty - not consuming (small length) */
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,"%9s","fileclass");
					goto next_token_stcp;
				}
				if (stcp->t_or_d == 'h' && stcp->u1.h.fileclass != 0) {
					fprintf(fd,"%9d",stcp->u1.h.fileclass);
				} else {
					fprintf(fd,"%9s",thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "tppool") == 0) {
				int themax;
				if (check_format_string) goto next_token_stcp;
				themax = THEMAX(CA_MAXPOOLNAMELEN,THEMAX(strlen("tppool"),strlen(thena)));
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					/* Unless last token we have to reserve space */
					if (qnext) {
						fprintf(fd,"%-*s",themax,"tppool");
					} else {
						fprintf(fd,"%s","tppool");
					}
					goto next_token_stcp;
				}
				if (qnext) {
					fprintf(fd,"%-*s",themax,stcp->t_or_d == 'h' && stcp->u1.h.tppool[0] != '\0' ? stcp->u1.h.tppool : thena);
				} else {
					fprintf(fd,"%s",stcp->t_or_d == 'h' && stcp->u1.h.tppool[0] != '\0' ? stcp->u1.h.tppool : thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "retenp_on_disk") == 0) {
				char tmpbuf[21];
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,"%20s","retenp_on_disk");
					goto next_token_stcp;
				}
				fprintf(fd,"%20s",stcp->t_or_d == 'h' && stcp->u1.h.retenp_on_disk >= 0 ? u64tostr(stcp->u1.h.retenp_on_disk,tmpbuf,20) : thena);
				/* ----------------------------------- */
			} else if (strcmp (q, "mintime_beforemigr") == 0) {
				char tmpbuf[21];
				if (check_format_string) goto next_token_stcp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,"%20s","mintime_beforemigr");
					goto next_token_stcp;
				}
				fprintf(fd,"%20s",stcp->t_or_d == 'h' && stcp->u1.h.mintime_beforemigr >= 0 ? u64tostr(stcp->u1.h.mintime_beforemigr,tmpbuf,20) : thena);
				/* ----------------------------------- */
			} else {
				stage_errmsg(NULL, STG02, "stageqry", "--format", q);
				if (! check_format_string) fprintf(fd,"\n");
				free(p);
				if (output_fd_toclean) fclose(output_fd);
				exit(USERR);
			}
		  next_token_stcp:
			dospace = 1;
			q = qnext;
		}
		if (! check_format_string) fprintf(fd,"\n");
		free(p);
		if (! check_format_string) {
			if ((! noheader_flag) && (! doneheader)) {
				/* We did the header, we will now have to do the first line */
				dospace = -1;
				doneheader = 1;
				goto redo_parse_stcp;
			}
		}
	}
	if (stpp != NULL ||  (check_format_string && ((flags & STAGE_LINKNAME) == STAGE_LINKNAME))) {
		/* format contain the list of members to print out */
		/* format is like: member1,member2,member3,...,membern */
		/* format can contain: */
	  redo_parse_stpp:
		if ((p = strdup(format)) == NULL) {
			stage_errmsg(NULL, STG02, "stageqry", "strdup", strerror(errno));
			if (output_fd_toclean) fclose(output_fd);
			exit(USERR);
		}
		
		q = strtok (p,",");
		while (q != NULL) {
			qnext = strtok(NULL, ",");
			if (dospace == 1 && ! check_format_string) fprintf(fd,thespace);
			/* ----------------------------------- */
			if (strcmp (q, "reqid") == 0) {
				if (check_format_string) goto next_token_stpp;
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					fprintf(fd,"%6s","reqid");
					goto next_token_stpp;
				}
				if (stpp->reqid) {
					fprintf(fd,"%6d",stpp->reqid);
				} else {
					fprintf(fd,"%6s",thena);
				}
				/* ----------------------------------- */
			} else if (strcmp (q, "upath") == 0) {
				int themax;
				if (check_format_string) goto next_token_stpp;
				themax = THEMAX((CA_MAXHOSTNAMELEN+MAXPATH),THEMAX(strlen("upath"),strlen(thena)));
				if ((! noheader_flag) && (! doneheader)) {
					/* Do first the header */
					if (qnext) {
						fprintf(fd,"%-*s",themax,"upath");
					} else {
						fprintf(fd,"%s","upath");
					}
					goto next_token_stpp;
				}
				if (qnext) {
					fprintf(fd,"%-*s",themax,stpp->upath[0] != '\0' ? stpp->upath : thena);
				} else {
					fprintf(fd,"%s",stpp->upath[0] != '\0' ? stpp->upath : thena);
				}
				/* ----------------------------------- */
			} else {
				stage_errmsg(NULL, STG02, "stageqry", "--format", q);
				if (! check_format_string) fprintf(fd,"\n");
				free(p);
				if (output_fd_toclean) fclose(output_fd);
				exit(USERR);
			}
		  next_token_stpp:
			dospace = 1;
			q = qnext;
		}
		if (! check_format_string) fprintf(fd,"\n");
		free(p);
		if (! check_format_string) {
			if ((! noheader_flag) && (! doneheader)) {
				/* We did the header, we will now have to do the first line */
				dospace = -1;
				doneheader = 1;
				goto redo_parse_stpp;
			}
		}
	}
    
	if (check_format_string) {
		check_format_string = 0;
	}
}

