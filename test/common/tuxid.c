/*
 * $Id: tuxid.c,v 1.4 2001/05/23 11:16:21 jdurand Exp $
 */

/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: tuxid.c,v $ $Revision: 1.4 $ $Date: 2001/05/23 11:16:21 $ CERN IT-PDP/DM Jean-Philippe Baud, Jean-Damien Durand";
#endif /* not lint */

#define _POSIX_
#include <stdio.h>
#ifndef _WIN32
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#endif
#include <errno.h>
#include "grp.h"
#include "pwd.h"
#include "Cgetopt.h"
#include "osdep.h"
#include "serrno.h"
#include "Castor_limits.h"

static int help_flag = 0;
static int pwnam_flag = 0;
static int grnam_flag = 0;
static int uid_flag = 0;
static int verbose_flag = 0;
static int gid_flag = 0;
static int self_flag = 0;

void usage _PROTO((char *));

char *cuserid();

int
main(argc,argv)
	int argc;
	char **argv;
{
	struct group *gr;
	char logname[L_cuserid];
	int c, n;
	int errflg = 0;
	char *pwnam;
	int pwnam_done = 0;
	char *grnam;
	int grnam_done = 0;
	uid_t uid;
	int uid_done = 0;
	gid_t gid;
	int gid_done = 0;
	struct passwd *pw;
	static struct Coptions longopts[] =
	{
		{"grnam",   REQUIRED_ARGUMENT, &grnam_flag ,    1},
		{"gid",     REQUIRED_ARGUMENT, &gid_flag ,      1},
		{"help",    NO_ARGUMENT,       &help_flag ,     1},
		{"pwnam",   REQUIRED_ARGUMENT, &pwnam_flag ,    1},
		{"self",    NO_ARGUMENT,       &self_flag ,     1},
		{"uid",     REQUIRED_ARGUMENT, &uid_flag ,      1},
		{"verbose", NO_ARGUMENT,       &verbose_flag ,  1},
		{NULL,      0,                  NULL,           0}
	};

	Coptind = 1;
	Copterr = 1;
	while ((c = Cgetopt_long (argc, argv, "", longopts, NULL)) != -1) {
		switch (c) {
		case 0:
			if (help_flag) {
				usage(argv[0]);
				exit(0);
			}
			if (pwnam_flag && ! pwnam_done) {
              pwnam = Coptarg;
              pwnam_done++;
			}
			if (grnam_flag && ! grnam_done) {
              grnam = Coptarg;
              grnam_done++;
			}
			if (uid_flag && ! uid_done) {
              uid = atoi(Coptarg);
              uid_done++;
			}
			if (gid_flag && ! gid_done) {
              gid = atoi(Coptarg);
              gid_done++;
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
		usage (argv[0]);
		exit(1);
	}

    if (! (pwnam_flag || grnam_flag || uid_flag || gid_flag)) {
      /* Force the --self flag */
      self_flag++;
    }

    if (self_flag) {
      char *this;

      if (verbose_flag) {
        printf ("--- Self Test ---\n");
        fflush (stdout);
      }
      /* ============= */
      /* cuserid(NULL) */
      /* ============= */
      printf ("Testing cuserid(NULL)               ");
      fflush (stdout);
      this = cuserid(NULL);
      if (verbose_flag) {
        printf ("%s\n\tReturned \"%s\"\n",
                (this != NULL) ? "[  OK  ]" : "[FAILED]",
                (this != NULL) ? this : "(null)");
      } else {
        printf ("%s\n", (this != NULL) ? "[  OK  ]" : "[FAILED]");
      }
      fflush (stdout);

      /* ================ */
      /* cuserid(logname) */
      /* ================ */
      printf ("Testing cuserid(argument != NULL)   ");
      fflush (stdout);
      logname[0] = '\0';
      this = cuserid(logname);
      if (verbose_flag) {
        printf ("%s\n\tReturned \"%s\", argument \"%s\"\n",
                (this != NULL) ? "[  OK  ]" : "[FAILED]",
                (this != NULL) ? this : "(null)",
                (logname[0] == '\0') ? "(empty string)" : logname);
      } else {
        printf ("%s\n", (this != NULL) ? "[  OK  ]" : "[FAILED]");
      }
      fflush (stdout);

      if (this != NULL) {
        pwnam_flag++;
        pwnam = logname;
      }

      /* ======== */
      /* getuid() */
      /* ======== */
      printf ("Testing getuid()                    ");
      fflush (stdout);
      uid = getuid();
      if (verbose_flag) {
        printf ("%s\n\tReturned %d\n",
                (uid != CA_MAXUID) ? "[  OK  ]" : "[FAILED]",
                (int) uid);
      } else {
        printf ("%s\n", (uid != CA_MAXUID) ? "[  OK  ]" : "[FAILED]");
      }
      fflush (stdout);

      if (uid != CA_MAXUID) {
        uid_flag++;
      }

      /* ======== */
      /* getgid() */
      /* ======== */
      printf ("Testing getgid()                    ");
      fflush (stdout);
      gid = getgid();
      if (verbose_flag) {
        printf ("%s\n\tReturned %d\n",
                (gid != CA_MAXGID) ? "[  OK  ]" : "[FAILED]",
                (int) gid);
      } else {
        printf ("%s\n", (gid != CA_MAXGID) ? "[  OK  ]" : "[FAILED]");
      }
      fflush (stdout);

      if (gid != CA_MAXUID) {
        gid_flag++;
      }

    }

    if (pwnam_flag) {
      if (verbose_flag) {
        printf ("--- Login Name Test ---\n");
        fflush (stdout);
      }

      /* ============== */
      /* getpwnam(name) */
      /* ============== */
      printf ("Testing getpwnam(%-10s)        ", pwnam);
      fflush(stdout);
      pw = getpwnam (pwnam);
      if (verbose_flag) {
        printf ("%s\n\tReturned password structure address 0x%lx\n",
                (pw != NULL) ? "[  OK  ]" : "[FAILED]",
                (unsigned long) pw);
        fflush (stdout);
        if (pw != NULL) {
          printf ("\tpw->pw_name   = %s\n", pw->pw_name);
          fflush (stdout);
          printf ("\tpw->pw_passwd = %s\n", pw->pw_passwd);
          fflush (stdout);
          printf ("\tpw->pw_uid    = %d\n", (int) pw->pw_uid);
          fflush (stdout);
          printf ("\tpw->pw_gid    = %d\n", (int) pw->pw_gid);
          fflush (stdout);
          printf ("\tpw->pw_gecos  = %s\n", pw->pw_gecos);
          fflush (stdout);
          printf ("\tpw->pw_shell  = %s\n", pw->pw_shell);
          fflush (stdout);
        }
      } else {
        printf ("%s\n", (pw != NULL) ? "[  OK  ]" : "[FAILED]");
        fflush (stdout);
      }

    }

    if (uid_flag) {
      if (verbose_flag) {
        printf ("--- Uid Test ---\n");
        fflush (stdout);
      }

      /* ============= */
      /* getpwuid(uid) */
      /* ============= */
      printf ("Testing getpwuid(%-5d)             ", uid);
      fflush (stdout);
      pw = getpwuid (uid);
      if (verbose_flag) {
        printf ("%s\n\tReturned password structure address 0x%lx\n",
                (pw != NULL) ? "[  OK  ]" : "[FAILED]",
                (unsigned long) pw);
        fflush (stdout);
        if (pw != NULL) {
          printf ("\tpw->pw_name   = %s\n", pw->pw_name);
          fflush (stdout);
          printf ("\tpw->pw_passwd = %s\n", pw->pw_passwd);
          fflush (stdout);
          printf ("\tpw->pw_uid    = %d\n", (int) pw->pw_uid);
          fflush (stdout);
          printf ("\tpw->pw_gid    = %d\n", (int) pw->pw_gid);
          fflush (stdout);
          printf ("\tpw->pw_gecos  = %s\n", pw->pw_gecos);
          fflush (stdout);
          printf ("\tpw->pw_shell  = %s\n", pw->pw_shell);
          fflush (stdout);
        }
      } else {
        printf ("%s\n", (pw != NULL) ? "[  OK  ]" : "[FAILED]");
        fflush (stdout);
      }
    }

    if (gid_flag) {
      if (verbose_flag) {
        printf ("--- Gid Test ---\n");
        fflush (stdout);
      }

      /* ============= */
      /* getgrgid(gid) */
      /* ============= */
      printf ("Testing getgrgid(%-5d)             ", gid);
      fflush (stdout);
      gr = getgrgid (gid);
      if (verbose_flag) {
        printf ("%s\n\tReturned group structure address 0x%lx\n",
                (gr != NULL) ? "[  OK  ]" : "[FAILED]",
                (unsigned long) gr);
        fflush (stdout);
        if (gr != NULL) {
          printf ("\tgr->gr_name   = %s\n", gr->gr_name);
          fflush (stdout);
          printf ("\tgr->gr_passwd = %s\n", gr->gr_passwd);
          fflush (stdout);
          printf ("\tgr->gr_gid    = %d\n", (int) gr->gr_gid);
          fflush (stdout);
          n = 0;
          while (gr->gr_mem[n]) {
            printf ("\tgr_mem[%d]    = %s\n", n, gr->gr_mem[n]);
            fflush (stdout);
            ++n;
          }
        }
      } else {
        printf ("%s\n", (gr != NULL) ? "[  OK  ]" : "[FAILED]");
        fflush (stdout);
      }

      /* If we were executed because of the --self option, we force also the --grnam */
      /* Note that gr_name is temporary variable but immediately forgotten as soon as */
      /* it will be used - that's why we use grnam pointer safely here */
      grnam_flag++;
      grnam = gr->gr_name;
    }

    if (grnam_flag) {
      if (verbose_flag) {
        printf ("--- Group Name Test ---\n");
        fflush (stdout);
      }

      /* ============== */
      /* getgrnam(name) */
      /* ============== */
      printf ("Testing getgrnam(%-10s)        ", grnam);
      fflush(stdout);
      gr = getgrnam (grnam);
      if (verbose_flag) {
        printf ("%s\n\tReturned group structure address 0x%lx\n",
                (gr != NULL) ? "[  OK  ]" : "[FAILED]",
                (unsigned long) gr);
        fflush (stdout);
        if (gr != NULL) {
          printf ("\tgr->gr_name   = %s\n", gr->gr_name);
          fflush (stdout);
          printf ("\tgr->gr_passwd = %s\n", gr->gr_passwd);
          fflush (stdout);
          printf ("\tgr->gr_gid    = %d\n", (int) gr->gr_gid);
          fflush (stdout);
          n = 0;
          while (gr->gr_mem[n]) {
            printf ("\tgr_mem[%d]    = %s\n", n, gr->gr_mem[n]);
            fflush (stdout);
            ++n;
          }
        }
      } else {
        printf ("%s\n", (gr != NULL) ? "[  OK  ]" : "[FAILED]");
        fflush (stdout);
      }
    }
	exit (0);
}

void usage(progname)
     char *progname;
{
  printf("Usage: %s [options]\n"
          "where options are:\n"
          "  --grnam <grnam> Test getgrnam() (group entry name)\n"
          "  --gid   <gid>   Test getgrgid() (group entry id)\n"
          "  --help          This help\n"
          "  --pwnam <pwnam> Test getpwnam() (password entry name)\n"
          "  --self          Test everything using your local account\n"
          "  --uid   <uid>   Test getpwuid() (password entry uid)\n"
          "  --verbose       See values in return\n"
          "\n"
         "If no option is given, then self-test using your login name, uid and gid will be performed\n",
          progname);
}
