/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2001-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "Cinit.h"
#include "getconfent.h"
#include "marshall.h"
#include "net.h"
#include "rbtsubr_constants.h"
#include "rmc_constants.h"
#include "rmc_logit.h"
#include "rmc_procreq.h"
#include "rmc_sendrep.h"
#include "rmc_smcsubr.h"
#include "scsictl.h"
#include "serrno.h"
#include "Cdomainname.h"
#include <sys/types.h>
#include <sys/stat.h>
#include "rmc_send_scsi_cmd.h"

#define PATH_CONF "/etc/cta/cta-rmcd.conf"

/* Forward declaration */
static int rmc_getreq(const int s, int *const req_type, char *const req_data, char **const clienthost);
static void rmc_procreq(const int rpfd, const int req_type, char *const req_data, char *const clienthost);
static int rmc_dispatchRqstHandler(const int req_type, const struct rmc_srv_rqst_context *const rqst_context);
static void rmc_doit(const int rpfd);

/* extern globals */
int g_jid;
struct extended_robot_info g_extended_robot_info;

/* globals with file scope */
char g_localhost[CA_MAXHOSTNAMELEN+1];
int g_maxfds;

int rmc_main(const char *const robot)
{
	int c;
	char domainname[CA_MAXHOSTNAMELEN+1];
	struct sockaddr_in from;
	socklen_t fromlen = sizeof(from);
	const char *msgaddr;
	int on = 1;	/* for REUSEADDR */
	fd_set readfd, readmask;
	int s;
	struct sockaddr_in sin;
	struct smc_status smc_status;
	struct timeval timeval;
	const char* const func = "rmc_serv";

	g_jid = getpid();
	rmc_logit (func, "started\n");

	char localhost[CA_MAXHOSTNAMELEN+1];
	gethostname(localhost, CA_MAXHOSTNAMELEN+1);
	localhost[CA_MAXHOSTNAMELEN] = '\0';
	if(strchr(localhost, '.') != NULL) {
		strncpy(g_localhost, localhost, CA_MAXHOSTNAMELEN+1);
	} else {
		if(Cdomainname(domainname, sizeof(domainname)) < 0) {
			rmc_logit(func, "Unable to get domainname\n");
		}
		if(snprintf(g_localhost, CA_MAXHOSTNAMELEN+1, "%s.%s", localhost, domainname) != 0) {
			rmc_logit(func, "localhost.domainname exceeds maximum length\n");
		}
	}

	if(*robot == '\0') {
		rmc_logit(func, RMC06, "robot");
		exit(USERR);
	}

	g_extended_robot_info.smc_ldr[CA_MAXRBTNAMELEN] = '\0';
	if(*robot == '/') {
		strncpy(g_extended_robot_info.smc_ldr, robot, CA_MAXRBTNAMELEN+1);
        } else {
		snprintf(g_extended_robot_info.smc_ldr, CA_MAXRBTNAMELEN+1, "/dev/%s", robot);
	}
	if(g_extended_robot_info.smc_ldr[CA_MAXRBTNAMELEN] != '\0') {
		rmc_logit(func, RMC06, "robot");
		exit(USERR);
	}
	g_extended_robot_info.smc_fd = -1;

	/* get robot geometry */
	{
		const int max_nb_attempts = 3;
		int attempt_nb = 1;
		for(attempt_nb = 1; attempt_nb <= max_nb_attempts;
                        attempt_nb++) {
                        rmc_logit (func,
                                "Trying to get geometry of tape library"
                                ": attempt_nb=%d\n", attempt_nb);
			c = smc_get_geometry (g_extended_robot_info.smc_fd,
				g_extended_robot_info.smc_ldr,
				&g_extended_robot_info.robot_info);

			if(0 == c) {
                                rmc_logit (func,
                                         "Got geometry of tape library\n");
				break;
			}

			c = smc_lasterror (&smc_status, &msgaddr);
			rmc_logit (func, RMC02, "get_geometry", msgaddr);

                        // If this was the last attempt
			if(max_nb_attempts == attempt_nb) {
				exit(c);
			} else {
                                sleep(1);
                        }
		}
	}

	FD_ZERO (&readmask);
	FD_ZERO (&readfd);
	signal (SIGPIPE, SIG_IGN);
	signal (SIGXFSZ, SIG_IGN);

	/* open request socket */

	if ((s = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
		rmc_logit (func, RMC02, "socket", neterror());
		exit (CONFERR);
	}
	memset ((char *)&sin, 0, sizeof(struct sockaddr_in)) ;
	sin.sin_family = AF_INET ;
	{
		const char *p;
		if ((p = getenv ("RMC_PORT")) || (p = getconfent_fromfile (PATH_CONF,"RMC", "PORT", 0))) {
			sin.sin_port = htons ((unsigned short)atoi (p));
		} else {
			sin.sin_port = htons ((unsigned short)RMC_PORT);
		}
	}
	// rmcd should only accept connections from the loopback interface
	sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	if (setsockopt (s, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0) {
		rmc_logit (func, RMC02, "setsockopt", neterror());
        }
	if (bind (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
		rmc_logit (func, RMC02, "bind", neterror());
		exit (CONFERR);
	}
	listen (s, 5) ;

	FD_SET (s, &readmask);

		/* main loop */

	while (1) {
		if (FD_ISSET (s, &readfd)) {
			FD_CLR (s, &readfd);
			const int rpfd =
				accept (s, (struct sockaddr *) &from, &fromlen);
			(void) rmc_doit (rpfd);
		}
		memcpy (&readfd, &readmask, sizeof(readmask));
		timeval.tv_sec = RMC_CHECKI;
		timeval.tv_usec = 0;
		if (select (g_maxfds, &readfd, (fd_set *)0, (fd_set *)0, &timeval) < 0) {
			FD_ZERO (&readfd);
		}
	}
}

/**
 * Returns 1 if the rmcd daemon should run in the background or 0 if the
 * daemon should run in the foreground.
 */
static int run_rmcd_in_background(const int argc, char **argv) {
  int i = 0;
  for(i = 1; i < argc; i++) {
    if(0 == strcmp(argv[i], "-f")) {
      return 0;
    }
  }

  return 1;
}

/**
 * Returns the number of command-line arguments that start with '-'.
 */
static int get_nb_cmdline_options(const int argc, char **argv) {
  int nbOptions = 0;
  for(int i = 1; i < argc; i++) {
    if(*argv[i] == '-') {
      nbOptions++;
    }
  }
  return nbOptions;
}

int main(const int argc, char **argv)
{
	const char *robot = "";
	const int nb_cmdline_options = get_nb_cmdline_options(argc, argv);

	switch(argc) {
	case 1:
		fprintf(stderr, "RMC01 - wrong arguments given ,specify the device file of the tape library\n");
		exit (USERR);
	case 2:
		if(0 == nb_cmdline_options) {
			robot = argv[1];
		} else {
			fprintf(stderr, "RMC01 - robot parameter is mandatory\n");
			exit (USERR);
		}
		break;
	case 3:
		if(0 == nb_cmdline_options) {
			fprintf(stderr, "Too many robot parameters\n");
			exit (USERR);
		} else if(2 == nb_cmdline_options) {
			fprintf(stderr, "RMC01 - robot parameter is mandatory\n");
			exit (USERR);
		/* At this point there is one argument starting with '-' */
		} else if(0 == strcmp(argv[1], "-f")) {
			robot = argv[2];
		} else if(0 == strcmp(argv[2], "-f")) {
			robot = argv[1];
		} else {
			fprintf(stderr, "Unknown option\n");
			exit (USERR);
		}
		break;
	default:
		fprintf(stderr, "Too many command-line arguments\n");
		exit (USERR);
	}

	if(run_rmcd_in_background(argc, argv)) {
		if ((g_maxfds = Cinitdaemon ("rmcd", NULL)) < 0) {
			exit (SYERR);
		}
	}
	exit (rmc_main (robot));
}

static void rmc_doit(const int rpfd)
{
	int c;
	char *clienthost;
	char req_data[RMC_REQBUFSZ-3*LONGSIZE];
	int req_type = 0;

	if ((c = rmc_getreq (rpfd, &req_type, req_data, &clienthost)) == 0)
		rmc_procreq (rpfd, req_type, req_data, clienthost);
	else if (c > 0)
		rmc_sendrep (rpfd, RMC_RC, c);
	else
		close (rpfd);
}

static int rmc_getreq(
  const int s,
  int *const req_type,
  char *const req_data,
  char **const clienthost)
{
	struct sockaddr_in from;
	socklen_t fromlen = sizeof(from);
	struct hostent *hp;
	int l;
	int magic;
	int msglen;
	int n;
	char *rbp;
	char req_hdr[3*LONGSIZE];
	const char* const func = "rmc_getreq";

	l = netread_timeout (s, req_hdr, sizeof(req_hdr), RMC_TIMEOUT);
	if (l == sizeof(req_hdr)) {
		rbp = req_hdr;
		unmarshall_LONG (rbp, magic);
		unmarshall_LONG (rbp, n);
		*req_type = n;
		unmarshall_LONG (rbp, msglen);
		if (msglen > RMC_REQBUFSZ) {
			rmc_logit (func, RMC46, RMC_REQBUFSZ);
			return (-1);
		}
		l = msglen - sizeof(req_hdr);
		n = netread_timeout (s, req_data, l, RMC_TIMEOUT);
		if (getpeername (s, (struct sockaddr *) &from, &fromlen) < 0) {
			rmc_logit (func, RMC02, "getpeername", neterror());
			return (ERMCUNREC);
		}
		hp = gethostbyaddr ((char *)(&from.sin_addr),
			sizeof(struct in_addr), from.sin_family);
		if (hp == NULL)
			*clienthost = inet_ntoa (from.sin_addr);
		else
			*clienthost = hp->h_name ;
		return (0);
	} else {
		if (l > 0) {
			rmc_logit (func, RMC04, l);
		} else if (l < 0) {
			rmc_logit (func, RMC02, "netread", sstrerror(serrno));
                }
		return (ERMCUNREC);
	}
}

static void rmc_procreq(
  const int rpfd,
  const int req_type,
  char *const req_data,
  char *const clienthost)
{
	struct rmc_srv_rqst_context rqst_context;

	rqst_context.localhost = g_localhost;
	rqst_context.rpfd = rpfd;
	rqst_context.req_data = req_data;
	rqst_context.clienthost = clienthost;

	const int handlerRc = rmc_dispatchRqstHandler(req_type, &rqst_context);

	if(ERMCUNREC == handlerRc) {
		rmc_sendrep (rpfd, MSG_ERR, RMC03, req_type);
	}
	rmc_sendrep (rpfd, RMC_RC, handlerRc);
}

/**
 * Dispatches the appropriate request handler.
 *
 * @param req_type The type of the request to be handled.
 * @param rqst_context The context of the request.
 * @return The result of handling the request.
 */
static int rmc_dispatchRqstHandler(const int req_type,
	const struct rmc_srv_rqst_context *const rqst_context) {
	switch (req_type) {
	case RMC_MOUNT:
		return rmc_srv_mount (rqst_context);
	case RMC_UNMOUNT:
		return rmc_srv_unmount (rqst_context);
	case RMC_EXPORT:
		return rmc_srv_export (rqst_context);
	case RMC_IMPORT:
		return rmc_srv_import (rqst_context);
	case RMC_GETGEOM:
		return rmc_srv_getgeom (rqst_context);
	case RMC_READELEM:
		return rmc_srv_readelem (rqst_context);
	case RMC_FINDCART:
		return rmc_srv_findcart (rqst_context);
	default:
		return ERMCUNREC;
	}
}
