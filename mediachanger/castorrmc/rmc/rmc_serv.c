/*
 * Copyright (C) 2001-2002 by CERN/IT/PDP/DM
 * All rights reserved
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
#include "h/Cinit.h"
#include "h/getconfent.h"
#include "h/marshall.h"
#include "h/net.h"
#include "h/rbtsubr_constants.h"
#include "h/rmc_constants.h"
#include "h/rmc_logit.h"
#include "h/rmc_procreq.h"
#include "h/rmc_sendrep.h"
#include "h/rmc_smcsubr.h"
#include "h/scsictl.h"
#include "h/serrno.h"
#include "h/Cdomainname.h"
#include <sys/types.h>
#include <sys/stat.h>
#include "h/rmc_send_scsi_cmd.h"

/* Forward declaration */
static int getreq(const int s, int *const req_type, char *const req_data,
  char **const clienthost);
static void procreq(const int rpfd, const int req_type, char *const req_data,
  char *const clienthost);
static int dispatchRqstHandlerWithFastRetry(const int req_type,
  const struct rmc_srv_rqst_context *const rqst_context,
  const unsigned int maxNbAttempts, const unsigned int delayInSec);
static const char *rmc_req_type_to_str(const int req_type);
static int dispatchRqstHandler(const int req_type,
  const struct rmc_srv_rqst_context *const rqst_context);
static void rmc_doit(const int rpfd);

int jid;
char localhost[CA_MAXHOSTNAMELEN+1];
int maxfds;
struct extended_robot_info extended_robot_info;

int rmc_main(const char *const robot)
{
	int c;
	unsigned char cdb[12];
	char domainname[CA_MAXHOSTNAMELEN+1];
	struct sockaddr_in from;
	socklen_t fromlen = sizeof(from);
	char *msgaddr;
	int nb_sense_ret;
	int on = 1;	/* for REUSEADDR */
	char plist[40];
	fd_set readfd, readmask;
	int s;
	char sense[MAXSENSE];
	struct sockaddr_in sin;
	struct smc_status smc_status;
	struct timeval timeval;
	char func[16];

	strncpy (func, "rmc_serv", sizeof(func));
	func[sizeof(func) - 1] = '\0';

	jid = getpid();
	rmc_logit (func, "started\n");
        
	gethostname (localhost, CA_MAXHOSTNAMELEN+1);
	if (strchr (localhost, '.') == NULL) {
		if (Cdomainname (domainname, sizeof(domainname)) < 0) {
			rmc_logit (func, "Unable to get domainname\n");
		}
		strcat (localhost, ".");
		strcat (localhost, domainname);
	}

	if (*robot == '\0' ||
	    (strlen (robot) + (*robot == '/') ? 0 : 5) > CA_MAXRBTNAMELEN) {
		rmc_logit (func, RMC06, "robot");
		exit (USERR);
	}
	if (*robot == '/')
		strcpy (extended_robot_info.smc_ldr, robot);
	else
		sprintf (extended_robot_info.smc_ldr, "/dev/%s", robot);

	extended_robot_info.smc_fd = -1;

	/* get robot geometry */
	{
		const int max_nb_attempts = 3;
		int attempt_nb = 1;
		for(attempt_nb = 1; attempt_nb <= max_nb_attempts;
                        attempt_nb++) {
                        rmc_logit (func,
                                "Trying to get geometry of tape library"
                                ": attempt_nb=%d\n", attempt_nb);
			c = smc_get_geometry (extended_robot_info.smc_fd,
				extended_robot_info.smc_ldr,
				&extended_robot_info.robot_info);

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

	/* check if robot support Volume Tag */

	extended_robot_info.smc_support_voltag = 1;
	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0xB6;		/* send volume tag */
	cdb[5] = 5;
	cdb[9] = 40;
	memset (plist, 0, sizeof(plist));
	strcpy (plist, "DUMMY0");
	c = rmc_send_scsi_cmd (extended_robot_info.smc_fd,
                     extended_robot_info.smc_ldr, 0, cdb, 12, (unsigned char*)plist, 40,
	    sense, 38, SCSI_OUT, &nb_sense_ret, &msgaddr);
	if (c < 0) {
		if (c == -4 && nb_sense_ret >= 14 && (sense[2] & 0xF) == 5 &&
		    sense[12] == 0x20) {
			extended_robot_info.smc_support_voltag = 0;
		} else {
			rmc_logit (func, RMC02, "find_cartridge", msgaddr);
			exit (SYERR);
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
		if ((p = getenv ("RMC_PORT")) || (p = getconfent ("RMC", "PORT", 0))) {
			sin.sin_port = htons ((unsigned short)atoi (p));
		} else {
			sin.sin_port = htons ((unsigned short)RMC_PORT);
		}
	}
	sin.sin_addr.s_addr = htonl(INADDR_ANY);
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
		if (select (maxfds, &readfd, (fd_set *)0, (fd_set *)0, &timeval) < 0) {
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
 * Returns 1 if the specified command-line argument start with '-' else
 * returns 0.
 */
static int cmdline_arg_is_an_option(const char *arg) {
  if(strlen(arg) < 1) {
    return 0;
  } else {
    if('-' == arg[0]) {
      return 1;
    } else {
      return 0;
    }
  }
}

/**
 * Returns the number of command-line arguments that start with '-'.
 */
static int get_nb_cmdline_options(const int argc, char **argv) {
  int i = 0;
  int nbOptions = 0;

  for(i = 1; i < argc; i++) {
    if(cmdline_arg_is_an_option(argv[i])) {
      nbOptions++;
    }
  }

  return nbOptions;
}

int main(const int argc,
         char **argv)
{
	const char *robot = "";
	const int nb_cmdline_options = get_nb_cmdline_options(argc, argv);

	switch(argc) {
	case 1:
		fprintf(stderr, "RMC01 - robot parameter is mandatory\n");
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
		if ((maxfds = Cinitdaemon ("rmcd", NULL)) < 0) {
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

	if ((c = getreq (rpfd, &req_type, req_data, &clienthost)) == 0)
		procreq (rpfd, req_type, req_data, clienthost);
	else if (c > 0)
		rmc_sendrep (rpfd, RMC_RC, c);
	else
		close (rpfd);
}

static int getreq(
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
	char func[16];

	strncpy (func, "rmc_serv", sizeof(func));
	func[sizeof(func) - 1] = '\0';

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
			rmc_logit (func, RMC02, "netread", strerror(errno));
                }
		return (ERMCUNREC);
	}
}

static void procreq(
  const int rpfd,
  const int req_type,
  char *const req_data,
  char *const clienthost)
{
	struct rmc_srv_rqst_context rqst_context;

	rqst_context.localhost = localhost;
	rqst_context.rpfd = rpfd;
	rqst_context.req_data = req_data;
	rqst_context.clienthost = clienthost;

	const unsigned int maxNbAttempts = 3;
	const unsigned int delayInSec = 1;
	const int handlerRc = dispatchRqstHandlerWithFastRetry(req_type,
		&rqst_context, maxNbAttempts, delayInSec);

	if(ERMCUNREC == handlerRc) {
		rmc_sendrep (rpfd, MSG_ERR, RMC03, req_type);
	}
	rmc_sendrep (rpfd, RMC_RC, handlerRc);
}

/**
 * Dispatches the appropriate request handler in a loop while the result is
 * RBT_FAST_RETRY until the specified maximum number of attempts has been
 * reached.
 *
 * @param req_type The type of the request to be handled.
 * @param rqst_context The context of the request.
 * @param maxNbAttempts The maximum number of attempts.
 * @param delayInSec The delay in seconds between attempts.
 * @return The result of handling the request.
 */
static int dispatchRqstHandlerWithFastRetry(const int req_type,
  const struct rmc_srv_rqst_context *const rqst_context,
  const unsigned int maxNbAttempts, const unsigned int delayInSec) {
  unsigned int attemptNb = 1;
  const char *const req_type_str = rmc_req_type_to_str(req_type);
  char func[16];

  strncpy (func, "dispatch", sizeof(func));
  func[sizeof(func) - 1] = '\0';

  for(attemptNb = 1; attemptNb <= maxNbAttempts; attemptNb++) {
    // If this is not the first attempt then this is a fast retry
    if(attemptNb > 1) {
      rmc_logit(func, "Sleeping before fast retry: delayInSec=%u"
        " req_type=%d req_type_str=%s attemptNb=%u maxNbAttempts=%u\n",
        delayInSec, req_type, req_type_str, attemptNb, maxNbAttempts);
      sleep(delayInSec);
    }

    rmc_logit(func, "Dispatching request handler: req_type=%d req_type_str=%s"
      " attemptNb=%u maxNbAttempts=%u\n",
      req_type, req_type_str, attemptNb, maxNbAttempts);
    const int handlerRc = dispatchRqstHandler(req_type, rqst_context);

    if(ERMCFASTR != handlerRc) {
      return handlerRc;
    }
  }

  // The maximum number of attempts has been reached
  rmc_logit(func, "Maximum number of attempts reached"
    ": req_type=%d req_type_str=%s maxNbAttempts=%u\n",
    req_type, req_type_str, maxNbAttempts);
  return ERMCUNREC;
}

/**
 * Thread safe function that returns the string representation of the specified
 * RMC request-type.
 *
 * If the request type is unknown then "UNKNOWN" is returned.
 *
 * @param req_type The request type as an integer.
 * @return The string representation.
 */
static const char *rmc_req_type_to_str(const int req_type) {
	switch (req_type) {
	case RMC_MOUNT   : return "RMC_MOUNT";
	case RMC_UNMOUNT : return "RMC_UNMOUNT";
	case RMC_EXPORT  : return "RMC_EXPORT";
	case RMC_IMPORT  : return "RMC_IMPORT";
	case RMC_GETGEOM : return "RMC_GETGEOM";
	case RMC_READELEM: return "RMC_READELEM";
	case RMC_FINDCART: return "RMC_FINDCART";
	default          : return "UNKNOWN";
	}
}

/**
 * Dispatches the appropriate request handler.
 *
 * @param req_type The type of the request to be handled.
 * @param rqst_context The context of the request.
 * @return The result of handling the request.
 */
static int dispatchRqstHandler(const int req_type,
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
