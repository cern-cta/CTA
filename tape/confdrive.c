/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: confdrive.c,v $ $Revision: 1.7 $ $Date: 2007/03/13 16:22:42 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#if defined(_AIX) && defined(RS6000PCTA)
#include <sys/ioctl.h>
#include <sys/mtio.h>
#endif
#if defined(ADSTAR)
#include <sys/Atape.h>
#endif
#include "Ctape.h"
#include "Ctape_api.h"
#if SACCT
#include "sacct.h"
#include "serrno.h"
#endif
#if VDQM
#include "net.h"
#include "vdqm_api.h"
#endif
#include <unistd.h>
#include "tplogger_api.h"
int jid;
int main(argc, argv)
int	argc;
char	**argv;
{
	int c;
	char *dgn;
	char *drive;
	char *dvn;
	char *dvrname;
	char func[16];
	gid_t gid;
	int reason;
	int rpfd;
	int status;
	int tapefd;
	uid_t uid;
	int vdqm_rc;
	int vdqm_status;

	ENTRY (confdrive);

        tl_init_handle( &tl_tpdaemon, "dlf" );
        tl_tpdaemon.tl_init( &tl_tpdaemon, 0 );

	drive = argv[1];
	dvn = argv[2];
	rpfd = atoi (argv[3]);
	uid = atoi (argv[4]);
	gid = atoi (argv[5]);
	jid = atoi (argv[6]);
	dgn = argv[7];
	dvrname = argv[8];
	status = atoi (argv[9]);
	reason = atoi (argv[10]);

	c = 0;
	if (status == CONF_UP) {
		tapefd = open (dvn, O_RDONLY|O_NDELAY);
		if (tapefd < 0 &&
		    (errno == ENOENT || errno == ENXIO || errno == EBUSY ||
		     errno == ENODEV ||
		    (strcmp (dvrname, "Atape") == 0 && errno == EIO))) {
			c = errno;
		} else {
			if (tapefd >= 0) {
#if defined(_AIX) && defined(RS6000PCTA)
				if (strncmp (dvrname, "mtdd", 4) == 0)
					if (ioctl (tapefd, MTASSIGN, 0) < 0)
						c = errno;
#endif
#if defined(_AIX) && defined(ADSTAR)
				if (strcmp (dvrname, "Atape") == 0)
					if (ioctl (tapefd, SIOC_RESERVE, 0) < 0)
						c = errno;
#endif
				close (tapefd);
			}
		}
#if SACCT
		if (c == 0)
			tapeacct (TPCONFUP, 0, 0, jid, dgn, drive, "", 0, reason);
#endif
	} else {
#if defined(_AIX) && defined(RS6000PCTA)
		if (strncmp (dvrname, "mtdd", 4) == 0)
			if ((tapefd = open (dvn, O_RDONLY|O_NDELAY)) >= 0) {
				ioctl (tapefd, MTUNASSIGN, 0);
				close (tapefd);
			}
#endif
#if defined(ADSTAR)
		if (strcmp (dvrname, "Atape") == 0)
			if ((tapefd = open (dvn, O_RDONLY|O_NDELAY)) >= 0) {
				ioctl (tapefd, SIOC_RELEASE, 0);
				close (tapefd);
			}
#endif
#if SACCT
		tapeacct (TPCONFDN, 0, 0, jid, dgn, drive, "", 0, reason);
#endif
	}
#if VDQM
	if (c == 0) {
		vdqm_status = (status == CONF_UP) ? VDQM_UNIT_UP : VDQM_UNIT_DOWN;
		tplogit (func, "calling vdqm_UnitStatus\n");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 2,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, "calling vdqm_UnitStatus" );
		while ((vdqm_rc = vdqm_UnitStatus (NULL, NULL, dgn, NULL, drive,
			&vdqm_status, NULL, 0)) &&
			(serrno == SECOMERR || serrno == EVQHOLD))
				sleep (60);
		tplogit (func, "vdqm_UnitStatus returned %s\n",
			vdqm_rc ? sstrerror(serrno) : "ok");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 3,
                                    "func",    TL_MSG_PARAM_STR, func, 
                                    "Message", TL_MSG_PARAM_STR, "vdqm_UnitStatus returned",
                                    "Error",   TL_MSG_PARAM_STR, vdqm_rc ? sstrerror(serrno) : "ok");
	}
#endif
	if (rpfd >= 0)
		sendrep (rpfd, TAPERC, c);

        tl_tpdaemon.tl_exit( &tl_tpdaemon, 0 );        
	exit (c);
}
