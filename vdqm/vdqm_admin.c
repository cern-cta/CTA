/*
 * Copyright (C) 2000 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vdqm_admin.c,v $ $Revision: 1.4 $ $Date: 2002/10/25 13:04:02 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * vdqm_admin.c - VDQM administration utility
 */

#include <stdio.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif /* _WIN32 */
#include <time.h>
#include <Castor_limits.h>
#include <serrno.h>
#include <net.h>
#include <osdep.h>
#include <vdqm_constants.h>
#include <vdqm.h>
#include <vdqm_api.h>

static void usage(char *cmd, char cmds[][20]) {
   int i;
   fprintf(stderr,"Usage: %s",cmd);
   for (i=0; *cmds[i] != '\0'; i++) {
       if ( *cmd != '-' ) fprintf(stderr," [%s]",cmds[i]);
       else fprintf(stderr," [%s value]",cmds[i]);
   }
   if ( *cmd != '-' ) fprintf(stderr," <cmd keywords>\n");
   else fprintf(stderr,"\n");
   return;
}

int main(int argc, char *argv[]) {
    int rc;
    int i,j,k;

    char cmds[][20] =  {"-ping",
                        "-shutdown",
                        "-hold",
                        "-release",
                        "-dedicate",
                        "-deldrv",
                        "-delvol",
                        ""};
    char keyw[][20] =  {"-dgn",
                        "-server",
                        "-drive",
                        "-reqid",
                        "-jobid",
                        "-match",
                        "-vid",
                        "-port",
                        ""};
    int icmds[] = {VDQM_PING,
                   VDQM_SHUTDOWN,
                   VDQM_HOLD,
                   VDQM_RELEASE,
                   VDQM_DEDICATE_DRV,
                   VDQM_DEL_DRVREQ,
                   VDQM_DEL_VOLREQ,
                   -1};
    char dgn[CA_MAXDGNLEN+1];
    char server[CA_MAXHOSTNAMELEN+1];
    char drive[CA_MAXUNMLEN+1];
    char match[CA_MAXLINELEN+1];
    char vid[CA_MAXVIDLEN+1];
    char *keyvalues[] = {NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL};
    int reqid,jobid,port;
    int *ikeyvalues[] = {NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL};

    keyvalues[0] = dgn;
    keyvalues[1] = server;
    keyvalues[2] = drive;
    ikeyvalues[3] = &reqid;
    ikeyvalues[4] = &jobid;
    keyvalues[5] = match;
    keyvalues[6] = vid;
    ikeyvalues[7] = &port;

    if ( argc > 1 ) for (i=0; *cmds[i] != '\0' && strcmp(cmds[i],argv[1]) != 0; i++);
    if ( argc <= 1 || icmds[i] == -1 ) {
        usage(argv[0],cmds);
        exit(2);
    }

    switch (icmds[i]) {
    case VDQM_SHUTDOWN:
    case VDQM_HOLD:
    case VDQM_RELEASE:
        if ( argc > 2 ) {
            usage(argv[0],cmds);
            exit(2);
        }
        rc = vdqm_admin(NULL,icmds[i]);
        break;
    case VDQM_PING:
    case VDQM_DEDICATE_DRV:
    case VDQM_DEL_VOLREQ:
    case VDQM_DEL_DRVREQ: 
        *dgn = *server = *drive = *match = '\0';
        reqid = jobid = 0;
        for (j=2; j<argc; j+=2) {
            for (k=0; *keyw[k] != '\0' && strcmp(keyw[k],argv[j]) != 0 ; k++);
            if ( *keyw[k] == '\0' ) {
                fprintf(stderr,"Missing keyword or syntax error\n");
                usage(cmds[i],keyw);
                exit(2);
            }
            if ( j+1 >= argc ) {
                fprintf(stderr,"Missing value for %s keyword\n",keyw[k]);
                usage(cmds[i],keyw);
                exit(2);
            }
            if ( keyvalues[k] != NULL && argv[j+1] != NULL ) 
                strcpy(keyvalues[k],argv[j+1]);
            if ( ikeyvalues[k] != NULL && argv[j+1] != NULL ) 
                *ikeyvalues[k] = atoi(argv[j+1]);
        }
        if ( icmds[i] == VDQM_PING ) {
            rc = vdqm_PingServer(NULL,dgn,reqid);
            if ( rc >= 0 ) {
                fprintf(stdout,"Current queue position for reqid %d is %d\n",
                        reqid,rc);
            } else if ( serrno != SECOMERR && serrno != EVQHOLD ) {
                fprintf(stdout,"%s\n","ALIVE");
                exit(0);
            } else if ( serrno == EVQHOLD ) {
                fprintf(stdout,"%s\n","HOLD");
                exit(0);
            } else {
                fprintf(stdout,"%s\n","DEAD");
                exit(1);
            } 
        } else if ( icmds[i] == VDQM_DEDICATE_DRV ) {
            if ( *match == '\0' ) {
                fprintf(stdout,"Revoke dedication of %s@%s\n",
                        drive,server);
            } else {
                fprintf(stdout,"Dedicate %s@%s to %s\n",
                        drive,server,match);
            }
            rc = vdqm_DedicateDrive(NULL,dgn,server,drive,match);
        } else if ( icmds[i] == VDQM_DEL_VOLREQ ) {
            rc = vdqm_DelVolumeReq(NULL,reqid,vid,dgn,server,drive,port);
        } else if ( icmds[i] == VDQM_DEL_DRVREQ ) {
            rc = vdqm_DelDrive(NULL,dgn,server,drive);
        }
        break;
    default:
        fprintf(stderr,"Unknown request\n");
        usage(argv[0],cmds);
        exit(2);
    }
    if ( rc == -1 ) {
        fprintf(stderr,"error: %s\n",sstrerror(serrno));
        exit(1);
    }

    exit(0);
}
