/*
 * Copyright (C) 2000 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: tpsrv_in_prod.c,v $ $Revision: 1.3 $ $Date: 2003/04/17 08:19:12 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * tpsrv_in_prod.c - prod/development status for tape server.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif /* _WIN32 */
#include <time.h>
#include <Castor_limits.h>
#include <serrno.h>
#include <net.h>
#include <osdep.h>
#if !defined(VDQMSERV)
#define VDQMSERV
#endif /* VDQMSERV */
#include <vdqm_constants.h>
#include <vdqm.h>
#include <vdqm_api.h>
#include <Ctape_constants.h>

int main(int argc, char *argv[]) {
    int rc, c, last_id;
    int prod_dev = 0;

    struct vdqm_reqlist {
        vdqmDrvReq_t drvreq;
        struct vdqm_reqlist *next;
        struct vdqm_reqlist *prev;
    } *reqlist = NULL;
    struct vdqm_reqlist *tmp, *tmp1;
    vdqmnw_t *nw = NULL;
    char *p = NULL;
    char server[CA_MAXHOSTNAMELEN+1];
    char match[CA_MAXLINELEN+1];
    extern char * optarg ; 
    extern int    optind ;

    *server = '\0';
    while ( (c = getopt(argc,argv,"t:")) != EOF ) {
        switch(c) {
        case 't':
            if ( strncmp(optarg,"dev",3) == 0 ) prod_dev = 1;
            break;
        case '?':
            fprintf(stderr,"Usage: %s [-t [prod] [dev]]\n",argv[0]);
            exit(2);
        }
    }

    gethostname(server,CA_MAXHOSTNAMELEN);
    p = strchr(server,'.');
    if ( p != NULL ) *p = '\0';
    /*
     * Get drive status
     */
    rc = 0;
    tmp = NULL;
    last_id = 0;
    do {
       if ( tmp == NULL ) 
           tmp = (struct vdqm_reqlist *)calloc(1,sizeof(struct vdqm_reqlist));
       strcpy(tmp->drvreq.server,server); 
       rc = vdqm_NextDrive(&nw,&tmp->drvreq);
       if ( rc != -1 && *tmp->drvreq.server != '\0' && *tmp->drvreq.drive != '\0' && (tmp->drvreq.DrvReqID != last_id) ) {
           CLIST_INSERT(reqlist,tmp); 
           last_id = tmp->drvreq.DrvReqID;
           tmp = NULL;
       }
    } while (rc != -1);

    /*
     * Set or revoke dedicate of all drives on this server
     */

    CLIST_ITERATE_BEGIN(reqlist,tmp) {
        if ( strcmp(tmp->drvreq.server,server) == 0 ) {
            if ( prod_dev == 0 ) {
                sprintf(match,"host=%s",server);
                if ( strstr(tmp->drvreq.dedicate,match) == NULL ) {
                    fprintf(stderr,"*** WARNING drive %s@%s not in dev\n",
                            tmp->drvreq.drive,tmp->drvreq.server);
                } else {
                    *match = '\0'; 
                    fprintf(stdout,"Revoke server dedication of %s@%s\n",
                            tmp->drvreq.drive,tmp->drvreq.server);
                }
            } else { 
                if ( *tmp->drvreq.dedicate != '\0' ) {
                    fprintf(stderr,
                            "\n*** ERROR cannot reset exiting dedication: %s\n",
                            tmp->drvreq.dedicate);
                    exit(2);
                }
                sprintf(match,"host=%s",server);
                fprintf(stdout,"Dedicate %s@%s to %s\n",
                        tmp->drvreq.drive,tmp->drvreq.server,match);
            }
            if ( prod_dev == 1 || *match == '\0' ) {
                rc = vdqm_DedicateDrive(NULL,tmp->drvreq.dgn,
                                             tmp->drvreq.server,
                                             tmp->drvreq.drive,match);
                if ( rc == -1 ) {
                    fprintf(stderr,"vdqm_DedicateDrive(NULL,%s,%s,%s,%s)\n",
                            tmp->drvreq.dgn,tmp->drvreq.server,
                            tmp->drvreq.drive,match);
                }
            }
        }
    } CLIST_ITERATE_END(reqlist,tmp);

    exit(0);
}
