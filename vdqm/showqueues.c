/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: showqueues.c,v $ $Revision: 1.8 $ $Date: 2000/06/26 13:00:15 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * showqueues.c - command to list queues and running jobs.
 */

#include <stdio.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif /* _WIN32 */
#include <regex.h>
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
char strftime_format[] = "%b %d %H:%M:%S";

int main(int argc, char *argv[]) {
    int rc, c, last_id;
    int give_jid = 1;
    time_t now;
    struct tm *tp;
    char    timestr[64] ;   /* Time in its ASCII format             */

    struct vdqm_reqlist {
        vdqmVolReq_t volreq;
        vdqmDrvReq_t drvreq;
        struct vdqm_reqlist *next;
        struct vdqm_reqlist *prev;
    } *reqlist = NULL;
    struct vdqm_reqlist *tmp, *tmp1;
    char drv_status[10];
    vdqmnw_t *nw = NULL;
    char dgn[CA_MAXDGNLEN+1];
    char server[CA_MAXHOSTNAMELEN+1];
    extern char * optarg ; 
    extern int    optind ;


    *dgn = *server = '\0';
    while ( (c = getopt(argc,argv,"jg:S:")) != EOF ) {
        switch(c) {
        case 'j':
            give_jid = 0;
            break;
        case 'g' :
            strcpy(dgn,optarg);
            break;
        case 'S' :
            strcpy(server,optarg);
            break;
        case '?':
            fprintf(stderr,"Usage: %s [-j] [-g <dgn>] [-S <server>]\n",argv[0]);
            exit(2);
        }
    }

    /*
     * Get drive status
     */
    rc = 0;
    tmp = NULL;
    last_id = 0;
    do {
       if ( tmp == NULL ) 
           tmp = (struct vdqm_reqlist *)calloc(1,sizeof(struct vdqm_reqlist));
       strcpy(tmp->drvreq.dgn,dgn);
       if ( *server != '\0' ) strcpy(tmp->drvreq.server,server); 
       rc = vdqm_NextDrive(&nw,&tmp->drvreq);
       if ( rc != -1 && *tmp->drvreq.server != '\0' && *tmp->drvreq.drive != '\0' && (tmp->drvreq.DrvReqID != last_id) ) {
           CLIST_INSERT(reqlist,tmp); 
           last_id = tmp->drvreq.DrvReqID;
           tmp = NULL;
       }
    } while (rc != -1);

    nw = NULL;
    /*
     * Get volume queue
     */
    rc = 0;
    tmp = NULL;
    last_id = 0;
    do {
       if ( tmp == NULL )
           tmp = (struct vdqm_reqlist *)calloc(1,sizeof(struct vdqm_reqlist));
       strcpy(tmp->volreq.dgn,dgn);
       if ( *server != '\0' ) strcpy(tmp->volreq.server,server);
       rc = vdqm_NextVol(&nw,&tmp->volreq);
       if ( rc != -1 && tmp->volreq.VolReqID > 0 && 
            (tmp->volreq.VolReqID != last_id) ) {
           last_id = tmp->volreq.VolReqID;
           CLIST_ITERATE_BEGIN(reqlist,tmp1) {
               if ( tmp->volreq.VolReqID == tmp1->drvreq.VolReqID ) {
                   tmp1->volreq = tmp->volreq;
                   break;
               }
           } CLIST_ITERATE_END(reqlist,tmp1); 
           if ( tmp->volreq.VolReqID != tmp1->drvreq.VolReqID ) {
               CLIST_INSERT(reqlist,tmp);
               tmp = NULL;
           }
       }
    } while (rc != -1);

    /*
     * Print the queues.
     */
    (void)time(&now);

    CLIST_ITERATE_BEGIN(reqlist,tmp1) {
        if ( tmp1->drvreq.VolReqID > 0 ) {
            tp = localtime((time_t *)&tmp1->volreq.recvtime);
            (void)strftime(timestr,64,strftime_format,tp);
            if ( tmp1->drvreq.status == 
                 (VDQM_UNIT_UP|VDQM_UNIT_ASSIGN|VDQM_UNIT_BUSY) ) {
                fprintf(stdout,"%s@%s (%d MB) jid %d %s(%s) user (%d,%d) %d secs.\n",
                    tmp1->drvreq.drive,tmp1->drvreq.server,
                    (int)tmp1->drvreq.TotalMB,
                    (give_jid==1 ? tmp1->drvreq.jobID : tmp1->drvreq.VolReqID),
                    tmp1->volreq.volid,
                    (tmp1->volreq.mode == 0 ? "read" : "write"),
                    tmp1->volreq.clientUID,tmp1->volreq.clientGID,
                    now - tmp1->drvreq.recvtime);
            } else if ( tmp1->drvreq.status == (VDQM_UNIT_UP|VDQM_UNIT_BUSY)) { 
                 fprintf(stdout,"%s@%s (%d MB) START ReqID %d %s(%s) user (%d,%d) %d secs.\n",
                    tmp1->drvreq.drive,tmp1->drvreq.server,
                    (int)tmp1->drvreq.TotalMB,
                    tmp1->drvreq.VolReqID,
                    tmp1->volreq.volid,
                    (tmp1->volreq.mode == 0 ? "read" : "write"),
                    tmp1->volreq.clientUID,tmp1->volreq.clientGID,
                    now - tmp1->drvreq.recvtime);
            } else if ( tmp1->drvreq.status == (VDQM_UNIT_UP|VDQM_UNIT_BUSY|VDQM_UNIT_RELEASE|VDQM_UNIT_UNKNOWN) ) {
                 fprintf(stdout,"%s@%s (%d MB) RELEASE jid %d %s(%s) user (%d,%d) %d secs.\n",
                    tmp1->drvreq.drive,tmp1->drvreq.server,
                    (int)tmp1->drvreq.TotalMB,
                    (give_jid==1 ? tmp1->drvreq.jobID : tmp1->drvreq.VolReqID),
                    tmp1->volreq.volid,
                    (tmp1->volreq.mode == 0 ? "read" : "write"),
                    tmp1->volreq.clientUID,tmp1->volreq.clientGID,
                    now - tmp1->drvreq.recvtime);
            } else {
                fprintf(stdout,"%s@%s (%d MB) UNKNOWN jid %d %s(%s) user (%d,%d) %d secs.\n",
                    tmp1->drvreq.drive,tmp1->drvreq.server,
                    (int)tmp1->drvreq.TotalMB,
                    (give_jid==1 ? tmp1->drvreq.jobID : tmp1->drvreq.VolReqID),
                    tmp1->volreq.volid,
                    (tmp1->volreq.mode == 0 ? "read" : "write"),
                    tmp1->volreq.clientUID,tmp1->volreq.clientGID,
                    now - tmp1->drvreq.recvtime);
            }
        } else if ( *tmp1->drvreq.drive != '\0' ) {
            tp = localtime((time_t *)&tmp1->drvreq.recvtime);
            (void)strftime(timestr,64,strftime_format,tp);
            if ( tmp1->drvreq.status & VDQM_UNIT_DOWN ) 
                strcpy(drv_status,"DOWN");
            else if ( tmp1->drvreq.status == (VDQM_UNIT_UP|VDQM_UNIT_FREE) )
                strcpy(drv_status,"FREE");
            else if ( tmp1->drvreq.status == (VDQM_UNIT_UP |VDQM_UNIT_RELEASE |VDQM_UNIT_BUSY |VDQM_UNIT_UNKNOWN) )
                strcpy(drv_status,"RELEASE");
            else strcpy(drv_status,"UNKN"); 

            fprintf(stdout,"%s@%s (%d MB) %s vid: %s last update %s\n",
                    tmp1->drvreq.drive,tmp1->drvreq.server,
                    (int)tmp1->drvreq.TotalMB,drv_status,tmp1->drvreq.volid,
                    timestr);
            if ( *tmp1->drvreq.dedicate != 0 )
                fprintf(stdout,"Dedicated: %s\n",tmp1->drvreq.dedicate);
        } else {
            tp = localtime((time_t *)&tmp1->volreq.recvtime);
            (void)strftime(timestr,64,strftime_format,tp);
            fprintf(stdout,"QUEUED: %s ReqID: %d user (%d,%d)@%s received at %s\n",
                    tmp1->volreq.volid,
                    tmp1->volreq.VolReqID,
                    tmp1->volreq.clientUID,
                    tmp1->volreq.clientGID,
                    tmp1->volreq.client_host,timestr);
        }
    } CLIST_ITERATE_END(reqlist,tmp1);

    exit(0);
}
