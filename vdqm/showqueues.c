/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: showqueues.c,v $ $Revision: 1.15 $ $Date: 2003/04/22 08:17:43 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * showqueues.c - command to list queues and running jobs.
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
#include "Cpwd.h"
#include "Cgrp.h"

#define BUF_SIZE     30
#define BUF_ID_SIZE   100
#define NO_DEDICATION "No_dedication"
#define NO_DED "ND"
#define DED    "DE"
#define SEP ','

char strftime_format[] = "%b %d %H:%M:%S";


struct vdqm_reqlist {
    vdqmVolReq_t volreq;
    vdqmDrvReq_t drvreq;
    struct vdqm_reqlist *next;
    struct vdqm_reqlist *prev; 
};


void shq_display_standard(struct vdqm_reqlist *list, int give_jid);
void shq_decode_status_DA(int status, char *buf, int buf_size);
void shq_decode_status_DN(int status, char *buf, int buf_size);
char *shq_getuname(uid_t uid);
char *shq_getgname(gid_t gid);
void shq_build_id_str(uid_t uid, gid_t gid, char *buf, int buf_size);
void shq_parse_dedication(char *dedication, char *reduced_dedication, int buf_size);



int main(int argc, char *argv[]) {
    int rc, c, last_id;
    int give_jid = 1;
    time_t now;
    struct tm *tp;
    char    timestr[64] ;   /* Time in its ASCII format             */

    struct vdqm_reqlist *reqlist = NULL;
    struct vdqm_reqlist *tmp, *tmp1;
    char drv_status[10];
    vdqmnw_t *nw = NULL;
    char dgn[CA_MAXDGNLEN+1];
    char server[CA_MAXHOSTNAMELEN+1];
    extern char * optarg ; 
    extern int    optind ;
    int std = 0;
    int display_dedication = 1;

    *dgn = *server = '\0';
    while ( (c = getopt(argc,argv,"jg:S:x")) != EOF ) {
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
        case 'x':
            std = 1;
            break;
        case 'd':
            display_dedication = 0;
            break;
        case '?':
            fprintf(stderr,"Usage: %s [-j] [-g <dgn>] [-S <server>] [-x]\n",argv[0]);
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
    if ( tmp != NULL ) free(tmp);

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
       memset(tmp,'\0',sizeof(tmp));
       strcpy(tmp->volreq.dgn,dgn);
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

/* Uses standard display if required by the option -s */
    if(std) {
        shq_display_standard(reqlist, give_jid);
        exit(0);
    }


    
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
                if ( *tmp1->drvreq.dedicate != 0 )
                    fprintf(stdout,"Dedicated: %s\n",tmp1->drvreq.dedicate);
            } else if ( tmp1->drvreq.status == (VDQM_UNIT_UP|VDQM_UNIT_BUSY)) { 
                 fprintf(stdout,"%s@%s (%d MB) START ReqID %d %s(%s) user (%d,%d) %d secs.\n",
                    tmp1->drvreq.drive,tmp1->drvreq.server,
                    (int)tmp1->drvreq.TotalMB,
                    tmp1->drvreq.VolReqID,
                    tmp1->volreq.volid,
                    (tmp1->volreq.mode == 0 ? "read" : "write"),
                    tmp1->volreq.clientUID,tmp1->volreq.clientGID,
                    now - tmp1->drvreq.recvtime);
                if ( *tmp1->drvreq.dedicate != 0 )
                    fprintf(stdout,"Dedicated: %s\n",tmp1->drvreq.dedicate);
            } else if ( tmp1->drvreq.status == (VDQM_UNIT_UP|VDQM_UNIT_BUSY|VDQM_UNIT_RELEASE|VDQM_UNIT_UNKNOWN) ) {
                 fprintf(stdout,"%s@%s (%d MB) RELEASE jid %d %s(%s) user (%d,%d) %d secs.\n",
                    tmp1->drvreq.drive,tmp1->drvreq.server,
                    (int)tmp1->drvreq.TotalMB,
                    (give_jid==1 ? tmp1->drvreq.jobID : tmp1->drvreq.VolReqID),
                    tmp1->volreq.volid,
                    (tmp1->volreq.mode == 0 ? "read" : "write"),
                    tmp1->volreq.clientUID,tmp1->volreq.clientGID,
                    now - tmp1->drvreq.recvtime);
                if ( *tmp1->drvreq.dedicate != 0 )
                    fprintf(stdout,"Dedicated: %s\n",tmp1->drvreq.dedicate);
            } else {
                fprintf(stdout,"%s@%s (%d MB) UNKNOWN jid %d %s(%s) user (%d,%d) %d secs.\n",
                    tmp1->drvreq.drive,tmp1->drvreq.server,
                    (int)tmp1->drvreq.TotalMB,
                    (give_jid==1 ? tmp1->drvreq.jobID : tmp1->drvreq.VolReqID),
                    tmp1->volreq.volid,
                    (tmp1->volreq.mode == 0 ? "read" : "write"),
                    tmp1->volreq.clientUID,tmp1->volreq.clientGID,
                    now - tmp1->drvreq.recvtime);
                if ( *tmp1->drvreq.dedicate != 0 )
                    fprintf(stdout,"Dedicated: %s\n",tmp1->drvreq.dedicate);
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
        } else if ( *server == '\0' || 
                    strcmp(server,tmp1->volreq.server) == 0 ) {
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


/**
 * Displays the list of drives/jobs in the queue in a systematic way, easier to
 * parse with regexp.
 */
void shq_display_standard(struct vdqm_reqlist *reqlist, int give_jid) {

    struct vdqm_reqlist *tmp1;
    time_t now;
    struct tm *tp;
    char timestr[64] ;   /* Time in its ASCII format */
    char drv_status[10];
    char server[CA_MAXHOSTNAMELEN+1];

    (void)time(&now);

    server[0] = '\0';
    CLIST_ITERATE_BEGIN(reqlist,tmp1) {
        if ( tmp1->drvreq.VolReqID > 0 ) {
            char buf[BUF_SIZE];
            char buf_id[BUF_ID_SIZE];
            char buf_ded[BUF_ID_SIZE];
            
            tp = localtime((time_t *)&tmp1->volreq.recvtime);
            (void)strftime(timestr,64,strftime_format,tp);

            shq_decode_status_DA(tmp1->drvreq.status, buf, BUF_SIZE);
            shq_build_id_str(tmp1->volreq.clientUID,
                             tmp1->volreq.clientGID,
                             buf_id,
                             BUF_ID_SIZE);
            shq_parse_dedication(tmp1->drvreq.dedicate, buf_ded, BUF_ID_SIZE);
            
            fprintf(stdout,"DA %s %s@%s %s %d (%s) %s %s %d (%s)@%s\n",
                    tmp1->drvreq.dgn,
                    tmp1->drvreq.drive,
                    tmp1->drvreq.server,
                    buf,
                    now - tmp1->drvreq.recvtime,
                    (*tmp1->drvreq.dedicate != 0)?buf_ded:NO_DEDICATION,
                    tmp1->volreq.volid,
                    (tmp1->volreq.mode == 0 ? "R" : "W"),
                    (give_jid==1 ? tmp1->drvreq.jobID : tmp1->drvreq.VolReqID),
                    buf_id,
                    tmp1->volreq.client_host);


        } else if ( *tmp1->drvreq.drive != '\0' ) {

            char buf[BUF_SIZE];
            char buf_ded[BUF_ID_SIZE];
            
            tp = localtime((time_t *)&tmp1->drvreq.recvtime);
            (void)strftime(timestr,64,strftime_format,tp);

            shq_decode_status_DN(tmp1->drvreq.status, buf, BUF_SIZE);
            shq_parse_dedication(tmp1->drvreq.dedicate, buf_ded, BUF_ID_SIZE);
            
            
            fprintf(stdout,"DN %s %s@%s %s %d (%s)\n",
                    tmp1->drvreq.dgn,
                    tmp1->drvreq.drive,
                    tmp1->drvreq.server,
                    buf,
                    now - tmp1->drvreq.recvtime,
                    (*tmp1->drvreq.dedicate != 0)?buf_ded:NO_DEDICATION);

        } else if ( *server == '\0' || 
                    strcmp(server,tmp1->volreq.server) == 0 ) {
            char buf_id[BUF_ID_SIZE];
            
            tp = localtime((time_t *)&tmp1->volreq.recvtime);
            (void)strftime(timestr,64,strftime_format,tp);

            shq_build_id_str(tmp1->volreq.clientUID,
                             tmp1->volreq.clientGID,
                             buf_id,
                             BUF_ID_SIZE);
            
            fprintf(stdout,"Q %s %s %s %d (%s)@%s %d\n", 
                    tmp1->volreq.dgn,
                    tmp1->volreq.volid,
                    (tmp1->volreq.mode == 0 ? "R" : "W"),
                    tmp1->volreq.VolReqID,
                    buf_id,
                    tmp1->volreq.client_host,
                    now - tmp1->volreq.recvtime);
        }
    } CLIST_ITERATE_END(reqlist,tmp1);
}


/**
 * Decodes drive status, providing a readable string.
 * Should be used for assigned drives.
 */
void shq_decode_status_DA(int status, char *buf, int buf_size) {

    if (status == (VDQM_UNIT_UP|VDQM_UNIT_ASSIGN|VDQM_UNIT_BUSY) ) {
        strncpy(buf,"RUNNING", buf_size);
    } else if (status == (VDQM_UNIT_UP|VDQM_UNIT_BUSY)) { 
        strncpy(buf, "START", buf_size);
    } else if (status == (VDQM_UNIT_UP|VDQM_UNIT_BUSY|VDQM_UNIT_RELEASE|VDQM_UNIT_UNKNOWN) ) {
        strncpy(buf, "RELEASE", buf_size);
    } else {
        strncpy(buf, "UNKNOWN",  buf_size);    
    }
    buf[buf_size-1]='\0';
}

/**
 * Decodes drive status, providing a readable string.
 * Shoould be used for non-assigned drives.
 */
void shq_decode_status_DN(int status, char *buf, int buf_size) {

    if (status & VDQM_UNIT_DOWN ) 
        strncpy(buf, "DOWN", buf_size);
    else if (status == (VDQM_UNIT_UP|VDQM_UNIT_FREE) )
        strncpy(buf, "FREE", buf_size);
    else if (status == (VDQM_UNIT_UP |VDQM_UNIT_RELEASE |VDQM_UNIT_BUSY |VDQM_UNIT_UNKNOWN) ) {
        strncpy(buf, "RELEASE", buf_size);
    } else {
        strncpy(buf,"UNKNOWN", buf_size); 
    }
    buf[buf_size-1]='\0';
}



char *shq_getuname(uid_t uid) {
  
  struct passwd *pwd;

  pwd = Cgetpwuid(uid);
  if (pwd == NULL) {
    serrno = SEUSERUNKN;
    return(NULL);
  }
  return(pwd->pw_name);
}


char *shq_getgname(gid_t gid) {
  
  struct group *gr;

  gr = Cgetgrgid(gid);
  if (gr == NULL) {
    serrno = SEGROUPUNKN;
    return(NULL);
  }
  return(gr->gr_name);
}

void shq_build_id_str(uid_t uid, gid_t gid, char *buf, int buf_size) {

    int nb_chars = 0; /* Nb of chars printed in the buffer */
    int ret;
    char *uname = NULL;
    char *gname = NULL;

    /* Retrieving the user name / group name */
    uname = shq_getuname(uid);
    gname = shq_getgname(gid);

    /* Printing the user name in the buffer */
    if (uname == NULL) {
        ret = snprintf(buf, buf_size, "%d,", uid);
    } else {
        ret = snprintf(buf, buf_size, "%s,", uname);
    }
    nb_chars += ret;

    /* Leaving now if buffer full !*/
    if (nb_chars >= buf_size) {
        return;
    }
    
    /* Printing the group name */
    if (gname == NULL) {
        ret = snprintf(buf + nb_chars, buf_size - nb_chars, "%d", gid);
    } else {
        ret = snprintf(buf + nb_chars, buf_size - nb_chars, "%s", gname);
    }

    /* In all cases set the last buffer char to '\0' to be sure */
    buf[buf_size-1] = '\0';

}

/**
 * Reduces the dedication string removing all the entries of the form: xxx=.*
 */
void shq_parse_dedication(char *dedication, char *reduced_dedication, int buf_size) {

    int len, remaining_length;
    int cont = 1; 
    char *ret, *pos, *reduced_pos;
    
    len = strlen(dedication);
    pos = dedication;

    reduced_pos = reduced_dedication;
    
    while (cont) {
        
        /* Looking for next ',' in the string */
        ret = strchr(pos, SEP);

        /* Coma not found, we're at the end of the string */
        if (ret == NULL) {
            ret = dedication + len;
            cont = 0;
        }

        /* Checking whether the regexp finishes with '=.*' */
        if ( !((*(ret - 1)== '*') 
               && (*(ret - 2)== '.')
               && (*(ret - 3)== '=') )) {
            /* copy the string to the reduced regexp */
            char *tmp;

            tmp = pos;
            while (tmp <= ret
                   && (reduced_pos < (reduced_dedication + buf_size ))) {
                *reduced_pos = *tmp;
                reduced_pos++;
                tmp++;
            }
            

        }

        /* Now going on after the ',' */
        pos = ret + 1;
    }

    if (reduced_pos == reduced_dedication) {
        /*Empty buffer, in this case just copy the whole string */
        strncpy(reduced_dedication, dedication, buf_size);
        reduced_dedication[buf_size-1] = '\0';
    } else {
    
        /* Making sure there's a '\0' at the end */
        if (*(reduced_pos-1)== SEP){
            /* Reemoving trailing ',' */
            *(reduced_pos-1) = '\0';
        } else {
            *reduced_pos = '\0';
        }
    }
}










