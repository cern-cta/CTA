/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

/*
 * showqueues.c - command to list queues and running jobs.
 */

#include <stdio.h>
#include <string.h>
#include <regex.h>
#include <time.h>
#include <errno.h>
#include <sys/types.h>
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
    int jid,rc,status,reqid;
    char *volid = NULL; 
    char *dgn = NULL;
    char *unit = NULL;

    if ( argc <= 4 ) {
        fprintf(stderr,"Usage: %s [jid] [dgn] [volid] [unit]\n",argv[0]);
        exit(2);
    }
    jid = atoi(argv[1]);
    dgn = argv[2];
    volid = argv[3];
    unit = argv[4];
    if ( jid < 0 ) {
        fprintf(stderr,"jid (%d) < 0 \n",jid);
        exit(1);
    }

    status = VDQM_UNIT_UP|VDQM_UNIT_ASSIGN|VDQM_UNIT_BUSY;
    reqid = 9999999;
    rc = vdqm_UnitStatus(NULL,volid,dgn,NULL,unit,&status,&reqid,jid);
    if ( rc == -1 ) {
        fprintf(stderr,"vdqm_UnitStatus(NULL,%s,%s,NULL,%s,%d,NULL,%d): (%d) %s\n",
                volid,dgn,unit,status,jid,serrno,sstrerror(serrno));
        exit(1);
    }

    exit(0);
}
