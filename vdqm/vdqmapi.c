/*
 * $Id: vdqmapi.c,v 1.17 2008/10/29 19:17:13 murrayc3 Exp $
 *
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

/*
 * vdqmapi.c - VDQM client API routines (client only).
 */

#include <stdlib.h>
#include <string.h>
#include <pwd.h>
#include <grp.h>
#include <unistd.h>
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */

#include <errno.h>
#include <Castor_limits.h>
#include <osdep.h>
#include <net.h>
#include <Cnetdb.h>
#include <Cpwd.h>
#include <log.h>
#include <serrno.h>
#include <trace.h>
#include <vdqm_constants.h>
#include <vdqm.h>
#include <vdqm_api.h>

#define VDQM_API_ENTER(X)  char __vdqm_API_func[]=#X; \
     INIT_TRACE("VDQM_TRACE"); \
     TRACE(1,"vdqm","%s called",__vdqm_API_func);
#define VDQM_API_RETURN(X) { \
     TRACE(1,"vdqm","%s return(%d), serrno=%d",__vdqm_API_func,(X),serrno); \
     END_TRACE(); return((X));}


int vdqm_Connect(vdqmnw_t **nw) {
    /* reserve space for both VDQM host and replica host */
    char vdqm_host[2*CA_MAXHOSTNAMELEN+1];
    char *vdqm_primary,*vdqm_replica,*try_host;
    struct hostent *hp = NULL;
    struct sockaddr_in sin ; /* Internet address */
    struct servent *sp ;   /* Service entry */
    extern char * getconfent() ;
    extern char * getenv() ;
    char *p ;
    int vdqm_port = -1;
#ifdef VDQMCSEC
    int n;
    int secure_connection = 0;
#endif

    VDQM_API_ENTER(vdqm_Connect);

    if ( nw != NULL && *nw != NULL && (*nw)->connect_socket!=-1 ) {
        TRACE(1,"vdqm","vdqm_Connect() called with invalid socket");
        serrno = EINVAL;
        VDQM_API_RETURN(0);
    }
    /*
     * Connect to appropriate VDQM server and port. The server
     * name is take from:
     *        (1) env variable if defined
     *        (2) configuration variable
     *        (3) compiler constant
     * The service port is selected by:
     *        (1) env variable if defined
     *        (2) configuration variable
     *        (3) service entry
     *        (4) compiler constant
     *        (5) -1 : return error
     */
    *vdqm_host = '\0';
    if ( (p = getenv("VDQM_HOST")) != (char *)NULL ) {
      strcpy(vdqm_host,p);
    } else if ( (p = getconfent("VDQM","HOST",1)) != (char *)NULL ) {
      strcpy(vdqm_host,p);
    }
    p = strtok(vdqm_host,", \t");
    vdqm_primary = p;
    p = strtok(NULL,", \t");
    vdqm_replica = p;
#ifdef VDQMCSEC
    if (getenv("SECURE_CASTOR") != NULL) secure_connection++;
    if (secure_connection) {
      if ( (p = getenv("SVDQM_PORT")) != (char *)NULL ) {
        vdqm_port = atoi(p);
      } else if ( (p = getconfent("SVDQM","PORT",0)) != (char *)NULL ) {
        vdqm_port = atoi(p);
      } else if ( (sp = getservbyname("svdqm","tcp")) != (struct servent *)NULL ) {
        vdqm_port = (int)ntohs(sp->s_port);
      } else {
#if defined(SVDQM_PORT)
        vdqm_port = SVDQM_PORT;
#endif /* SVDQM_PORT */
      }
    } else {
#endif
      if ( (p = getenv("VDQM_PORT")) != (char *)NULL ) {
        vdqm_port = atoi(p);
      } else if ( (p = getconfent("VDQM","PORT",0)) != (char *)NULL ) {
        vdqm_port = atoi(p);
      } else if ( (sp = getservbyname("vdqm","tcp")) != (struct servent *)NULL ) {
        vdqm_port = (int)ntohs(sp->s_port);
      } else {
#if defined(VDQM_PORT)
        vdqm_port = VDQM_PORT;
#endif /* VDQM_PORT */
      }
#ifdef VDQMCSEC
    }
#endif
    if ( vdqm_port < 0 ) {
        TRACE(1,"vdqm","vdqm_Connnect() vdqm_port = %d",vdqm_port);
        serrno = SENOSSERV;
        VDQM_API_RETURN(-1);
    }
    if ( *nw == NULL ) {
        *nw = (vdqmnw_t *)calloc(1,sizeof(vdqmnw_t));
        if ( *nw == NULL ) {
            TRACE(1,"vdqm","vdqm_Connnect() calloc(): %s",sstrerror(errno));
            serrno = SEWOULDBLOCK;
            VDQM_API_RETURN(-1);
        }
        (*nw)->listen_socket = (*nw)->accept_socket = 
            (*nw)->connect_socket = -1;
    }

    (*nw)->connect_socket = socket(AF_INET,SOCK_STREAM,0);
    if ( (*nw)->connect_socket == -1 ) {
        TRACE(1,"vdqm","vdqm_Connnect() socket(): %s",neterror());
        free(*nw);
        serrno = SECOMERR;
        VDQM_API_RETURN(-1);
    }
    try_host = vdqm_primary;
    /* 
     * Loop of primary and replica hosts
     */
    for (;;) {
        if ( (hp = Cgethostbyname(try_host)) == NULL ) {
            TRACE(1,"vdqm","vdqm_Connect() gethostbyname(%s): h_errno=%d, %s",
                  try_host,h_errno,neterror());
            free(*nw);
            serrno = SENOSHOST;
            VDQM_API_RETURN(-1);
        }
        sin.sin_port = htons((short)vdqm_port);
        sin.sin_family = AF_INET;
        sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

        TRACE(1,"vdqm","vdqm_Connect() try to connect to VDQM at %s:%d",
              try_host,vdqm_port);
        if ( connect((*nw)->connect_socket,(struct sockaddr *)&sin, sizeof(struct sockaddr_in)) ==
            -1) {
            TRACE(1,"vdqm","vdqm_Connect() connect(): %s",neterror());
            close((*nw)->connect_socket);

            if ( vdqm_replica == NULL || vdqm_replica == try_host ) {
                TRACE(1,"vdqm","vdqm_Connect() cannot connect to VDQM");
                free(*nw);
                serrno = SECOMERR;
                VDQM_API_RETURN(-1);
            } else {
                (*nw)->connect_socket = socket(AF_INET,SOCK_STREAM,0);
                if ( (*nw)->connect_socket == -1 ) {
                    TRACE(1,"vdqm","vdqm_Connnect() socket(): %s",neterror());
                    free(*nw);
                    serrno = SECOMERR;
                    VDQM_API_RETURN(-1);
                }

                try_host = vdqm_replica;
                continue;
            }
        }
        break;
    } /* for (;;) */
    TRACE(1,"vdqm","vdqm_Connect() successful");
#ifdef VDQMCSEC
    if (secure_connection) {
      if (Csec_client_initContext(&((*nw)->sec_ctx), CSEC_SERVICE_TYPE_CENTRAL, NULL) <0) {
	TRACE (1, "vdqm_CSEC", "Could not init context\n");
	close((*nw)->connect_socket);
	free(*nw);
	serrno = ESEC_CTX_NOT_INITIALIZED;
	VDQM_API_RETURN(-1);
      }
	
      if(Csec_client_establishContext(&((*nw)->sec_ctx), (*nw)->connect_socket)< 0) {
	TRACE (1, "vdqm_CSEC", "Could not establish context\n");
	close((*nw)->connect_socket);
	free(*nw);
	serrno = ESEC_NO_CONTEXT;
	return -1;
      }
	
      Csec_clearContext(&((*nw)->sec_ctx));
    }
#endif

    VDQM_API_RETURN(0);
}

int vdqm_Disconnect(vdqmnw_t **nw) {
    int rc, retval;
    VDQM_API_ENTER(vdqm_Disconnect);

    retval = 0;
    if ( nw == NULL || *nw == NULL ) return(-1);
    if ( (*nw)->connect_socket != -1 ) {
        if ( (rc = shutdown((*nw)->connect_socket,2)) == -1 ) {
            TRACE(1,"vdqm","vdqm_Disconnect() shutdown(): %s",neterror());
            retval = -1;
        }
        if ( (rc = close((*nw)->connect_socket)) == -1 ) {
            TRACE(1,"vdqm","vdqm_Disconnect() close(%d): %s",
                  (*nw)->connect_socket,neterror());
            retval = -1;
        }
        free(*nw);
        *nw = NULL;
    }
    if ( retval == -1 ) serrno = SECOMERR;
    if ( retval == 0 ) TRACE(1,"vdqm","vdqm_Disconnect() successful");
    VDQM_API_RETURN(retval);
}

int vdqm_SendVolReq(vdqmnw_t *nw,
                    int  *reqID,
                    char *VID, 
                    char *dgn,
                    char *server, 
                    char *unit, 
                    int mode,
                    int client_port) {
    vdqmVolReq_t volreq;
    vdqmnw_t *tmpnw = NULL;
    struct passwd *pw;
    int save_serrno = 0;
    int rc = 0;
    VDQM_API_ENTER(vdqm_SendVolReq);

    memset(&volreq,'\0',sizeof(vdqmVolReq_t));
    if ( (nw != NULL && nw->connect_socket == -1) ||
        VID == NULL || dgn == NULL || client_port < 0 ) {
        TRACE(1,"vdqm","vdqm_SendVolReq() called with invalid socket");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( nw == NULL ) {
        rc = vdqm_Connect(&tmpnw);
        if ( rc < 0 ) VDQM_API_RETURN(rc);
    } else tmpnw = nw;
    strcpy(volreq.volid,VID);
    strcpy(volreq.dgn,dgn);
    volreq.client_port = client_port;
    volreq.mode = mode;
    if ( unit != NULL ) strcpy(volreq.drive,unit);
    if ( server != NULL ) strcpy(volreq.server,server);
    volreq.clientUID = (getenv("VDQM_EUID") != NULL) ? atoi(getenv("VDQM_EUID")) : (int)geteuid();
    volreq.clientGID = (getenv("VDQM_EGID") != NULL) ? atoi(getenv("VDQM_EGID")) : (int)getegid();
    pw = Cgetpwuid(volreq.clientUID);
    if ( pw == NULL ) {
        TRACE(1,"vdqm","vdqm_SendVolReq() Cgetpwuid() error: %s\n",
              sstrerror(serrno));
        VDQM_API_RETURN(-1);
    }
    strcpy(volreq.client_name,pw->pw_name);
    TRACE(1,"vdqm","vdqm_SendVolReq() send request VID %s, drive %s@%s, dgn %s, for %s (%d,%d)",
          volreq.volid,(*volreq.drive!='\0' ? volreq.drive : "*"),
          (*volreq.server!='\0' ? volreq.server : "*"),volreq.dgn,
          volreq.client_name,volreq.clientUID,volreq.clientGID);
    rc = vdqm_SendReq(tmpnw,NULL,&volreq,NULL);
    if ( rc != -1 ) {
        rc = vdqm_RecvAckn(tmpnw);
        TRACE(1,"vdqm","vdqm_SendVolReq() vdqm_RecvAckn() rc = 0x%x",rc);
        if ( rc == VDQM_COMMIT ) {
			memset(&volreq,'\0',sizeof(volreq));
            rc = vdqm_RecvReq(tmpnw,NULL,&volreq,NULL);
            if ( rc != -1 ) {
                rc = vdqm_AcknCommit(tmpnw);
                rc = 0;
            }
        } else {
            if ( rc > 0 ) save_serrno = rc;
            rc = -1;
        }
    }
    TRACE(1,"vdqm","vdqm_SendVolReq() received rc=%d, VolReqID=%d",
          rc,volreq.VolReqID);
    if ( nw == NULL ) vdqm_Disconnect(&tmpnw);
    if ( rc != -1 && reqID != NULL ) *reqID = volreq.VolReqID;
    if ( rc == -1 && save_serrno != 0 ) serrno = save_serrno;
    VDQM_API_RETURN(rc);
}

int vdqm_DelVolumeReq(vdqmnw_t *nw,
                    int  reqID,
                    char *VID, 
                    char *dgn,
                    char *server, 
                    char *unit, 
                    int client_port) {
    vdqmVolReq_t volreq;
    vdqmHdr_t hdr;
    vdqmnw_t *tmpnw = NULL;
    int save_serrno = 0;
    int rc = 0;
    VDQM_API_ENTER(vdqm_DelVolumeReq);

    memset(&hdr,'\0',sizeof(hdr));
    memset(&volreq,'\0',sizeof(vdqmVolReq_t));
    if ( (nw != NULL && nw->connect_socket == -1) ||
        reqID <= 0 || VID == NULL || dgn == NULL || client_port < 0 ) {
        TRACE(1,"vdqm","vdqm_DelVolumeReq() called with invalid argument");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( nw == NULL ) {
        rc = vdqm_Connect(&tmpnw);
        if ( rc < 0 ) VDQM_API_RETURN(rc);
    } else tmpnw = nw;
    strcpy(volreq.volid,VID);
    strcpy(volreq.dgn,dgn);
    volreq.client_port = client_port;
    if ( unit != NULL ) strcpy(volreq.drive,unit);
    if ( server != NULL ) strcpy(volreq.server,server);
    volreq.clientUID = geteuid();
    volreq.clientGID = getegid();
    volreq.VolReqID = reqID;
    hdr.reqtype = VDQM_DEL_VOLREQ;
    rc = vdqm_SendReq(tmpnw,&hdr,&volreq,NULL);
    if ( rc != -1 ) {
        rc = vdqm_RecvAckn(tmpnw);
        TRACE(1,"vdqm","vdqm_DelVolReq() vdqm_RecvAckn() rc = 0x%x",rc);
        if ( rc == VDQM_COMMIT ) {
			memset(&volreq,'\0',sizeof(volreq));
            rc = vdqm_RecvReq(tmpnw,&hdr,&volreq,NULL);
            if ( rc != -1 ) {
                rc = vdqm_AcknCommit(tmpnw);
                rc = 0;
            }
        } else {
            if ( rc > 0 ) save_serrno = rc;
            rc = -1;
        }
    }
    if ( nw == NULL ) vdqm_Disconnect(&tmpnw);
    if ( rc == -1 && save_serrno > 0 ) serrno = save_serrno;
    VDQM_API_RETURN(rc);
}

int vdqm_NextVol(vdqmnw_t **nw, vdqmVolReq_t *volreq) {
    vdqmHdr_t hdr;
    int save_serrno = 0;
    int rc = 0;
    VDQM_API_ENTER(vdqm_NextVol);

    if ( nw == NULL || volreq == NULL ) {
        TRACE(1,"vdqm","vdqm_NextVol() called with invalid argument");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( *nw != NULL && (*nw)->connect_socket == -1 ) {
        TRACE(1,"vdqm","vdqm_NextVol() called with invalid argument");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( *nw == NULL ) {
        rc = vdqm_Connect(nw);
        if ( rc < 0 ) VDQM_API_RETURN(rc);
        memset(&hdr,'\0',sizeof(hdr));
        hdr.reqtype = VDQM_GET_VOLQUEUE;
        TRACE(1,"vdqm","vdqm_NextVol() dgn=%s",volreq->dgn);
        rc = vdqm_SendReq(*nw,&hdr,volreq,NULL);
    }
    if ( rc != -1 ) {
		memset(volreq,'\0',sizeof(vdqmVolReq_t));
        rc = vdqm_RecvReq(*nw,&hdr,volreq,NULL);
        TRACE(1,"vdqm","vdqm_NextVol(): vdqm_RecvReq() rc=%d",rc);
    }
    if ( rc == -1 || volreq->VolReqID == -1 ) {
        rc = vdqm_RecvAckn(*nw);
        TRACE(1,"vdqm","vdqm_NextVol() vdqm_RecvAckn() rc = 0x%x",rc);
        if ( rc == VDQM_COMMIT ) {
			memset(volreq,'\0',sizeof(vdqmVolReq_t));
            rc = vdqm_RecvReq(*nw,&hdr,volreq,NULL);
            if ( rc != -1 ) {
                rc = vdqm_AcknCommit(*nw);
                rc = -1;
            }
        } else {
            if ( rc > 0 ) save_serrno = rc;
            rc = -1;
        }
        vdqm_Disconnect(nw);
    }
    if ( rc == -1 && save_serrno > 0 ) serrno = save_serrno;
    VDQM_API_RETURN(rc);
}

int vdqm_UnitStatus(vdqmnw_t *nw, 
                    char *VID, 
                    char *dgn, 
                    char *server, 
                    char *unit,
                    int *status, 
                    int *value,
                    int jobID) {
    vdqmDrvReq_t drvreq;
    vdqmnw_t *tmpnw = NULL;
    int len;
    int save_serrno = 0;
    int rc = 0;
    VDQM_API_ENTER(vdqm_UnitStatus);

    memset(&drvreq,'\0',sizeof(vdqmDrvReq_t));
    if ( (nw != NULL && nw->connect_socket == -1) ||
        status == NULL || (*status != VDQM_TPD_STARTED && 
        (unit == NULL || dgn == NULL)) ) {
        TRACE(1,"vdqm","vdqm_UnitStatus() called with invalid argument");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( (jobID <= 0 || value == NULL) && (*status & VDQM_UNIT_ASSIGN) ) {
        TRACE(1,"vdqm","vdqm_UnitStatus() VDQM_UNIT_ASSIGN requested without jobID and VolReqID");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( jobID <= 0  && (*status & (VDQM_VOL_MOUNT | VDQM_UNIT_RELEASE )) ) {
        TRACE(1,"vdqm","vdqm_UnitStatus() VDQM_VOL_MOUNT|VDQM_UNIT_RELEASE requested without jobID (%d)",jobID);
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }

    if ( nw == NULL ) {
        rc = vdqm_Connect(&tmpnw);
        if ( rc < 0 ) VDQM_API_RETURN(rc);
    } else tmpnw = nw;
    if ( VID != NULL ) strcpy(drvreq.volid,VID);
    if ( server != NULL ) strcpy(drvreq.server,server);
    else {
        len = sizeof(drvreq.server)-1;
        rc = gethostname(drvreq.server,len);
        if ( rc == -1 ) {
            TRACE(1,"vdqm","vdqm_UnitStatus() gethostname() %s",neterror());
            serrno = SECOMERR;
            VDQM_API_RETURN(-1);
        }
    }
    if ( dgn != NULL ) strcpy(drvreq.dgn,dgn);
    if ( unit != NULL ) strcpy(drvreq.drive,unit);
    drvreq.status = *status;
    if ( jobID > 0 || (*status & VDQM_VOL_UNMOUNT) != 0 ) drvreq.jobID = jobID;
    else drvreq.jobID = (int)getpid();

    if ( value != NULL ) {
        if ( *status & VDQM_UNIT_MBCOUNT ) {
            drvreq.MBtransf = *value;
        } else if ( *status & VDQM_UNIT_ASSIGN ) {
            drvreq.VolReqID = *value;
        } 
    }

    TRACE(1,"vdqm","vdqm_UnitStatus() send status 0x%x for jobID %d, VolReqID %d vid=%s drive %s@%s dgn=%s",
          drvreq.status,drvreq.jobID,drvreq.VolReqID,drvreq.volid,
          drvreq.drive,drvreq.server,drvreq.dgn);
    rc = vdqm_SendReq(tmpnw,NULL,NULL,&drvreq);
    if ( rc != -1 ) {
        rc = vdqm_RecvAckn(tmpnw);
        TRACE(1,"vdqm","vdqm_SendDrvReq() vdqm_RecvAckn() rc = 0x%x",rc);
        if ( rc == VDQM_COMMIT ) {
			memset(&drvreq,'\0',sizeof(drvreq));
            rc = vdqm_RecvReq(tmpnw,NULL,NULL,&drvreq);
            if ( rc != -1 ) {
                rc = vdqm_AcknCommit(tmpnw);
                rc = 0;
            }
            rc = 0;
        } else {
            if ( rc > 0 ) save_serrno = rc;
            rc = -1;
        }
    }
    TRACE(1,"vdqm","vdqm_UnitStatus() returned status 0x%x for jobID %d, VolReqID %d vid=%s drive %s@%s dgn=%s",
          drvreq.status,drvreq.jobID,drvreq.VolReqID,drvreq.volid,
          drvreq.drive,drvreq.server,drvreq.dgn);

    if ( nw == NULL ) vdqm_Disconnect(&tmpnw);
    if ( rc != -1 ) {
        if ( *status == VDQM_UNIT_QUERY ) {
            if ( VID != NULL ) strcpy(VID,drvreq.volid);
            if ( server != NULL ) strcpy(server,drvreq.server);
            if ( dgn != NULL ) strcpy(dgn,drvreq.dgn);
            if ( value != NULL && drvreq.jobID > 0 ) *value = drvreq.jobID;
            else *value = drvreq.VolReqID;
        } else {
            if ( value != NULL ) *value = drvreq.jobID;
            if ( VID != NULL && (drvreq.status & VDQM_VOL_UNMOUNT) ) 
                strcpy(VID,drvreq.volid);
        }
        *status = drvreq.status;
    }
    if ( rc == -1 && save_serrno > 0 ) serrno = save_serrno;
    VDQM_API_RETURN(rc);
}

int vdqm_DelDrive(vdqmnw_t *nw,
                           char *dgn,
                           char *server,
                           char *unit) {
    vdqmDrvReq_t drvreq;
    vdqmHdr_t hdr;
    vdqmnw_t *tmpnw = NULL;
    int len;
    int save_serrno = 0;
    int rc = 0;
    VDQM_API_ENTER(vdqm_DelDrive);

    memset(&hdr,'\0',sizeof(hdr));
    memset(&drvreq,'\0',sizeof(vdqmDrvReq_t));
    if ( (nw != NULL && nw->connect_socket == -1) ||
        unit == NULL || dgn == NULL ) {
        TRACE(1,"vdqm","vdqm_DelDrive() called with invalid argument");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( nw == NULL ) {
        rc = vdqm_Connect(&tmpnw);
        if ( rc < 0 ) VDQM_API_RETURN(rc);
    } else tmpnw = nw;
    if ( server != NULL ) strcpy(drvreq.server,server);
    else {
        len = sizeof(drvreq.server)-1;
        rc = gethostname(drvreq.server,len);
        if ( rc == -1 ) {
            TRACE(1,"vdqm","vdqm_DelDrive() gethostname() %s",neterror());
            serrno = SECOMERR;
            VDQM_API_RETURN(-1);
        }
    }
    strcpy(drvreq.dgn,dgn);
    strcpy(drvreq.drive,unit);
    hdr.reqtype = VDQM_DEL_DRVREQ;
    rc = vdqm_SendReq(tmpnw,&hdr,NULL,&drvreq);
    if ( rc != -1 ) {
        rc = vdqm_RecvAckn(tmpnw);
        TRACE(1,"vdqm","vdqm_DelDrive() vdqm_RecvAckn() rc = 0x%x",rc);
        if ( rc == VDQM_COMMIT ) {
			memset(&drvreq,'\0',sizeof(drvreq));
            rc = vdqm_RecvReq(tmpnw,&hdr,NULL,&drvreq);
            if ( rc != -1 ) {
                rc = vdqm_AcknCommit(tmpnw);
                rc = 0;
            }
            rc = 0;
        } else {
            if ( rc > 0 ) save_serrno = rc;
            rc = -1;
        }
    }
    if ( nw == NULL ) vdqm_Disconnect(&tmpnw);
    if ( rc == -1 && save_serrno > 0 ) serrno = save_serrno;
    VDQM_API_RETURN(rc);
}

int vdqm_NextDrive(vdqmnw_t **nw, vdqmDrvReq_t *drvreq) {
    vdqmHdr_t hdr;
    int rc = 0;
    int save_serrno = 0;
    VDQM_API_ENTER(vdqm_NextDrive);

    if ( nw == NULL || drvreq == NULL ) {
        TRACE(1,"vdqm","vdqm_NextDrive() called with invalid argument");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( *nw != NULL && (*nw)->connect_socket == -1 ) {
        TRACE(1,"vdqm","vdqm_NextDrive() called with invalid socket");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( *nw == NULL ) {
        rc = vdqm_Connect(nw);
        if ( rc < 0 ) return(rc);
        memset(&hdr,'\0',sizeof(hdr));
        hdr.reqtype = VDQM_GET_DRVQUEUE;
        rc = vdqm_SendReq(*nw,&hdr,NULL,drvreq);
    }
    if ( rc != -1 ) {
		memset(drvreq,'\0',sizeof(vdqmDrvReq_t));
        rc = vdqm_RecvReq(*nw,&hdr,NULL,drvreq);
    }
    if ( rc == -1 || drvreq->DrvReqID == -1 ) {
        rc = vdqm_RecvAckn(*nw);
        TRACE(1,"vdqm","vdqm_NextDrive() vdqm_RecvAckn() rc = 0x%x",rc);
        if ( rc == VDQM_COMMIT ) {
			memset(drvreq,'\0',sizeof(vdqmDrvReq_t));
            rc = vdqm_RecvReq(*nw,&hdr,NULL,drvreq);
            if ( rc != -1 ) {
                rc = vdqm_AcknCommit(*nw);
                rc = -1;
            }
        } else {
            if ( rc > 0 ) save_serrno = rc;
            rc = -1;
        }
        vdqm_Disconnect(nw);
    }
    if ( rc == -1 && save_serrno > 0 ) serrno = save_serrno;
    VDQM_API_RETURN(rc);
}

int vdqm_DedicateDrive(vdqmnw_t *nw, 
                                char *dgn,
                                char *server,
                                char *unit,
                                char *dedicate) {
    vdqmDrvReq_t drvreq;
    vdqmHdr_t hdr;
    vdqmnw_t *tmpnw = NULL;
    int len, i;
    int save_serrno = 0;
    int rc = 0;
    char tmpstr[CA_MAXLINELEN+1], *p, *q;
    char keywords[][20] = VDQM_DEDICATE_PREFIX;
    char defaults[][20] = VDQM_DEDICATE_DEFAULTS;

    VDQM_API_ENTER(vdqm_dedicate);

    if ( dgn == NULL || unit == NULL ) {
        TRACE(1,"vdqm","vdqm_dedicate() called with invalid argument");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }

    memset(&hdr,'\0',sizeof(hdr));
    memset(&drvreq,'\0',sizeof(drvreq));
    if ( dedicate == NULL || *dedicate == '\0' ) {
      TRACE(1,"vdqm","vdqm_dedicate() null dedicate string given");
      serrno = EINVAL;
      VDQM_API_RETURN(-1);
    } else {
        if ( strlen(dedicate) > sizeof(tmpstr)-1 ) {
            TRACE(1,"vdqm","vdqm_dedicate() dedication string too long (%d>%d)",
                  strlen(dedicate),sizeof(drvreq.dedicate)-1);
            serrno = E2BIG;
            VDQM_API_RETURN(-1);
        }
        strcpy(tmpstr,dedicate);

        for (i=0; *keywords[i] != '\0'; i++) {
            if ( (p = strstr(tmpstr,keywords[i])) == NULL ) {
                if ( strlen(drvreq.dedicate) + strlen(defaults[i]) > 
                     sizeof(drvreq.dedicate) - 1 ) {
                    TRACE(1,"vdqm","vdqm_dedicate() expanded dedication too long");
                    serrno = E2BIG;
                    VDQM_API_RETURN(-1);
                }
                strcat(drvreq.dedicate,defaults[i]);
            } else {
                q = strchr(p,',');
                if ( q != NULL ) *q = '\0';
                if ( strlen(drvreq.dedicate) + strlen(p) > 
                     sizeof(drvreq.dedicate) - 1 ) {
                    TRACE(1,"vdqm","vdqm_dedicate() expanded dedication too long");
                    serrno = E2BIG;
                    VDQM_API_RETURN(-1);
                }
                strcat(drvreq.dedicate,p);
                if ( q != NULL ) *q = ',';
            }
            if ( *keywords[i+1] != '\0' ) strcat(drvreq.dedicate,",");
        }
        TRACE(1,"vdqm","vdqm_dedicate() expanded dedicate = %s",
              drvreq.dedicate);
    }

    if ( server != NULL ) strcpy(drvreq.server,server);
    else {
        len = sizeof(drvreq.server)-1;
        rc = gethostname(drvreq.server,len);
        if ( rc == -1 ) {
            TRACE(1,"vdqm","vdqm_dedicate() gethostname() %s",neterror());
            serrno = SECOMERR;
            VDQM_API_RETURN(-1);
        }
    }
    strcpy(drvreq.dgn,dgn);
    strcpy(drvreq.drive,unit);

    if ( nw == NULL ) {
        rc = vdqm_Connect(&tmpnw);
        if ( rc < 0 ) VDQM_API_RETURN(rc);
    } else tmpnw = nw;

    hdr.reqtype = VDQM_DEDICATE_DRV;
    rc = vdqm_SendReq(tmpnw,&hdr,NULL,&drvreq);
    if ( rc != -1 ) {
        rc = vdqm_RecvAckn(tmpnw);
        TRACE(1,"vdqm","vdqm_dedicate() vdqm_RecvAckn() rc = 0x%x",rc);
        if ( rc == VDQM_COMMIT ) {
			memset(&drvreq,'\0',sizeof(drvreq));
            rc = vdqm_RecvReq(tmpnw,&hdr,NULL,&drvreq);
            if ( rc != -1 ) {
                rc = vdqm_AcknCommit(tmpnw);
                rc = 0;
            }
            rc = 0;
        } else {
            if ( rc > 0 ) save_serrno = rc;
            rc = -1;
        }
    }

    if ( nw == NULL ) vdqm_Disconnect(&tmpnw);
    if ( rc == -1 && save_serrno > 0 ) serrno = save_serrno;
    VDQM_API_RETURN(rc);
} 

int vdqm_admin(vdqmnw_t *nw, int admin_req) {
    vdqmHdr_t hdr;
    vdqmnw_t *tmpnw = NULL;
    int rc = 0;
    int save_serrno = 0;
    VDQM_API_ENTER(vdqm_admin);

    if ( (admin_req != VDQM_SHUTDOWN) && (admin_req != VDQM_HOLD) &&
        (admin_req != VDQM_RELEASE) ) {
        TRACE(1,"vdqm","vdqm_admin() called with invalid argument");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( nw != NULL && nw->connect_socket == -1 ) {
        TRACE(1,"vdqm","vdqm_admin() called with an invalid socket");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( nw == NULL ) {
        rc = vdqm_Connect(&tmpnw);
        if ( rc < 0 ) VDQM_API_RETURN(rc);
    } else tmpnw = nw;
    memset(&hdr,'\0',sizeof(hdr));
    hdr.reqtype = admin_req;
    rc = vdqm_SendReq(tmpnw,&hdr,NULL,NULL);
    if ( rc != -1 ) {
        rc = vdqm_RecvAckn(tmpnw);
        TRACE(1,"vdqm","vdqm_admin() vdqm_RecvAckn() rc = 0x%x",rc);
        if ( rc == VDQM_COMMIT ) {
            rc = vdqm_RecvReq(tmpnw,&hdr,NULL,NULL);
            if ( rc != -1 ) {
                rc = vdqm_AcknCommit(tmpnw);
                rc = 0;
            }
            rc = 0;
        } else {
            if ( rc > 0 ) save_serrno = rc;
            rc = -1;
        }
    }
    if ( nw == NULL ) vdqm_Disconnect(&tmpnw);
    if ( admin_req == VDQM_SHUTDOWN ) {
        /*
         * Send a ping to wake up server to complete shutdown
         */
        rc = vdqm_PingServer(NULL,NULL,0);
    }
    if ( save_serrno > 0 ) {
        rc = -1;
        serrno = save_serrno;
    }
    VDQM_API_RETURN(0);
}

int vdqm_GetClientAddr(char *buf,
                       char *clienthost, 
                       int *clientport, 
                       int *VolReqID, 
                       int *uid, int *gid,
                       char *name,
                       char *dgn,
                       char *tapeunit) {
    vdqmVolReq_t volreq;
    vdqmDrvReq_t drvreq;
    int rc;

    if ( buf == NULL || clienthost == NULL || clientport == NULL || 
         VolReqID == NULL || uid == NULL || gid == NULL || name == NULL ||
         dgn == NULL || tapeunit == NULL) {
        serrno = EINVAL;
        return(-1);
    }
    memset(&volreq,'\0',sizeof(volreq));
    memset(&drvreq,'\0',sizeof(drvreq));
    rc = vdqm_GetRTCPReq(buf,&volreq,&drvreq);
    strcpy(clienthost,volreq.client_host);
    *clientport = volreq.client_port;
    *VolReqID = volreq.VolReqID;
    *uid = volreq.clientUID;
    *gid = volreq.clientGID;
    strcpy(name,volreq.client_name);
    strcpy(dgn,drvreq.dgn);
    strcpy(tapeunit,drvreq.drive);
    return(rc);
}

int vdqm_AcknClientAddr(int s,
                                 int status,
                                 int errmsglen,
                                 char *errmsg) {
    int rc,l_status,l_errmsglen;

    if ( s == -1 ) {
        serrno = EINVAL;
        return(-1);
    }
    l_status = l_errmsglen = 0;
    l_status = status;
    l_errmsglen = errmsglen;
    if ( errmsg == NULL ) l_errmsglen = 0;
    rc = vdqm_SendRTCPAckn(s,&l_status,&l_errmsglen,errmsg);
    return(rc);
}

int vdqm_PingServer(vdqmnw_t *nw, char *dgn, int reqID) {
    vdqmVolReq_t volreq;
    vdqmnw_t *tmpnw = NULL;
    int rc, save_serrno;
    VDQM_API_ENTER(vdqm_PingServer)

    memset(&volreq,'\0',sizeof(vdqmVolReq_t));
    volreq.VolReqID = reqID;
    if ( nw == NULL ) {
        rc = vdqm_Connect(&tmpnw);
        if ( rc < 0 ) VDQM_API_RETURN(rc);
    } else tmpnw = nw;
    if ( dgn != NULL ) strcpy(volreq.dgn,dgn);

    rc = vdqm_SendPing(tmpnw,NULL,&volreq);
    save_serrno = serrno;
    if ( rc != -1 ) {
        rc = vdqm_RecvPingAckn(tmpnw);
        save_serrno = serrno;
    }
    if ( nw == NULL ) vdqm_Disconnect(&tmpnw);
    if ( rc == -1 ) serrno = save_serrno;
    VDQM_API_RETURN(rc);
}

int vdqm_SendVolPriority(char *vid, int tpMode, int priority,
  int lifespanType) {
    vdqmnw_t *nw = NULL;
    vdqmVolPriority_t msg;
    vdqmnw_t *tmpnw = NULL;
    int save_serrno = 0;
    int rc = 0;
    VDQM_API_ENTER(vdqm_SendVolPriority);

    memset(&msg,'\0',sizeof(msg));
    if ( vid == NULL ) {
        TRACE(1,"vdqm","vdqm_SendVolPriority() called with invalid argument");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }

    rc = vdqm_Connect(&tmpnw);
    if ( rc < 0 ) VDQM_API_RETURN(rc);

    msg.priority = priority;
    msg.clientUID = geteuid();
    msg.clientGID = getegid();
    /* msg.clientHost is set by vdqm_SendVolPriority_Transfer */
    strncpy(msg.vid, vid, sizeof(msg.vid));
    msg.vid[sizeof(msg.vid)-1] = '\0';
    msg.tpMode = tpMode;
    msg.lifespanType = lifespanType;
    rc = vdqm_SendVolPriority_Transfer(tmpnw,&msg);
    if ( rc != -1 ) {
        rc = vdqm_RecvAckn(tmpnw);
        TRACE(1,"vdqm","vdqm_SendVolPriority() vdqm_RecvAckn() rc = 0x%x",rc);
        if ( rc == VDQM_COMMIT ) {
            rc = vdqm_AcknCommit(tmpnw);
            rc = 0;
        } else {
            if ( rc > 0 ) save_serrno = rc;
            rc = -1;
        }
    }
    if ( nw == NULL ) vdqm_Disconnect(&tmpnw);
    if ( rc == -1 && save_serrno > 0 ) serrno = save_serrno;
    VDQM_API_RETURN(rc);
}

int vdqm_SendDelDrv(char *server, char *drive, char *dgn) {
    vdqmnw_t *nw = NULL;
    vdqmDelDrv_t msg;
    vdqmnw_t *tmpnw = NULL;
    int save_serrno = 0;
    int rc = 0;
    VDQM_API_ENTER(vdqm_SendDelDrv);

    memset(&msg,'\0',sizeof(msg));
    if ( server == NULL || drive == NULL || dgn == NULL ) {
        TRACE(1,"vdqm","vdqm_SendDelDrv() called with invalid argument");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }

    rc = vdqm_Connect(&tmpnw);
    if ( rc < 0 ) VDQM_API_RETURN(rc);

    msg.clientUID = geteuid();
    msg.clientGID = getegid();
    /* msg.clientHost is set by vdqm_SendDelDrv_Transfer */
    strncpy(msg.server, server, sizeof(msg.server));
    msg.server[sizeof(msg.server)-1] = '\0';
    strncpy(msg.drive, drive, sizeof(msg.drive));
    msg.drive[sizeof(msg.drive)-1] = '\0';
    strncpy(msg.dgn, dgn, sizeof(msg.dgn));
    msg.dgn[sizeof(msg.dgn)-1] = '\0';

    rc = vdqm_SendDelDrv_Transfer(tmpnw,&msg);
    if ( rc != -1 ) {
        rc = vdqm_RecvAckn(tmpnw);
        TRACE(1,"vdqm","vdqm_SendDelDrv() vdqm_RecvAckn() rc = 0x%x",rc);
        if ( rc == VDQM_COMMIT ) {
            rc = vdqm_AcknCommit(tmpnw);
            rc = 0;
        } else {
            if ( rc > 0 ) save_serrno = rc;
            rc = -1;
        }
    }
    if ( nw == NULL ) vdqm_Disconnect(&tmpnw);
    if ( rc == -1 && save_serrno > 0 ) serrno = save_serrno;
    VDQM_API_RETURN(rc);
}

int vdqm_SendDedicate(char *server, char *drive, char *dgn,
    char *dedicate) {
    vdqmnw_t *nw = NULL;
    vdqmDedicate_t msg;
    vdqmnw_t *tmpnw = NULL;
    int save_serrno = 0;
    int rc = 0;
    VDQM_API_ENTER(vdqm_SendDedicate);

    memset(&msg,'\0',sizeof(msg));
    if ( server == NULL || drive == NULL || dgn == NULL || dedicate == NULL ) {
        TRACE(1,"vdqm","vdqm_SendDedicate() called with invalid argument");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }

    /* Expand dedication string */
    {
        char tmpstr[CA_MAXLINELEN+1], *p, *q;
        char keywords[][20] = VDQM_DEDICATE_PREFIX;
        char defaults[][20] = VDQM_DEDICATE_DEFAULTS;

        if ( *dedicate == '\0' ) {
           msg.dedicate[0] = '\0';
        } else {
            int i = 0;

            if ( strlen(dedicate) > sizeof(tmpstr)-1 ) {
                TRACE(1,"vdqm",
                      "vdqm_SendDedicate() dedication string too long (%d>%d)",
                      strlen(dedicate),sizeof(msg.dedicate)-1);
                serrno = E2BIG;
                VDQM_API_RETURN(-1);
            }
            strcpy(tmpstr,dedicate);

            for (i=0; *keywords[i] != '\0'; i++) {
                if ( (p = strstr(tmpstr,keywords[i])) == NULL ) {
                    if ( strlen(msg.dedicate) + strlen(defaults[i]) >
                         sizeof(msg.dedicate) - 1 ) {
                        TRACE(1,"vdqm",
                           "vdqm_SendDedicate() expanded dedication too long");
                        serrno = E2BIG;
                        VDQM_API_RETURN(-1);
                    }
                    strcat(msg.dedicate,defaults[i]);
                } else {
                    q = strchr(p,',');
                    if ( q != NULL ) *q = '\0';
                    if ( strlen(msg.dedicate) + strlen(p) >
                         sizeof(msg.dedicate) - 1 ) {
                        TRACE(1,"vdqm",
                            "vdqm_SendDedicate() expanded dedication too long");
                        serrno = E2BIG;
                        VDQM_API_RETURN(-1);
                    }
                    strcat(msg.dedicate,p);
                    if ( q != NULL ) *q = ',';
                }
                if ( *keywords[i+1] != '\0' ) strcat(msg.dedicate,",");
            }
            TRACE(1,"vdqm","vdqm_SendDedicate() expanded dedicate = %s",
                msg.dedicate);
        }
    }

    rc = vdqm_Connect(&tmpnw);
    if ( rc < 0 ) VDQM_API_RETURN(rc);

    msg.clientUID = geteuid();
    msg.clientGID = getegid();
    /* msg.clientHost is set by vdqm_SendDedicate_Transfer */
    strncpy(msg.server, server, sizeof(msg.server));
    msg.server[sizeof(msg.server)-1] = '\0';
    strncpy(msg.drive, drive, sizeof(msg.drive));
    msg.drive[sizeof(msg.drive)-1] = '\0';
    strncpy(msg.dgn, dgn, sizeof(msg.dgn));
    msg.dgn[sizeof(msg.dgn)-1] = '\0';
    strncpy(msg.dedicate, dedicate, sizeof(msg.dedicate));
    msg.dedicate[sizeof(msg.dedicate)-1] = '\0';

    rc = vdqm_SendDedicate_Transfer(tmpnw,&msg);
    if ( rc != -1 ) {
        rc = vdqm_RecvAckn(tmpnw);
        TRACE(1,"vdqm","vdqm_SendDedicate() vdqm_RecvAckn() rc = 0x%x",rc);
        if ( rc == VDQM_COMMIT ) {
            rc = vdqm_AcknCommit(tmpnw);
            rc = 0;
        } else {
            if ( rc > 0 ) save_serrno = rc;
            rc = -1;
        }
    }
    if ( nw == NULL ) vdqm_Disconnect(&tmpnw);
    if ( rc == -1 && save_serrno > 0 ) serrno = save_serrno;
    VDQM_API_RETURN(rc);
}


int vdqm_CreateRequestForAggregator(vdqmnw_t *nw,
                    int  *reqID,
                    char *VID, 
                    char *dgn,
                    char *server, 
                    char *unit, 
                    int  mode,
                    int  client_port) {
    vdqmVolReq_t  volreq;
    struct passwd *pw         = NULL;
    int           save_serrno = 0;
    int           rc          = 0;

    VDQM_API_ENTER(vdqm_CreateRequestForAggregator);

    /* Check function parameters are valid */
    if ( reqID == NULL ) {
        TRACE(1,"vdqm",
            "vdqm_CreateRequestForAggregator() called with NULL reqID");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( nw == NULL ) {
        TRACE(1,"vdqm",
            "vdqm_CreateRequestForAggregator() called with nw == NULL");

        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( nw->connect_socket == -1 ) {
        TRACE(1,"vdqm",
            "vdqm_CreateRequestForAggregator() called with invalid socket");

        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( VID == NULL ) {
        TRACE(1,"vdqm",
            "vdqm_CreateRequestForAggregator() called with NULL VID");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( dgn == NULL ) {
        TRACE(1,"vdqm",
            "vdqm_CreateRequestForAggregator() called with NULL dgn");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( client_port < 0) {
        TRACE(1,"vdqm",
          "vdqm_CreateRequestForAggregator() called with negative client port");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }

    /* Clear and then fill the VDQM volume request */
    memset(&volreq,'\0',sizeof(vdqmVolReq_t));
    strcpy(volreq.volid,VID);
    strcpy(volreq.dgn,dgn);
    volreq.client_port = client_port;
    volreq.mode = mode;
    if ( unit != NULL ) {
        strcpy(volreq.drive,unit);
    }
    if ( server != NULL ) {
        strcpy(volreq.server,server);
    }
    volreq.clientUID = (getenv("VDQM_EUID") != NULL) ?
      atoi(getenv("VDQM_EUID")) : (int)geteuid();
    volreq.clientGID = (getenv("VDQM_EGID") != NULL) ?
      atoi(getenv("VDQM_EGID")) : (int)getegid();
    pw = Cgetpwuid(volreq.clientUID);
    if ( pw == NULL ) {
        TRACE(1,"vdqm",
            "vdqm_CreateRequestForAggregator() Cgetpwuid() error: %s\n",
            sstrerror(serrno));
        VDQM_API_RETURN(-1);
    }
    strcpy(volreq.client_name,pw->pw_name);
    TRACE(1,"vdqm","vdqm_CreateRequestForAggregator() send request VID %s, drive %s@%s, dgn %s, for %s (%d,%d)",
        volreq.volid,(*volreq.drive!='\0' ? volreq.drive : "*"),
        (*volreq.server!='\0' ? volreq.server : "*"),volreq.dgn,
        volreq.client_name,volreq.clientUID,volreq.clientGID);

    /* Send the VDQM volume request */
    rc = vdqm_AggregatorVolReq_Send(nw,NULL,&volreq);
    if (rc == -1 ) {
      VDQM_API_RETURN(rc);
    }

    /* Receive the acknowledge */
    rc = vdqm_RecvAckn(nw);
    TRACE(1,"vdqm",
        "vdqm_CreateRequestForAggregator() vdqm_RecvAckn() rc = 0x%x",rc);
    if ( rc != VDQM_COMMIT ) {
      if ( rc > 0 ) {
          serrno = rc;
      }
      VDQM_API_RETURN(-1);
    }

    /* Clear the VDQM volume request */
    memset(&volreq,'\0',sizeof(volreq));

    /* Receive the reply */
    rc = vdqm_AggregatorVolReq_Recv(nw,NULL,&volreq);
    save_serrno = serrno;
    TRACE(1,"vdqm",
        "vdqm_CreateRequestForAggregator() received rc=%d, VolReqID=%d", rc,
        volreq.VolReqID);
    if ( rc == -1 ) {
        serrno = save_serrno;
        VDQM_API_RETURN(rc);
    }

    /* Extract the request ID and return success */
    *reqID = volreq.VolReqID;
    VDQM_API_RETURN(0);
}


int vdqm_QueueRequestForAggregator(vdqmnw_t *nw) {
    int rc = 0;

    VDQM_API_ENTER(vdqm_ConfirmRequestForAggregator);

    if ( nw == NULL ) {
        TRACE(1,"vdqm",
            "vdqm_QueueRequestForAggregator() called with nw == NULL");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }
    if ( nw->connect_socket == -1 ) {
        TRACE(1,"vdqm",
          "vdqm_QueueRequestForAggregator() called with invalid socket");
        serrno = EINVAL;
        VDQM_API_RETURN(-1);
    }

    rc = vdqm_AcknCommit(nw);
    VDQM_API_RETURN(rc);
}
