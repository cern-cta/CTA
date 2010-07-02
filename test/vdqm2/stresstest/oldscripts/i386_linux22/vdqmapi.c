#include <stdlib.h>
#if defined(DEBUG)
#include <stdio.h>
#endif /* DEBUG */
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
#include <net.h>
#include <log.h>
#include <vdqm_constants.h>
#include <vdqm.h>
#include <vdqm_api.h>


#if !defined(sys_errlist) && !defined(linux)
extern char *sys_errlist[];
#else /* linux */
#include <stdio.h>   /* Contains definition of sys_errlist[] */
#endif /* sys_errlist && linux */


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
	int rc;

	if ( nw != NULL && *nw != NULL && (*nw)->connect_socket != INVALID_SOCKET ) return(0);
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
	} else {
#if defined(VDQM_HOST)
		strcpy(vdqm_host,VDQM_HOST);
#endif /* VDQM_HOST */
	}
	if ( vdqm_host != NULL ) p = strtok(vdqm_host,", \t");
	else return(-1);
    vdqm_primary = p;
    p = strtok(NULL,", \t");
    vdqm_replica = p;

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
	if ( vdqm_port < 0 ) {
#ifdef DEBUG
		fprintf(stderr,"vdqm_Connnect() vdqm_port = %d\n",vdqm_port);
#endif /* DEBUG */
		return(-1);
	}
	if ( *nw == NULL ) {
		*nw = (vdqmnw_t *)calloc(1,sizeof(vdqmnw_t));
		if ( *nw == NULL ) {
#ifdef DEBUG
			fprintf(stderr,"vdqm_Connect() malloc() %s\n",ERRTXT);
#endif /* DEBUG */
			return(-1);
		}
		(*nw)->listen_socket = (*nw)->accept_socket = 
			(*nw)->connect_socket = INVALID_SOCKET;
	}

	(*nw)->connect_socket = socket(AF_INET,SOCK_STREAM,0);
	if ( (*nw)->connect_socket == INVALID_SOCKET ) {
#ifdef DEBUG
		fprintf(stderr,"vdqm_Connect() socket(): %s\n",NWERRTXT);
#endif /* DEBUG */
        free(*nw);
		return(-1);
	}
    try_host = vdqm_primary;
    /* 
     * Loop of primary and replica hosts
     */
    for (;;) {
    	if ( (hp = gethostbyname(try_host)) == NULL ) {
#ifdef DEBUG
	    	fprintf(stderr,"vdqm_Connect(): gethostbyname(%s) %s\n",
		    	try_host,NWERRTXT);
#endif /* DEBUG */
            free(*nw);
    		return(-1);
	    }
    	sin.sin_port = htons((short)vdqm_port);
    	sin.sin_family = AF_INET;
    	sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

    	if ( connect((*nw)->connect_socket,(struct sockaddr *)&sin, sizeof(struct sockaddr_in)) ==
    		SOCKET_ERROR) {
#ifdef DEBUG
    		fprintf(stderr,"vdqm_Connect() connect(): %s\n",NWERRTXT);
#endif /* DEBUG */
            if ( vdqm_replica == NULL || vdqm_replica == try_host ) {
                closesocket((*nw)->connect_socket);
                free(*nw);
        		return(-1);
            } else {
                try_host = vdqm_replica;
                continue;
            }
        }
        break;
    } /* for (;;) */
	return(0);
}

int vdqm_Disconnect(vdqmnw_t **nw) {
	int rc;

    if ( nw == NULL || *nw == NULL ) return(-1);
	if ( (*nw)->connect_socket != INVALID_SOCKET ) {
		if ( (rc = shutdown((*nw)->connect_socket,2)) == SOCKET_ERROR ) {
#ifdef DEBUG
			fprintf(stderr,"vdqm_Disconnect() shutdown():%s\n",NWERRTXT);
#endif /* DEBUG */
			return(-1);
		}
		if ( (rc = closesocket((*nw)->connect_socket)) == SOCKET_ERROR ) {
#ifdef DEBUG
			fprintf(stderr,"vdqm_Disconnect() closesocket(): %s\n",
				NWERRTXT);
#endif /* DEBUG */
			return(-1);
		}
		free(*nw);
		*nw = NULL;
	}
	return(0);
}

int vdqm_SendVolReq(vdqmnw_t *nw,
					int  *reqID,
                    char *VID, 
                    char *dgn,
                    char *server, 
                    char *unit, 
                    int client_port) {
	vdqmVolReq_t volreq;
	vdqmnw_t *tmpnw = NULL;
	int rc = 0;
	memset(&volreq,'\0',sizeof(vdqmVolReq_t));
	if ( nw != NULL && nw->connect_socket == INVALID_SOCKET ||
		VID == NULL || dgn == NULL || client_port < 0 ) return(-1);
	if ( nw == NULL ) {
		rc = vdqm_Connect(&tmpnw);
		if ( rc < 0 ) return(rc);
	} else tmpnw = nw;
	strcpy(volreq.volid,VID);
	strcpy(volreq.dgn,dgn);
	volreq.client_port = client_port;
	if ( unit != NULL ) strcpy(volreq.drive,unit);
	if ( server != NULL ) strcpy(volreq.server,server);
	volreq.clientUID = getuid();
	volreq.clientGID = getgid();
	rc = vdqm_SendReq(tmpnw,NULL,&volreq,NULL);
	if ( rc != -1 ) {
		rc = vdqm_RecvAckn(tmpnw);
#ifdef DEBUG
		fprintf(stderr,"vdqm_SendVolReq() vdqm_RecvAckn() rc = 0x%x\n",
			rc);
#endif /* DEBUG */
		if ( rc == VDQM_COMMIT ) {
			rc = vdqm_RecvReq(tmpnw,NULL,&volreq,NULL);
			if ( rc != -1 ) {
				rc = vdqm_AcknCommit(tmpnw);
				rc = 0;
			}
		}
		else rc = -1;
	}
	if ( nw == NULL ) vdqm_Disconnect(&tmpnw);
	if ( reqID != NULL ) *reqID = volreq.VolReqID;
	return(rc);
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
	int rc = 0;
    memset(&hdr,'\0',sizeof(hdr));
	memset(&volreq,'\0',sizeof(vdqmVolReq_t));
	if ( nw != NULL && nw->connect_socket == INVALID_SOCKET ||
		reqID <= 0 || VID == NULL || dgn == NULL || client_port < 0 ) return(-1);
	if ( nw == NULL ) {
		rc = vdqm_Connect(&tmpnw);
		if ( rc < 0 ) return(rc);
	} else tmpnw = nw;
	strcpy(volreq.volid,VID);
	strcpy(volreq.dgn,dgn);
	volreq.client_port = client_port;
	if ( unit != NULL ) strcpy(volreq.drive,unit);
	if ( server != NULL ) strcpy(volreq.server,server);
	volreq.clientUID = getuid();
	volreq.clientGID = getgid();
    volreq.VolReqID = reqID;
    hdr.reqtype = VDQM_DEL_VOLREQ;
	rc = vdqm_SendReq(tmpnw,&hdr,&volreq,NULL);
	if ( rc != -1 ) {
		rc = vdqm_RecvAckn(tmpnw);
#ifdef DEBUG
		fprintf(stderr,"vdqm_DelVolReq() vdqm_RecvAckn() rc = 0x%x\n",
			rc);
#endif /* DEBUG */
		if ( rc == VDQM_COMMIT ) {
			rc = vdqm_RecvReq(tmpnw,&hdr,&volreq,NULL);
			if ( rc != -1 ) {
				rc = vdqm_AcknCommit(tmpnw);
				rc = 0;
			}
		}
		else rc = -1;
	}
	if ( nw == NULL ) vdqm_Disconnect(&tmpnw);
	return(rc);
}

int vdqm_NextVol(vdqmnw_t **nw, vdqmVolReq_t *volreq) {
    vdqmHdr_t hdr;
    int rc = 0;

    if ( nw == NULL || volreq == NULL ) return(-1);
	if ( *nw != NULL && (*nw)->connect_socket == INVALID_SOCKET ) return(-1);
	if ( *nw == NULL ) {
		rc = vdqm_Connect(nw);
		if ( rc < 0 ) return(rc);
        memset(&hdr,'\0',sizeof(hdr));
        hdr.reqtype = VDQM_GET_VOLQUEUE;
#ifdef DEBUG
        fprintf(stderr,"vdqm_NextVol() dgn=%s\n",volreq->dgn);
#endif /* DEBUG */
	    rc = vdqm_SendReq(*nw,&hdr,volreq,NULL);
    }
    if ( rc != -1 ) {
        rc = vdqm_RecvReq(*nw,&hdr,volreq,NULL);
#ifdef DEBUG
        fprintf(stderr,"vdqm_NextVol(): vdqm_RecvReq() rc=%d\n",rc);
#endif /* DEBUG */
    }
    if ( rc == -1 || volreq->VolReqID == -1 ) {
		rc = vdqm_RecvAckn(*nw);
#ifdef DEBUG
		fprintf(stderr,"vdqm_NextVol() vdqm_RecvAckn() rc = 0x%x\n",
			rc);
#endif /* DEBUG */
		if ( rc == VDQM_COMMIT ) {
			rc = vdqm_RecvReq(*nw,&hdr,volreq,NULL);
			if ( rc != -1 ) {
				rc = vdqm_AcknCommit(*nw);
				rc = -1;
			}
		}
		else rc = -1;
        vdqm_Disconnect(nw);
    }
    return(rc);
}

int vdqm_UnitStatus(vdqmnw_t *nw, 
                    char *VID, 
                    char *dgn, 
                    char *server, 
                    char *unit,
                    int *status, 
                    int *value) {
	vdqmDrvReq_t drvreq;
	vdqmnw_t *tmpnw = NULL;
	int len;
	int rc = 0;

	memset(&drvreq,'\0',sizeof(vdqmDrvReq_t));
	if ( nw != NULL && nw->connect_socket == INVALID_SOCKET ||
		unit == NULL || dgn == NULL || status == NULL ) return(-1);
    if ( value == NULL && (*status & (VDQM_UNIT_ASSIGN | 
        VDQM_VOL_MOUNT | VDQM_UNIT_RELEASE)) ) return(-1);

    if ( nw == NULL ) {
		rc = vdqm_Connect(&tmpnw);
		if ( rc < 0 ) return(rc);
	} else tmpnw = nw;
	if ( VID != NULL ) strcpy(drvreq.volid,VID);
	if ( server != NULL ) strcpy(drvreq.server,server);
	else {
		len = sizeof(drvreq.server)-1;
		rc = gethostname(drvreq.server,len);
		if ( rc == SOCKET_ERROR ) {
#ifdef DEBUG
			fprintf(stderr,"vdqm_UnitStatus() gethostname() %s\n",
				NWERRTXT);
#endif /* DEBUG */
			return(-1);
		}
	}
	strcpy(drvreq.dgn,dgn);
	strcpy(drvreq.drive,unit);
	drvreq.status = *status;
    if ( value != NULL ) {
        if ( *status & VDQM_UNIT_ASSIGN ) {
            drvreq.VolReqID = *value;
            drvreq.jobID = (int)getpid();
        } else drvreq.jobID = *value;
    }

    rc = vdqm_SendReq(tmpnw,NULL,NULL,&drvreq);
	if ( rc != -1 ) {
		rc = vdqm_RecvAckn(tmpnw);
#ifdef DEBUG
		fprintf(stderr,"vdqm_SendDrvReq() vdqm_RecvAckn() rc = 0x%x\n",
			rc);
#endif /* DEBUG */
		if ( rc == VDQM_COMMIT ) {
			rc = vdqm_RecvReq(tmpnw,NULL,NULL,&drvreq);
			if ( rc != -1 ) {
				rc = vdqm_AcknCommit(tmpnw);
				rc = 0;
			}
			rc = 0;
		}
		else rc = -1;
	}
	if ( nw == NULL ) vdqm_Disconnect(&tmpnw);
	*status = drvreq.status;
	if ( value != NULL ) *value = drvreq.jobID;
	if ( VID != NULL && (drvreq.status & VDQM_VOL_UNMOUNT) ) 
		strcpy(VID,drvreq.volid);
	return(rc);
}

int vdqm_DelDrive(vdqmnw_t *nw,
                           char *dgn,
                           char *server,
                           char *unit) {
	vdqmDrvReq_t drvreq;
    vdqmHdr_t hdr;
	vdqmnw_t *tmpnw = NULL;
	int len;
	int rc = 0;

    memset(&hdr,'\0',sizeof(hdr));
	memset(&drvreq,'\0',sizeof(vdqmDrvReq_t));
	if ( nw != NULL && nw->connect_socket == INVALID_SOCKET ||
		unit == NULL || dgn == NULL ) return(-1);
	if ( nw == NULL ) {
		rc = vdqm_Connect(&tmpnw);
		if ( rc < 0 ) return(rc);
	} else tmpnw = nw;
	if ( server != NULL ) strcpy(drvreq.server,server);
	else {
		len = sizeof(drvreq.server)-1;
		rc = gethostname(drvreq.server,len);
		if ( rc == SOCKET_ERROR ) {
#ifdef DEBUG
			fprintf(stderr,"vdqm_DelDrive() gethostname() %s\n",
				NWERRTXT);
#endif /* DEBUG */
			return(-1);
		}
	}
	strcpy(drvreq.dgn,dgn);
	strcpy(drvreq.drive,unit);
    hdr.reqtype = VDQM_DEL_DRVREQ;
	rc = vdqm_SendReq(tmpnw,&hdr,NULL,&drvreq);
	if ( rc != -1 ) {
		rc = vdqm_RecvAckn(tmpnw);
#ifdef DEBUG
		fprintf(stderr,"vdqm_DelDrive() vdqm_RecvAckn() rc = 0x%x\n",
			rc);
#endif /* DEBUG */
		if ( rc == VDQM_COMMIT ) {
			rc = vdqm_RecvReq(tmpnw,&hdr,NULL,&drvreq);
			if ( rc != -1 ) {
				rc = vdqm_AcknCommit(tmpnw);
				rc = 0;
			}
			rc = 0;
		}
		else rc = -1;
	}
	if ( nw == NULL ) vdqm_Disconnect(&tmpnw);
    return(rc);
}

int vdqm_NextDrive(vdqmnw_t **nw, vdqmDrvReq_t *drvreq) {
    vdqmHdr_t hdr;
    int rc = 0;

    if ( nw == NULL || drvreq == NULL ) return(-1);
	if ( *nw != NULL && (*nw)->connect_socket == INVALID_SOCKET ) return(-1);
	if ( *nw == NULL ) {
		rc = vdqm_Connect(nw);
		if ( rc < 0 ) return(rc);
        memset(&hdr,'\0',sizeof(hdr));
        hdr.reqtype = VDQM_GET_DRVQUEUE;
	    rc = vdqm_SendReq(*nw,&hdr,NULL,drvreq);
    }
    if ( rc != -1 ) {
        rc = vdqm_RecvReq(*nw,&hdr,NULL,drvreq);
    }
    if ( rc == -1 || drvreq->DrvReqID == -1 ) {
		rc = vdqm_RecvAckn(*nw);
#ifdef DEBUG
		fprintf(stderr,"vdqm_NextDrive() vdqm_RecvAckn() rc = 0x%x\n",
			rc);
#endif /* DEBUG */
		if ( rc == VDQM_COMMIT ) {
			rc = vdqm_RecvReq(*nw,&hdr,NULL,drvreq);
			if ( rc != -1 ) {
				rc = vdqm_AcknCommit(*nw);
				rc = -1;
			}
		}
		else rc = -1;
        vdqm_Disconnect(nw);
    }
    return(rc);
}

int vdqm_admin(vdqmnw_t *nw, int admin_req) {
    vdqmHdr_t hdr;
    vdqmnw_t *tmpnw = NULL;
    int rc;

    if ( (admin_req != VDQM_SHUTDOWN) && (admin_req != VDQM_HOLD) &&
         (admin_req != VDQM_RELEASE) ) return(-1);
	if ( nw != NULL && nw->connect_socket == INVALID_SOCKET ) return(-1);
	if ( nw == NULL ) {
		rc = vdqm_Connect(&tmpnw);
		if ( rc < 0 ) return(rc);
    } else tmpnw = nw;
    memset(&hdr,'\0',sizeof(hdr));
    hdr.reqtype = admin_req;
    rc = vdqm_SendReq(tmpnw,&hdr,NULL,NULL);
	if ( rc != -1 ) {
		rc = vdqm_RecvAckn(tmpnw);
#ifdef DEBUG
		fprintf(stderr,"vdqm_admin() vdqm_RecvAckn() rc = 0x%x\n",
			rc);
#endif /* DEBUG */
		if ( rc == VDQM_COMMIT ) {
			rc = vdqm_RecvReq(tmpnw,&hdr,NULL,NULL);
			if ( rc != -1 ) {
				rc = vdqm_AcknCommit(tmpnw);
				rc = 0;
			}
			rc = 0;
		}
		else rc = -1;
	}
	if ( nw == NULL ) vdqm_Disconnect(&tmpnw);
    if ( admin_req == VDQM_SHUTDOWN ) {
        /*
         * Send a ping to wake up server to complete shutdown
         */
        rc = vdqm_PingServer(NULL,0);
    }
    return(0);
}

int vdqm_GetClientAddr(char *buf,
                       char *clienthost, 
                       int *clientport, 
                       char *dgn,
                       char *tapeunit) {
    vdqmVolReq_t volreq;
    vdqmDrvReq_t drvreq;
    int rc;

    if ( buf == NULL || clienthost == NULL || clientport == NULL || 
         dgn == NULL || tapeunit == NULL) return(-1);
    memset(&volreq,'\0',sizeof(volreq));
    memset(&drvreq,'\0',sizeof(drvreq));
    rc = vdqm_GetRTCPReq(buf,&volreq,&drvreq);
    strcpy(clienthost,volreq.client_host);
    *clientport = volreq.client_port;
    strcpy(dgn,drvreq.dgn);
    strcpy(tapeunit,drvreq.drive);
	return(rc);
}

int vdqm_AcknClientAddr(SOCKET s,
                                 int status,
                                 int errmsglen,
                                 char *errmsg) {
    int rc,l_status,l_errmsglen;

    if ( s == INVALID_SOCKET ) return(-1);
    l_status = l_errmsglen = 0;
    l_status = status;
    l_errmsglen = errmsglen;
    if ( errmsg == NULL ) l_errmsglen = 0;
    rc = vdqm_SendRTCPAckn(s,&l_status,&l_errmsglen,errmsg);
    return(rc);
}

int vdqm_PingServer(vdqmnw_t *nw,int reqID) {
	vdqmVolReq_t volreq;
	vdqmnw_t *tmpnw = NULL;
	int rc;

	memset(&volreq,'\0',sizeof(vdqmVolReq_t));
	volreq.VolReqID = reqID;
	if ( nw == NULL ) {
		rc = vdqm_Connect(&tmpnw);
		if ( rc < 0 ) return(rc);
	} else tmpnw = nw;
	rc = vdqm_SendPing(tmpnw,NULL,&volreq);
	if ( rc != -1 ) {
		rc = vdqm_RecvPingAckn(tmpnw);
	}
	if ( nw == NULL ) vdqm_Disconnect(&tmpnw);
	return(rc);
}

