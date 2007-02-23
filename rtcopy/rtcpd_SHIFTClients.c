/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcpd_SHIFTClients.c - RTCOPY routines to serve old SHIFT clients
 */

#include <stdlib.h>
#include <time.h>
#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
#else /* _WIN32 */
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#include <sys/time.h>
#endif /* _WIN32 */

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <signal.h>

#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <Cnetdb.h>
#include <Cpwd.h>
#include <log.h>
#include <osdep.h>
#include <net.h>
#include <marshall.h>
#include <Cthread_api.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <rtcp_api.h>
#include <Ctape_api.h>
#include <serrno.h>

/*
 * Special unmarshall_STRING macro to avoid unnecessary copies.
 */
#define unmarshall_STRING_nc(ptr,str) { str=ptr; INC_PTR(ptr,strlen(str)+1);}


static int rtcp_CheckClientHost(SOCKET *s, 
                                shift_client_t *req) {
    char local_host[CA_MAXHOSTNAMELEN+1];
    struct sockaddr_in from;
    struct hostent *hp;
    int fromlen,rc;

    if ( s == NULL || *s == INVALID_SOCKET || req == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    fromlen = sizeof(from);
    rc = getpeername(*s,(struct sockaddr *)&from,&fromlen);
    if ( rc == SOCKET_ERROR ) {
        rtcp_log(LOG_ERR,"rtcp_CheckClientHost() getpeername(): %s\n",
                neterror());
        return(-1);
    }
    hp = Cgethostbyaddr((void *)&(from.sin_addr),sizeof(struct in_addr),
                        from.sin_family);
    if ( hp == NULL ) {
        rtcp_log(LOG_ERR,"rtcp_CheckClientHost() Cgethostbyaddr(): h_errno=%d, %s\n",
                     h_errno, neterror());
        return(-1);
    }
    strcpy(req->clienthost,hp->h_name);

    rc = gethostname(local_host,CA_MAXHOSTNAMELEN);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcp_CheckClientHost() gethostname(): %s\n",
                 neterror());
        return(-1);
    }

    if ( strcmp(local_host,req->clienthost) == 0 ||
         (strncmp(local_host,req->clienthost,strlen(local_host)) == 0 &&
          req->clienthost[strlen(local_host)] == '.') ) {
        req->islocal = 1;
    }
    req->isoutofsite = isremote(from.sin_addr,req->clienthost);
    if ( req->isoutofsite == -1 ) return(-1);
    return(0);
}

static int rtcp_CheckClientAuth(rtcpHdr_t *hdr, shift_client_t *req) {
    int rc;
    FILE *fs;
    char buf[CA_MAXHOSTNAMELEN+1];
    int authorized, SHIFTrc;

    rc = rtcpd_CheckClient((int)req->uid,(int)req->gid,req->name,
                           req->acctstr,&SHIFTrc);
    if ( rc == -1 ) {
        if ( hdr->reqtype == RQST_INFO ) return(SHIFTrc);
        else return(SYERR);
    }

    authorized = 0;
    if ( req->islocal != 0 ) {
        rtcp_log(LOG_DEBUG,"Local access authorized\n");
        authorized = 1;
    } else {
        rtcp_log(LOG_DEBUG,"Check %s authorization in %s\n",
                 req->clienthost,AUTH_HOSTS);
        if ( (fs = fopen(AUTH_HOSTS,"r")) == NULL ) {
            rtcp_log(LOG_DEBUG,"File %s does not exist. Any host is authorized\n",AUTH_HOSTS);
            authorized = 1;
        } else {
            while( fscanf(fs, "%s%*[^\n]\n", buf) != EOF ) {
                if ( buf[0] != '#' ) { 
                    if ( strcmp(buf,"any") == 0 ) {
                        authorized = 1;
                        break;
                    }
                    if ( strcmp(buf, req->clienthost) == 0 ) {
                        authorized = 1;
                        break;
                    }
                    strcat(buf,".");
                    strcat(buf, DOMAINNAME);
                    if ( strcmp(buf, req->clienthost) == 0 ) {
                        authorized = 1;
                        break;
                    }
                }
            }
            fclose(fs);
        }
        rtcp_log(LOG_DEBUG,"Host %s %s authorized\n",req->clienthost,
                 (authorized == 0 ? "not" : ""));
    }
    if ( authorized == 0 ) {
        if ( hdr->reqtype == RQST_INFO ) return(HOSTNOTAUTH);
        else return(SYERR);
    } else {
        if ( hdr->reqtype == RQST_INFO ) return(AVAILABLE);
        else return(0);
    }
}

static int rtcp_GetOldCMsg(SOCKET *s, 
                           rtcpHdr_t *hdr,
                           shift_client_t **req) {
    int argc = -1;
    int rc, rfio_key, i;
    mode_t mask;
    char **argv = NULL;
    char *p;
    char *msgbuf = NULL;
    char *msgtxtbuf = NULL;
    static char commands[][20]={"tpread","tpwrite","dumptape"};

    if ( s == NULL || *s == INVALID_SOCKET || 
         hdr == NULL || req == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    /*
     * Header has already been received in rtcp_RecvReq()
     */
    rc = 0;
    if ( hdr->len > 0 ) {
        *req = (shift_client_t *)calloc(1,sizeof(shift_client_t));
        if ( *req == NULL ) {
            rtcp_log(LOG_ERR,"rtcp_GetOldCMsg() calloc(1,%d): %s\n",
                sizeof(shift_client_t),sstrerror(errno));
            return(-1);
        }
        msgbuf = (char *)malloc(hdr->len);
        if ( msgbuf == NULL ) {
            rtcp_log(LOG_ERR,"rtcp_GetOldCMsg() malloc(%d): %s\n",
                hdr->len,sstrerror(errno));
            return(-1);
        }

        rc = netread_timeout(*s,msgbuf,hdr->len,RTCP_NETTIMEOUT);
        switch (rc) {
        case -1:
            rtcp_log(LOG_ERR,"rtcp_GetOldCMsg() netread(%d): %s\n",
                hdr->len,neterror());
            free(msgbuf);
            return(-1);
        case 0:
            rtcp_log(LOG_ERR,"rtcp_GetOldCMsg() netread(%d): connection dropped\n",
                hdr->len);
            free(msgbuf);
            return(-1);
        }
        rc = rtcp_SendOldCAckn(s,hdr);
        if ( rc == -1 ) {
            free(msgbuf);
            return(-1);
        }

        p = msgbuf;
        unmarshall_STRING(p,(*req)->name);
        unmarshall_WORD(p,(*req)->uid);
        unmarshall_WORD(p,(*req)->gid);
        if ( hdr->reqtype == RQST_INFO ) {
            unmarshall_STRING(p,(*req)->dgn);
            free(msgbuf);
        } else if ( hdr->reqtype == RQST_DKTP ||
                    hdr->reqtype == RQST_TPDK ||
                    hdr->reqtype == RQST_DPTP ) {
            if ( hdr->magic == RTCOPY_MAGIC_SHIFT ) unmarshall_WORD(p,rfio_key);
            unmarshall_WORD(p,mask);
            umask(mask);
            unmarshall_LONG(p,argc);
            if ( argc < 0 ) {
                rtcp_log(LOG_ERR,"rtcp_GetOldCMsg() argc=%d received\n",argc);
                free(msgbuf);
                serrno = SEINTERNAL;
                return(-1);
            }
            argv = (char **)malloc((argc+1) * sizeof(char *));
            if ( argv == NULL ) {
                rtcp_log(LOG_ERR,"rtcp_GetOldCMsg() malloc(%d): %s\n",
                         (argc+1) * sizeof(char *),sstrerror(errno));
                free(msgbuf);
                serrno = SEINTERNAL;
                return(-1);
            }
            if ( hdr->reqtype == RQST_TPDK ) argv[0] = commands[0];
            else if ( hdr->reqtype == RQST_DKTP ) argv[0] = commands[1];
            else if ( hdr->reqtype == RQST_DPTP ) argv[0] = commands[2];
            
            /*
             * Don't duplicate the strings. We must make sure
             * to not free msgbuf until the command has been parsed.
             */
            for (i=1;i<argc; i++) unmarshall_STRING_nc(p,argv[i]);
            argv[argc] = NULL;
            msgtxtbuf = (char *)calloc(1,RTCP_SHIFT_BUFSZ);
            rtcp_InitLog(msgtxtbuf,NULL,NULL,s);
            if ( hdr->reqtype == RQST_DPTP ) {
                rc = rtcpc_BuildDumpTapeReq(&((*req)->tape),argc,argv);
            } else {
                rc = rtcpc_BuildReq(&((*req)->tape),argc,argv);
            }
            rtcp_InitLog(NULL,NULL,NULL,NULL);
            free(msgtxtbuf);
            free(msgbuf);
            free(argv);
        } else {
            rtcp_log(LOG_ERR,"rtcp_GetOldCMsg() unknown request type 0x%x\n",
                hdr->reqtype);
            free(msgbuf);
            serrno = SEINTERNAL;
            return(-1);
        }
    } else rc = rtcp_SendOldCAckn(s,hdr);
    return(rc);
}


static int rtcp_GetOldCinfo(rtcpHdr_t *hdr, shift_client_t *req) {
    char server[CA_MAXHOSTNAMELEN+1];
    int namelen = CA_MAXHOSTNAMELEN;
    int nb_queued, nb_used, nb_units, rc;
    extern char *getconfent _PROTO((const char *, const char *, int));


    if ( req == NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    nb_queued = nb_used = nb_units = 0;

    memset(&req->info,'\0',sizeof(tpqueue_info_t));
    if ( rtcpd_CheckNoMoreTapes() != 0 ) {
        req->info.status = NOTAVAIL;
        return(0);
    }
    rc = rtcp_CheckClientAuth(hdr,req);
    if ( rc != AVAILABLE ) {
        req->info.status = rc;
        return(0);
    }
    gethostname(server,namelen);

    rc = rtcpc_GetDeviceQueues(req->dgn,server,&nb_queued,&nb_units,&nb_used);

    if ( rc == -1 || nb_units == 0 ) req->info.status = ALLDOWN;
    else {
        req->info.status = AVAILABLE;
        req->info.nb_queued = nb_queued;
        req->info.nb_units = nb_units;
        req->info.nb_used = nb_used;
    }
    return(0);
}

static int rtcp_SendRC(SOCKET *s,
                        rtcpHdr_t *hdr,
                        int *status,
                        shift_client_t *req) {
    int retval, rc;
    char msgbuf[4*LONGSIZE], *p;

    if ( s == NULL || *s == INVALID_SOCKET || hdr == NULL || 
         status == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    if ( req != NULL && req->tape != NULL ) {
        rc = rtcp_RetvalSHIFT(req->tape,NULL,&retval);
        if ( rc == -1 ) retval = UNERR;
        *status = retval;
    } else retval = *status;
    p = msgbuf;
    marshall_LONG(p,hdr->magic);
    marshall_LONG(p,GIVE_RESU);
    marshall_LONG(p,LONGSIZE);
    marshall_LONG(p,retval);
    rc = netwrite_timeout(*s, msgbuf, 4*LONGSIZE,RTCP_NETTIMEOUT);
    switch (rc) {
    case -1:
        rtcp_log(LOG_ERR,"rtcp_SendRC() netwrite(): %s\n",neterror());
        return(-1);
    case 0:
        rtcp_log(LOG_ERR,"rtcp_SendRC() netwrite(): connection dropped\n");
        return(-1);
    }
    return(0);
}
    
int rtcpd_CleanUpSHIFT(shift_client_t **req, char **buf, int status) {
    if ( req == NULL || *req == NULL ) return(status);
    rtcpc_FreeReqLists(&((*req)->tape));
    free(*req);
    if ( buf != NULL && *buf != NULL ) {
        free(*buf);
        buf = NULL;
    }
    *req = NULL;
    return(status);
}


int rtcp_RunOld(SOCKET *s, rtcpHdr_t *hdr) {
    int rc, retval, status, namelen, CLThId;
    shift_client_t *req = NULL;
    char envacct[20];
    char *client_msg_buf = NULL;

    if ( s == NULL || *s == INVALID_SOCKET || hdr == NULL ) {
        serrno = EINVAL;
        if ( s != NULL && *s == INVALID_SOCKET ) serrno = SECOMERR;
        return(-1);
    }

    client_msg_buf = (char *)calloc(1,RTCP_SHIFT_BUFSZ);
    if ( client_msg_buf == NULL ) {
        rtcp_log(LOG_ERR,"rtcp_RunOld() calloc(1,%d): %s\n",
                 RTCP_SHIFT_BUFSZ,sstrerror(errno));
        serrno = SESYSERR;
        return(-1);
    }

    rtcp_log(LOG_DEBUG,"rtcp_RunOld() Get SHIFT request\n");

    rc = rtcp_GetOldCMsg(s,hdr,&req);
    rtcp_log(LOG_DEBUG,"rtcp_RunOld() rtcp_GetOldCMsg(): rc=%d\n",rc);
    if ( rc == -1 ) {
        retval = USERR;
        if ( hdr->reqtype != RQST_INFO ) (void) rtcp_SendRC(s,hdr,&retval,NULL);
        return(rtcpd_CleanUpSHIFT(&req,&client_msg_buf,-1));
    }

    rc = rtcp_CheckClientHost(s,req);
    if ( rc == -1 ) return(rtcpd_CleanUpSHIFT(&req,&client_msg_buf,-1));

    if ( hdr->reqtype == RQST_INFO ) {
        rtcp_log(LOG_INFO,"info request by user %s (%d,%d) from %s\n",
                 req->name,req->uid,req->gid,req->clienthost);

        rc = rtcp_GetOldCinfo(hdr,req);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"rtcp_RunOld() rtcp_GetOldCinfo(): %s\n",sstrerror(serrno));
            return(rtcpd_CleanUpSHIFT(&req,&client_msg_buf,-1));
        }
        rtcp_log(LOG_DEBUG,"info request returns status=%d, used=%d, queue=%d, units=%d\n",req->info.status,req->info.nb_used,req->info.nb_queued,req->info.nb_units);

        rc = rtcp_SendOldCinfo(s,hdr,req);

        if ( rc == -1 ) return(rtcpd_CleanUpSHIFT(&req,&client_msg_buf,-1));

        return(rtcpd_CleanUpSHIFT(&req,&client_msg_buf,0));
    }

    CLThId = -1;
    /*
     * Start the request
     */
    /*
     * Redirect logging to client socket
     */
    rc = rtcp_InitLog(client_msg_buf,NULL,NULL,s);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcp_RunOld() rtcp_InitLog(): %s\n",
                 sstrerror(errno));
        return(rtcpd_CleanUpSHIFT(&req,&client_msg_buf,-1));
    }
    /*
     * Check access
     */
    retval = rtcp_CheckClientAuth(hdr,req);
    while ( retval == 0 ) { 
        if ( CLThId == -1 ) {
#if !defined(_WIN32)
            if ( setgid((gid_t)req->gid) == -1 ) {
                rtcp_log(LOG_ERR,"setgid(%d): %s\n",req->gid,sstrerror(errno));
                return(rtcpd_CleanUpSHIFT(&req,&client_msg_buf,-1));
            }
            if ( setuid((uid_t)req->uid) == -1 ) {
                rtcp_log(LOG_ERR,"setuid(%d): %s\n",req->uid,sstrerror(errno));
                return(rtcpd_CleanUpSHIFT(&req,&client_msg_buf,-1));
            }
            if ( *req->acctstr != '\0' ) {
                (void) sprintf(envacct,"ACCOUNT=%s",req->acctstr);
                if ( putenv(envacct) != 0 ) {
                    rtcp_log(LOG_ERR,"putenv(%s) failed\n",envacct);
                    return(rtcpd_CleanUpSHIFT(&req,&client_msg_buf,-1));
                }
            }
#endif /* !_WIN32 */

            /*
             * Kick off client listen thread to answer pings etc.
             */
            CLThId = rtcpd_ClientListen(*s);
            if ( CLThId == -1 ) return(rtcpd_CleanUpSHIFT(&req,&client_msg_buf,-1));
        }
        namelen = sizeof(req->tape->tapereq.server)-1;
        gethostname(req->tape->tapereq.server,namelen);
 
        rc = rtcpc(req->tape);

        if ( rc == -1 && serrno == ERTUSINTR ) {
            (void)rtcp_CloseConnection(s);
            break;
        } else if ( rc == -1 && serrno == ETVBSY ) {
            /*
             * Infinite retry loop while volume is busy.
             */
            *req->tape->tapereq.err.errmsgtxt = '\0';
            req->tape->tapereq.err.errorcode = 0;
            req->tape->tapereq.err.severity = RTCP_OK;
            req->tape->tapereq.tprc = 0; 
            sleep(60);
        } else if ( rc == -1 && req->tape->file != NULL && 
                    *req->tape->file->filereq.stageID != '\0' &&
                    req->tape->file->filereq.stageSubreqID == -1 ) {
            /*
             * If the request comes from a SHIFT stager we cannot allow
             * for retries on another server because the SHIFT tpread doesn't
             * know where to restart a partially successful request.
             */
            (void)rtcp_RetvalSHIFT(req->tape,NULL,&retval);
            if ( retval == RSLCT ) {
                sleep(60);
                *req->tape->tapereq.unit = '\0';
                retval = 0;
            } else break;
        } else break;
    }
    (void) rtcp_SendRC(s,hdr,&retval,req);

    rtcp_log = (void (*)(int, const char *, ...))log;
    rtcp_log(LOG_DEBUG,"rtcp_RunOld() sent back status=%d to client\n",
             retval);

    if ( CLThId != -1 ) (void)rtcpd_WaitCLThread(CLThId,&status);
    return(rtcpd_CleanUpSHIFT(&req,&client_msg_buf,rc));
}

