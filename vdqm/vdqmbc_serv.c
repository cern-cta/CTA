/*
 * Copyright (C) 2001 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vdqmbc_serv.c,v $ $Revision: 1.1 $ $Date: 2001/02/05 11:06:20 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * vdqmbc_serv.c - Receive VDQM replica updates and broadcast them to connnected clients.
 */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <time.h>

#if !defined(_WIN32)
#include <regex.h>
#endif /* _WIN32 */

#include <osdep.h>
#include <common.h>
#include <net.h>
#include <log.h>
#include <serrno.h>
#include <Castor_limits.h>
#include <Cthread_api.h>
#include <vdqm_constants.h>
#include <vdqm.h>

#if (defined(_REENTRANT) || defined(_THREAD_SAFE)) && !defined(_WIN32)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* (_REENTRANT || _THREAD_SAFE) && !_WIN32 */

#define FREE_LIST(X,Y) {(Y)=(X); while((Y)!=NULL) { \
    CLIST_DELETE((X),(Y)); free((X)); (X)=(Y);}}

static int success = 0;
static int failure = -1;

extern int hold;

void *update_lock, *client_lock;
extern dgn_element_t *dgn_queues;
extern void *vdqm_ReplicaListenThread _PROTO((void *));
int vdqm_shutdown, vdqm_restart;

typedef struct update_queue {
    vdqmHdr_t hdr;
    vdqmVolReq_t *vol;
    vdqmDrvReq_t *drv;
    struct update_queue *next;
} update_queue_t;
update_queue_t *oldest = NULL;
update_queue_t *newest = NULL;

typedef struct client_list {
    vdqmnw_t nw;
    int cleanup;
    struct client_list *next;
    struct client_list *prev;
} client_list_t;
static client_list_t *clients = NULL;

static int InitQueueLock(void **queue, void **fast_lock) {
    int rc, save_serrno;
    rc = Cthread_mutex_lock(queue);
    save_serrno = serrno;
    if ( rc == -1 ) {
        log(LOG_ERR,"InitQueueLock(): Cthread_mutex_lock(): %s\n",
            sstrerror(save_serrno));
        serrno = save_serrno;
        return(-1);
    }
    rc = Cthread_mutex_unlock(queue);
    save_serrno = serrno;
    if ( rc == -1 ) {
        log(LOG_ERR,"InitQueueLock(): Cthread_mutex_unlock(): %s\n",
            sstrerror(save_serrno));
        serrno = save_serrno;
        return(-1);
    }
    *fast_lock = Cthread_mutex_lock_addr(queue);
    save_serrno = serrno;
    if ( fast_lock == NULL ) {
        log(LOG_ERR,"InitQueueLock() Cthread_mutex_lock_addr(): %s\n",
            sstrerror(save_serrno));
        serrno = save_serrno;
        return(-1);
    }
    return(0);
}

static int LockQueue(void *lock) {
    return(Cthread_mutex_lock_ext(lock));
}
static int UnlockQueue(void *lock) {
    return(Cthread_mutex_unlock_ext(lock));
}

static int AddUpdate(vdqmHdr_t *hdr, vdqmVolReq_t *vol, vdqmDrvReq_t *drv) {
    update_queue_t *tmp; 
    if ( vol == NULL && drv == NULL ) return(0);
    
    tmp = (update_queue_t *)calloc(1,sizeof(update_queue_t));
    if ( tmp == NULL ) return(-1);
    tmp->next = NULL;
    tmp->hdr = *hdr;
    if ( vol != NULL ) {
        tmp->vol = (vdqmVolReq_t *)calloc(1,sizeof(vdqmVolReq_t)); 
        *tmp->vol = *vol;
    }
    if ( drv != NULL ) {
        tmp->drv = (vdqmDrvReq_t *)calloc(1,sizeof(vdqmDrvReq_t));
        *tmp->drv = *drv;
    }

    if ( LockQueue(update_lock) == -1 ) return(-1);
    if ( oldest == NULL ) {
        oldest = tmp;
        newest = NULL;
        if ( Cthread_cond_broadcast_ext(update_lock) == -1 ) {
            log(LOG_ERR,"AddUpdate() Cthread_cond_broadcast_ext(): %s\n",
                sstrerror(serrno));
            UnlockQueue(update_lock);
            return(-1);
        }
    }
    if ( newest != NULL ) newest->next = tmp;
    else newest = tmp;
    return(UnlockQueue(update_lock));
} 

static int NextUpdate(vdqmHdr_t *hdr, vdqmVolReq_t *vol, vdqmDrvReq_t *drv) {
    update_queue_t *tmp;

    if ( vol == NULL || drv == NULL ) return(-1);
    memset(vol,'\0',sizeof(vdqmVolReq_t));
    memset(drv,'\0',sizeof(vdqmDrvReq_t));

    if ( LockQueue(update_lock) == -1 ) return(-1);
    while ( oldest == NULL ) {
        if ( Cthread_cond_wait_ext(update_lock) == -1 ) {
            log(LOG_ERR,"NextUpdate() Cthread_cond_wait_ext(): %s\n",
                sstrerror(serrno));
            UnlockQueue(update_lock);
            return(-1);
        }
    }
    tmp = oldest;
    if ( tmp != NULL ) oldest = tmp->next;
    if ( UnlockQueue(update_lock) == -1 ) return(-1);
    /*
     * cannot happen ...
     */
    if ( tmp == NULL ) return(0);

    if ( hdr != NULL ) *hdr = tmp->hdr;
    if ( tmp->vol != NULL ) {
        *vol = *tmp->vol;
        free(tmp->vol);
    }
    if ( tmp->drv != NULL ) {
        *drv = *tmp->drv;
        free(tmp->drv);
    }
    free(tmp);
    return(1);
}

static int InitNWBroadcast(vdqmnw_t **nw) {
    char env[20];
    int rc, vdqm_bc_port;
#if !defined(VDQMBC_PORT)
    vdqm_bc_port = 8890;
#else
    vdqm_bc_port = VDQMBC_PORT;
#endif 
    rc = vdqm_InitNWOnPort(nw,vdqm_bc_port);
    return(rc);
}

void CleanUpClients() {
    client_list_t *client;
    int more_cleanup = 1;

    client = clients;
    while ( more_cleanup == 1 ) {
        CLIST_ITERATE_BEGIN(clients,client) {
            if ( client->cleanup == 1 ) break;
        } CLIST_ITERATE_END(clients,client);
        if ( clients != NULL && client != NULL && client->cleanup == 1 ) {
            CLIST_DELETE(clients,client);
            free(client);
        } else more_cleanup = 0;
    } 
    return;
}

static int AddClient(vdqmnw_t *nw) {
    client_list_t *client = NULL;
    int rc;

    log(LOG_INFO,"AddClient() New client connection\n");
    if ( nw == NULL || nw->accept_socket == INVALID_SOCKET ) {
        log(LOG_ERR,"AddClient() invalid arguments nw=0x%x, nw->socket=%d\n",
            nw,(nw != NULL ? nw->accept_socket : -1));
        return(-1);
    }

    client = (client_list_t *)calloc(1,sizeof(client_list_t));
    if ( client == NULL ) return(-1);
    client->nw = *nw;
    client->cleanup = 0;
    if ( LockQueue(client_lock) == -1 ) {
        log(LOG_ERR,"AddClient() failed to lock client queue: %d %d\n",
            errno,serrno);
        free(client);
        return(-1);
    }

    if ( vdqm_LockAllQueues() == -1 ) {
        log(LOG_ERR,"AddClient() failed to lock all DGN queues for dump: %d %d\n",
            errno,serrno);
        return(-1);
    }
    log(LOG_DEBUG,"AddClient() dump queues to new client\n");
    rc = vdqm_DumpQueues(nw);
    if ( rc == -1 ) {
        (void)vdqm_UnlockAllQueues();
        UnlockQueue(client_lock);
        free(client);
        return(-1);
    }
    if ( vdqm_UnlockAllQueues() == -1 ) {
        UnlockQueue(client_lock);
        free(client);
        return(-1);
    }

    CLIST_INSERT(clients,client);
    if ( UnlockQueue(client_lock) == -1 ) return(-1);

    log(LOG_INFO,"AddClient() client at socket %d added\n",
        nw->accept_socket);
    return(0);
}

void *vdqm_BroadcastThread(void *arg) {
    vdqmVolReq_t *vol, *tmpvol;
    vdqmDrvReq_t *drv, *tmpdrv;
    vdqmHdr_t hdr;
    client_list_t *client;
    int rc, do_cleanup;

    vol = (vdqmVolReq_t *)calloc(1,sizeof(vdqmVolReq_t));
    drv = (vdqmDrvReq_t *)calloc(1,sizeof(vdqmDrvReq_t));
    log(LOG_DEBUG,"vdqm_BroadcastThread() entered\n");
    while ( NextUpdate(&hdr,vol,drv) == 1 ) {
        log(LOG_DEBUG,"vdqm_BroadcastThread() new update\n");
        tmpvol = NULL;
        tmpdrv = NULL;
        if ( vol->VolReqID > 0 ) tmpvol = vol;
        if ( drv->DrvReqID > 0 ) tmpdrv = drv; 
        do_cleanup = 0;
        if ( LockQueue(client_lock) == -1 ) return((void *)&failure);
        CLIST_ITERATE_BEGIN(clients,client) {
            log(LOG_DEBUG,"vdqm_BroadcastThread() propagate update to client at socket %d\n",client->nw.accept_socket);
            rc = vdqm_SendReq(&client->nw,&hdr,tmpvol,tmpdrv);
            if ( rc == -1 ) {
                (void)vdqm_CloseConn(&client->nw);
                client->cleanup = 1;
                do_cleanup = 1;
            } 
        } CLIST_ITERATE_END(clients,client);
        if ( do_cleanup == 1 ) CleanUpClients();
        if ( UnlockQueue(client_lock) == -1 ) return((void *)&failure);
        memset(vol,'\0',sizeof(vdqmVolReq_t));
        memset(drv,'\0',sizeof(vdqmDrvReq_t));
    } 
    return((void *)&success);
}

int vdqm_AddUpdate(vdqmHdr_t *hdr, vdqmVolReq_t *vol, vdqmDrvReq_t *drv) {
    return(AddUpdate(hdr,vol,drv));
}

int vdqmbc_main() {
    vdqmnw_t *nw = NULL;
    char *vdqm_host = NULL;
    int i, rc;
    extern int DLL_DECL (*vdqm_broadcast_upd) _PROTO((vdqmHdr_t *, vdqmVolReq_t *, vdqmDrvReq_t *));

    hold = 1;
#if !defined(_WIN32)
    signal(SIGPIPE,SIG_IGN);
#endif /* _WIN32 */

#if !defined(BCSERVER_LOGFILE)
#define BCSERVER_LOGFILE "vdqmbc.log"
#endif /* BCSERVER_LOGFILE */
    initlog("vdqmbc",LOG_INFO,BCSERVER_LOGFILE);
#if defined(__DATE__) && defined (__TIME__)
    log(LOG_INFO,"******* VDQM replication broadcast server generated at %s %s.\n",
             __DATE__,__TIME__);
#endif /* __DATE__ && __TIME__ */
    if ( (vdqm_host = getconfent("VDQM","HOST",0)) != NULL ) {
        vdqm_host = "castor5";
    }
    log(LOG_INFO,"Using primary host %s\n",vdqm_host);

    rc = vdqm_InitQueueLock();
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_InitQueueLock() %d %d\n",serrno,errno);
        exit(1);
    }

    rc = InitQueueLock((void *)&oldest,&update_lock);
    if ( rc == -1 ) log(LOG_ERR,"InitQueueLock() %d %d\n",serrno,errno);
    rc = InitQueueLock((void *)&clients,&client_lock);
    if ( rc == -1 ) log(LOG_ERR,"InitQueueLock() %d %d\n",serrno,errno);

    vdqm_broadcast_upd = (int (*) _PROTO((vdqmHdr_t *, vdqmVolReq_t *, vdqmDrvReq_t *)))vdqm_AddUpdate;
    rc = Cthread_create_detached((void *(*)(void *))vdqm_ReplicaListenThread,
                                 vdqm_host);
    if ( rc == -1 ) return(1);
    if ( InitNWBroadcast(&nw) == -1 ) {
        log(LOG_ERR,"InitNWBroadcast() %d %d\n",serrno,errno);
        return(1);
    }
    rc = Cthread_create_detached((void *(*)(void *))vdqm_BroadcastThread,NULL);
    if ( rc == -1 ) return(1);

    for (;;) {
        rc = vdqm_Listen(nw);
        if ( rc == -1 ) {
            log(LOG_ERR,"main() vdqm_Listen(): %s\n",sstrerror(serrno));
            return(1);
        }
        AddClient(nw);
    }

    return(0);
} 
int main() {

#if defined(_WIN32)
    if ( Cinitservice("vdqmbc",vdqmbc_main) == -1 ) exit(1);
#else /* _WIN32 */
    if ( Cinitdaemon("vdqmbc",NULL) == -1 ) exit(1);
    exit(vdqmbc_main(NULL));
#endif /* _WIN32 */
}

