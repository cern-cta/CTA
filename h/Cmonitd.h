/*
 * Copyright (C) 2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* 
 * monitor.h - Header for the CASTOR monitoring module  
 */

#ifndef _CMONITD_H_INCLUDED_
#define _CMONITD_H_INCLUDED_

#include "Castor_limits.h"
#include "Cmonit_api.h"
#include "Cnetdb.h"

#if defined(_WIN32)
#include <winsock2.h>
#include "Ctape.h"
#else
#include <netinet/in.h>
#include <arpa/inet.h>
#endif

#include "net.h"
#include "Cmonit_structs.h"
#include "Cmonit_constants.h"
#include "Cmonitd_lock.h"
#include "Cmonit_api.h"
 
#ifdef LOGFILE
#undef LOGFILE
#endif

#if defined(_WIN32)
#define LOGFILE "C:\log"
#else
#define LOGFILE "/usr/spool/monitor/log"
#endif

#define     SYERR     2     /* system error */


/* ---------------------------------------
 * Function declarations
 * --------------------------------------- */

void *Cmonit_recv_from_clients(void *);
void *Cmonit_process_stager_request(void *);
void *Cmonit_process_stager_list_request(void *);
void *Cmonit_process_stager_detail_request(void *);
void *Cmonit_process_tape_status_request(void *);
void *Cmonit_process_tapeserver_status_request(void *);
void *Cmonit_send_request_not_recognized(void *);

void Cmonit_exit(int val);
void *Cmonit_recv_from_daemons(void *);
void *Cmonit_check_stagers(void *);
/*  int Cmonit_processmsg_stager(char *buf, int nb_recv,  struct sockaddr_in *stager_host); */
int Cmonit_processmsg_tape(char *buf, int nb_recv, struct sockaddr_in *stager_host);
int Cmonit_processmsg_rtcopy(char *buf, int nb_recv, struct sockaddr_in *stager_host);
/*  void Cmonit_update_fs_status(struct Cmonit_fs_entry *entry); */
void Cmonit_update_tape_status(struct Cmonit_tape_entry *entry);
void Cmonit_update_rtcopy_status(struct Cmonit_tape_entry *entry);
/*  void Cmonit_check_stage_init(time_t last_init_time,  struct sockaddr_in *stager_host); */
/*  void Cmonit_initialize_stager_fs(unsigned int stager_host); */
void Cmonit_resolve_tape_uidgid(struct Cmonit_tape_entry *entry);
struct Cmonit_tape_entry *Cmonit_get_tape_entry(unsigned int host, char *drive);
struct Cmonit_tape_entry *Cmonit_get_tape_entry_by_jid(unsigned int host, int jid);
struct Cmonit_server_entry  *check_exist_Cmonit_server_entry(struct Cmonit_server_entry **serverlistptr,
							     struct sockaddr_in *host);
struct Cmonit_server_entry  *get_Cmonit_server_entry(struct Cmonit_server_entry *server_list, 
						     unsigned int ip);
struct Cm_stager *get_stager(struct sockaddr_in *stager_host);
int read_migrator_record(char **p, struct Cm_stager *stager);
int read_pool_record(char **p, struct Cm_stager *stager);
int read_record(char **p, struct Cm_stager *stager);
struct Cm_pool *get_pool(struct Cm_stager *stager,char *poolname);
struct Cm_migrator *get_migrator(struct Cm_stager *stager,char *migrname);
int Cmonit_processmsg_stager_v2 (char *buf, int nb_recv, struct sockaddr_in *stager_host);


void monitor_util_time(time_t this,char *timestr);
int lookup_monitor_port(void);
int lookup_client_request_port(void);


/* ---------------------------------------
 * Global Variables for Data Locking
 * --------------------------------------- */

Cthread_rwlock_t fs_lock;    /* Used to lock the Access to the Filesystems data */
Cthread_rwlock_t tape_lock;    /* Used to lock the Access to the Tape data */

/* Struct used for argument passing */
struct Cmonit_client_request_arg {
  int sd;
  struct sockaddr_in *clientAddress;
  char param[MAXPARAMLEN+1];
};

/* ---------------------------------------
 * Global Variables Containing System status
 * --------------------------------------- */

/* Head of the linked list of stager servers */
/*  extern struct Cmonit_server_entry *stgserver_list; */
/* Head of the linked list of pool entries */
/*  extern struct Cmonit_fs_entry *fs_entry_list; */

/* Head of the list of stagers */
extern struct Cm_stager *stagers;

/* Head of the linked list of tape  servers */
extern struct Cmonit_server_entry *tapeserver_list;

/* Head of the linked list of Tapes */
extern struct Cmonit_tape_entry *tape_entry_list;


/* Variables for threading / thread pools */
extern int client_tpool_id;
extern int actual_thread_nb;

/* ---------------------------------------
 * MISC Defines
 * --------------------------------------- */

#define CMONIT_COMMANDLEN 20 /* Maximum size for the castor command strings */

#ifdef SIXMONTHS
#undef SIXMONTHS
#endif
#define SIXMONTHS (6*30*24*60*60)

#if defined(_WIN32)
static char strftime_format_sixmonthsold[] = "%b %d %Y";
static char strftime_format[] = "%b %d %y %H:%M:%S";
#else /* _WIN32 */
static char strftime_format_sixmonthsold[] = "%b %e %Y";
static char strftime_format[] = "%b %e %y %H:%M:%S";
#endif /* _WIN32 */

EXTERN_C void initlog _PROTO((char *, int, char *));

#endif /*  _CMONITD_H_INCLUDED_ */





