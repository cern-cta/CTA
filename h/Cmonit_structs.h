/*
 * $Id: Cmonit_structs.h,v 1.1 2002/05/29 13:20:06 bcouturi Exp $
 * Copyright (C) 2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* 
 * Cmonit_structs.h - Header containing CASTOR monitoring structures
 */

#ifndef _MONITOR_STRUCT_H_INCLUDED_
#define _MONITOR_STRUCT_H_INCLUDED_

#include <time.h>
#include "Castor_limits.h"
#include "osdep.h"
#include "Cmonit_constants.h"

/* ---------------------------------------
 * Common Structures
 * --------------------------------------- */

/* Entry in the Linked list of Filesystems */
struct Cmonit_fs_entry {
  struct Cmonit_fs_entry *next;
  unsigned int stager_host;
  char pool[CA_MAXPOOLNAMELEN + 1];
  char server[CA_MAXSHORTHOSTLEN + 1];
  char fs[CA_MAXDVNLEN + 1];
  u_signed64	capacity;	/* filesystem capacity in bytes */
  u_signed64	free;		/* free space in bytes */
};

/* Struct used to keep trace of the servers */
struct Cmonit_server_entry {
  unsigned int hostip;
  char name[CA_MAXHOSTNAMELEN + 1];
  char ip[16];
  time_t last_init_time;
  time_t last_packet_time;
  time_t last_packet_time_rtcopy;
  int nb_files;
  int nb_failed;
  struct Cmonit_server_entry *next;
};

/* Entry in the list of Tape drives */
struct Cmonit_tape_entry {
 
  struct Cmonit_tape_entry *next; /* Entry in the list of NULL if last */
  unsigned int tape_server_host; /* IP of the tape server managing this drive */
  uid_t	uid;
  gid_t	gid;
  int	jid;		/* process group id or session id */
  char	dgn[CA_MAXDGNLEN+1];	/* device group name */
  int	up;		/* drive status: down = 0, up = 1 */
  short	asn;		/* assign flag: assigned = 1 */
  time_t	asn_time;	/* timestamp of drive assignment */
  char	drive[CA_MAXUNMLEN+1];	/* drive name */
  int	mode;		/* WRITE_DISABLE or WRITE_ENABLE */
  char	vid[CA_MAXVIDLEN+1];
  char	vsn[CA_MAXVSNLEN+1];
  int	tobemounted;	/* 1 means tape to be mounted */
  int	lblcode;	/* label code: AL, NL, SL or BLP */
  int   cfseq; /* Current file sequence */

  int size;     /* Size of the last tranfered file */
  int retryn;   /* Retry number for the current tranfered file */
  int stgreqid; /* Stager request ID */
  char reqtype; /* Request type */
  char ifce[5]; /* Network Interface */
  char errmsgtxt[CA_MAXLINELEN+1]; /* Last Error message reportted by RTCOPY */
  char clienthost[CA_MAXHOSTNAMELEN+1];  /* Hostname of the client connected the drive */ 
  char dsksrvr[CA_MAXHOSTNAMELEN+1]; /* Name of the disk server */

  char groupName[CA_MAXGRPNAMELEN+1];
  char userName[CA_MAXUSRNAMELEN+1];

  u_signed64 transfered; /* Amount of data transfered so far */
  u_signed64 total; /* Total amount of data in the file transfered */
  u_signed64 rate; /* Transmission rate computed by RTCOPY */

  int current_file; /* Nb of the file currently being transfered */
  int nb_files; /* Total number of files being transfered in the request */
  
};


/* Struct used to keep trace of the servers */
struct Cm_stager {
  unsigned int hostip;
  char name[CA_MAXHOSTNAMELEN + 1];
  char ip[16];
  time_t last_init_time;
  time_t last_packet_time;
  int nb_files;
  int nb_failed;

  char nomorestage_on;

  struct Cm_pool *pools;
  struct Cm_migrator *migrators;

  struct Cm_stager *next;
};


struct Cm_pool {
  struct Cm_pool *next;
  char	name[CA_MAXPOOLNAMELEN+1];
  struct Cm_pool_element *elemp;
  char	gc[CA_MAXHOSTNAMELEN+CA_MAXPATHLEN+1]; 
  int	gc_start_thresh;
  int	gc_stop_thresh;
  int	nbelem;
  u_signed64	capacity;	/* Capacity in bytes */
  u_signed64	free;		/* Free space in bytes */
  int	ovl_pid;
  time_t	cleanreqtime;
  char migr_name[CA_MAXMIGRNAMELEN+1];
  int	mig_start_thresh;
  int	mig_stop_thresh;
};


struct Cm_migrator {
  struct Cm_migrator *next;
  char name[CA_MAXMIGRNAMELEN+1];
  int mig_pid;
  time_t migreqtime;
  time_t migreqtime_last_start;
  time_t migreqtime_last_end;
  int nbfiles_canbemig;   
  u_signed64 space_canbemig;
  int nbfiles_delaymig;   
  u_signed64 space_delaymig;
  int nbfiles_beingmig;   
  u_signed64 space_beingmig;
};


struct Cm_pool_element {
  char	server[CA_MAXHOSTNAMELEN+1];
  char	dirpath[CA_MAXPATHLEN+1];
  u_signed64	capacity;	/* filesystem capacity in bytes */
  u_signed64	free;		/* free space in bytes */
};



#endif /*  _MONITOR_STRUCT_H_INCLUDED_ */










