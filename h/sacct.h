/*
 * $Id: sacct.h,v 1.17 2009/07/23 12:22:03 waldron Exp $
 */

/*
 * Copyright (C) 1994-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 */
/* Include file for CASTOR software accounting */

#ifndef H_SACCT_H 
#define H_SACCT_H 1

#include "Castor_limits.h"
#include "stage_limits.h"
#include "osdep.h"

struct accthdr {	/* header for accounting record */
	time_t	timestamp;
	int	package;
	int	len;
};

			/* package identifiers */

#define ACCTSYSTEM      0
#define	ACCTRFIO	1
#define	ACCTTAPE	2
#define	ACCTRTCOPY	3
#define	ACCTSTAGE	4
#define	ACCTNQS		5
#define ACCTRTCPTIM     6
#define	ACCTSTAGE2	7
#define	ACCTSTAGE64	8
#define	ACCTRFIO64	11

struct  acctsystem      {
	int     subtype;
};

#ifndef MAXPATH
#define MAXPATH 80
#endif

struct acctrfio {       /* accounting record for rfio software */
	int	reqtype;
        int     uid;
        int     gid;
        int     jid;
        int     accept_socket;
        union {
	  struct {
	    int     flag1;
	    int     flag2;
	  } anonymous;
	  struct {
	    int     flags;
	    int     mode;
	  } accesstypes;
	  struct {
	    int     owner;
	    int     group;
	  } chowntype;
	  struct {
	    int     function;
	    int     operation;
	  } lockftype;
	} flags;
        int     nb_read;
        int     nb_write;
        int     nb_ahead;
        int     nb_stat;
        int     nb_seek;
        int     nb_preseek;
        int     read_size;
        int     write_size;
        int     remote_addr;
        int     local_addr;
        int     status;
        int     rc;
        int     len1;
        int     len2;
        char    filename[2*MAXPATH+1];
};

struct acctrfio64 {       /* accounting record for rfio64 software */
	int	reqtype;
        int     uid;
        int     gid;
        int     jid;
        int     accept_socket;
        union {
	  struct {
	    int     flag1;
	    int     flag2;
	  } anonymous;
	  struct {
	    int     flags;
	    int     mode;
	  } accesstypes;
	  struct {
	    int     owner;
	    int     group;
	  } chowntype;
	  struct {
	    int     function;
	    int     operation;
	  } lockftype;
	} flags;
        int     nb_read;
        int     nb_write;
        int     nb_ahead;
        int     nb_stat;
        int     nb_seek;
        int     nb_preseek;
        int     padding;
        signed64 read_size;
        signed64 write_size;
        int     remote_addr;
        int     local_addr;
        int     status;
        int     rc;
        int     len1;
        int     len2;
        char    filename[2*MAXPATH+1];
};

			/* subtypes for system records */

#define SYSSHUTDOWN     0
#define SYSSTARTUP      1

#ifndef MAXFSEQ
#define MAXFSEQ 15
#endif

struct acctstage {	/* accounting record for stage software */
	int	subtype;
	uid_t	uid;
	gid_t	gid;
	int	reqid;
	int	req_type;
	int	retryn;		/* retry number */
	int	exitcode;
	union {
		char clienthost[CA_MAXHOSTNAMELEN+1];
		struct {
			char poolname[CA_MAXPOOLNAMELEN+1];
			char t_or_d;
			off_t actual_size;
			int nbaccesses;
			union {
				struct {		/* tape specific info */
					char dgn[CA_MAXDGNLEN+1];
					char fseq[CA_MAXFSEQLEN+1];
					char vid[CA_MAXVIDLEN+1];
					char tapesrvr[CA_MAXHOSTNAMELEN+1];
				} t;
				struct {		/* info for disk file stageing */
					char xfile[CA_MAXHOSTNAMELEN+MAXPATH+1];
				} d;
				struct {		/* info for disk file stageing */
					char xfile[STAGE_MAX_HSMLENGTH+1];
				} m;
				struct {		/* info for disk file stageing */
					char xfile[STAGE_MAX_HSMLENGTH+1];
					u_signed64 fileid;
				} h;
			} u1;
	    } s;
	} u2;
};

struct acctstage2 {	/* accounting record for stage software */
	int	subtype;
	uid_t	uid;
	gid_t	gid;
	int	reqid;
	int	req_type;
	int	retryn;		/* retry number */
	int	exitcode;
	union {
		char clienthost[CA_MAXHOSTNAMELEN+1];
		struct {
			char poolname[CA_MAXPOOLNAMELEN+1];
			char t_or_d;
			off_t actual_size;
			time_t c_time;
			int nbaccesses;
			union {
				struct {		/* tape specific info */
					int  side;
					char dgn[CA_MAXDGNLEN+1];
					char fseq[CA_MAXFSEQLEN+1];
					char vid[CA_MAXVIDLEN+1];
					char tapesrvr[CA_MAXHOSTNAMELEN+1];
				} t;
				struct {		/* info for disk file stageing */
					char xfile[CA_MAXHOSTNAMELEN+MAXPATH+1];
				} d;
				struct {		/* info for disk file stageing */
					char xfile[STAGE_MAX_HSMLENGTH+1];
				} m;
				struct {		/* info for disk file stageing */
					char xfile[STAGE_MAX_HSMLENGTH+1];
					u_signed64 fileid;
				} h;
			} u1;
	    } s;
	} u2;
};

struct acctstage64 {	/* accounting record for stage 64BITS software */
	int	subtype;
	uid_t	uid;
	gid_t	gid;
	int	reqid;
	int	req_type;
	int	retryn;		/* retry number */
	int	exitcode;
	union {
		char clienthost[CA_MAXHOSTNAMELEN+1];
		struct {
			char poolname[CA_MAXPOOLNAMELEN+1];
			char t_or_d;
			u_signed64 actual_size;
			TIME_T c_time;
			int nbaccesses;
			union {
				struct {		/* tape specific info */
					int  side;
					char dgn[CA_MAXDGNLEN+1];
					char fseq[CA_MAXFSEQLEN+1];
					char vid[CA_MAXVIDLEN+1];
					char tapesrvr[CA_MAXHOSTNAMELEN+1];
				} t;
				struct {		/* info for disk file stageing */
					char xfile[CA_MAXHOSTNAMELEN+MAXPATH+1];
				} d;
				struct {		/* info for disk file stageing */
					char xfile[STAGE_MAX_HSMLENGTH+1];
				} m;
				struct {		/* info for disk file stageing */
					char xfile[STAGE_MAX_HSMLENGTH+1];
					u_signed64 fileid;
				} h;
			} u1;
	    } s;
	} u2;
};

			/* subtypes for stage accounting records */

#define	STGSTART	0	/* stgdaemon started */
#define	STGCMDR		1	/* command received */
#define	STGFILS		2	/* file staged */
#define	STGCMDC		3	/* command completed (with success or not) */
#define	STGCMDS		4	/* stager started */
#define	STGFILC		5	/* file cleared */

#endif /* H_SACCT_H */
