/*
 * Copyright (C) 1994-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: sacct.h,v $ $Revision: 1.2 $ $Date: 1999/09/20 12:25:46 $ CERN CN-PDP/DH   Jean-Philippe Baud
 */
/* Include file for CASTOR software accounting */

#include "Castor_limits.h"
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

struct accttape {	/* accounting record for tape software */
	int	subtype;
#if defined(_WIN32)
	int     uid;
	int     gid;
#else
	uid_t	uid;
	gid_t	gid;
#endif /* _WIN32 */
	int	jid;
	char	filler[16];
	char	dgn[CA_MAXDGNLEN+1];	/* CART, 8500, SMCF... */
	char	drive[CA_MAXUNMLEN+1];	/* drive name */
	char	vid[CA_MAXVIDLEN+1];
	int	fseq;
	int	reason;
};

			/* subtypes for system records */

#define SYSSHUTDOWN     0
#define SYSSTARTUP      1

			/* subtypes for tape accounting records */

#define	TPDSTART	0	/* tpdaemon started */
#define	TPASSIGN	1	/* drive assigned */
#define	TP2MOUNT	2	/* tape to be mounted */
#define	TPMOUNTED	3	/* tape mounted */
#define	TPPOSIT		4	/* tape positionned to requested file */
#define	TPUNLOAD	5	/* tape is unloading */
#define	TPFREE		6	/* drive freed */
#define	TPCONFUP	7	/* drive configured up */
#define	TPCONFDN	8	/* drive configured down */
#define	TPDGQ		9	/* device group queue */

			/* tape remount reasons */

#define	TPM_NORM	0	/* normal mount */
#define	TPM_WNGR	1	/* wrong ring */
#define	TPM_WNGV	2	/* wrong vsn */
#define	TPM_RSLT	3	/* reselect */

			/* tape unload reason */

#define	TPU_NORM	0	/* normal unload */
#define	TPU_WNGR	1	/* wrong ring */
#define	TPU_WNGV	2	/* wrong vsn */
#define	TPU_RSLT	3	/* reselect */

			/* tpconfig down reason */

#define	TPCD_START	0	/* startup */
#define	TPCD_CLN	1	/* cleaning */
#define	TPCD_TST	2	/* test */
#define	TPCD_HWF	3	/* HW failure */
#define	TPCD_SYS	4	/* by system */
#define	TPCD_SUS	5	/* drive suspect */
#define	TPCD_UPG	6	/* drive upgrade */
#define	TPCD_OPS	7	/* operational reason */

			/* tpconfig up reason */

#define	TPCU_START	0	/* startup */
#define	TPCU_CLN	1	/* cleaned */
#define	TPCU_RPL	2	/* replaced */
#define	TPCU_RPR	3	/* repaired */
#define	TPCU_PRV	4	/* preventive */
#define	TPCU_UPG	5	/* drive upgraded */
#define	TPCU_OPS	6	/* operational reason */

struct acctrtcp {	/* accounting record for rtcopy software */
	int	subtype;
#if defined(_WIN32)
	int     uid;
	int     gid;
#else
	uid_t	uid;
	gid_t	gid;
#endif /* _WIN32 */
	int	jid;
	int	stgreqid;	/* stager request id */
	char	reqtype;	/* R -> tpread, W -> tpwrite, D -> dumptape */
	char	ifce[5];	/* network interface used for data transfer */
	char	vid[CA_MAXVIDLEN+1];
	int	size;
	int	retryn;		/* retry number */
	int	exitcode;
	char	clienthost[CA_MAXHOSTNAMELEN+1];
	char	dsksrvr[CA_MAXHOSTNAMELEN+1];
};

			/* subtypes for rtcopy accounting records */

#define	RTCPCMDR	1	/* command received */
#define	RTCPPRC		2	/* completion of partial request */
#define	RTCPCMDC	3	/* command completed (with success or not) */
#define	RTCPPRR		4	/* retry of partial request */
#define	RTCPTPR		5	/* retry of tape mount */
#define RTCPCMDD	6 	/* Decrypted command line */

#ifndef MAXFSEQ
#define MAXFSEQ 15
#endif

struct acctstage {	/* accounting record for stage software */
	int	subtype;
#if defined(_WIN32)
	int     uid;
	int     gid;
#else
	uid_t	uid;
	gid_t	gid;
#endif /* _WIN32 */
	int	reqid;
	int	req_type;
	int	retryn;		/* retry number */
	int	exitcode;
	union {
	    char	clienthost[CA_MAXHOSTNAMELEN+1];
	    struct {
		char	poolname[CA_MAXPOOLNAMELEN+1];
		char	t_or_d;
		off_t	actual_size;
		int	nbaccesses;
		union {
		    struct {		/* tape specific info */
			char	dgn[9];
			char	fseq[MAXFSEQ];
			char	vid[7];
			char	tapesrvr[CA_MAXHOSTNAMELEN+1];
		    } t;
		    struct {		/* info for disk file stageing */
			char    xfile[CA_MAXHOSTNAMELEN+MAXPATH+1];
		    } d;
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
