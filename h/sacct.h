/*
 * Copyright (C) 1994-1998 by CERN/CN/PDP/DH
 * All rights reserved
 */

/*
 * @(#)sacct.h	1.10 11/30/98  CERN CN-PDP/DH   Jean-Philippe Baud
 */
/* Include file for SHIFT software accounting */

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
#if (defined(sun) && !defined(SOLARIS)) || defined(_WIN32)
	int     uid;
	int     gid;
#else
	uid_t	uid;
	gid_t	gid;
#endif /* (sun && !SOLARIS) || _WIN32 */
	int	jid;
	char	nqsid[16];
	char	dgn[9];		/* CART, 8500, SMCF... */
	char	unm[9];		/* unit name */
	char	vid[7];
	int	fseq;		/* file seq if TPPOSIT, queue size if TPDGQ */
	int	reason;
};

			/* subtypes for system records */

#define SYSSHUTDOWN     0
#define SYSSTARTUP      1

			/* subtypes for tape accounting records */

#define	TPDSTART	0	/* tpdaemon started */
#define	TPASSIGN	1	/* unit assigned */
#define	TP2MOUNT	2	/* tape to be mounted */
#define	TPMOUNTED	3	/* tape mounted */
#define	TPPOSIT		4	/* tape positionned to requested file */
#define	TPUNLOAD	5	/* tape is unloading */
#define	TPFREE		6	/* unit freed */
#define	TPCONFUP	7	/* unit configured up */
#define	TPCONFDN	8	/* unit configured down */
#define	TPDGQ		9	/* device group queue */

			/* tape remount reasons */

#define	TPM_NORM	0	/* normal mount */
#define	TPM_WNGR	1	/* wrong ring */
#define	TPM_WNGV	2	/* wrong vsn */
#define	TPM_RSLT	3	/* reselect */

#ifndef MAXHOSTNAMELEN
#define MAXHOSTNAMELEN  64
#endif

struct acctrtcp {	/* accounting record for rtcopy software */
	int	subtype;
#if (defined(sun) && !defined(SOLARIS)) || defined(_WIN32)
	int     uid;
	int     gid;
#else
	uid_t	uid;
	gid_t	gid;
#endif /* (sun && !SOLARIS) || _WIN32 */
	int	jid;
	int	stgreqid;	/* stager request id */
	char	reqtype;	/* R -> tpread, W -> tpwrite, D -> dumptape */
	char	ifce[5];	/* network interface used for data transfer */
	char	vid[7];
	int	size;
	int	retryn;		/* retry number */
	int	exitcode;
	char	clienthost[MAXHOSTNAMELEN];
	char	dsksrvr[MAXHOSTNAMELEN];
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
#ifndef MAXPOOLNAMELEN
#define MAXPOOLNAMELEN  16
#endif

struct acctstage {	/* accounting record for stage software */
	int	subtype;
#if (defined(sun) && !defined(SOLARIS)) || defined(_WIN32)
	int     uid;
	int     gid;
#else
	uid_t	uid;
	gid_t	gid;
#endif /* (sun && !SOLARIS) || _WIN32 */
	int	reqid;
	int	req_type;
	int	retryn;		/* retry number */
	int	exitcode;
	union {
	    char	clienthost[MAXHOSTNAMELEN];
	    struct {
		char	poolname[MAXPOOLNAMELEN];
		char	t_or_d;
		off_t	actual_size;
		int	nbaccesses;
		union {
		    struct {		/* tape specific info */
			char	dgn[9];
			char	fseq[MAXFSEQ];
			char	vid[7];
			char	tapesrvr[MAXHOSTNAMELEN];
		    } t;
		    struct {		/* info for disk file stageing */
			char    xfile[MAXHOSTNAMELEN+MAXPATH];
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
