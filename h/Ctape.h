/*
 * Ctape.h,v 1.43 2004/01/23 10:08:05 bcouturi Exp
 */

/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)Ctape.h,v 1.43 2004/01/23 10:08:05 CERN IT-PDP/DM Jean-Philippe Baud
 */

#ifndef _CTAPE_H
#define _CTAPE_H

			/* tape daemon constants and macros */

#include "Ctape_constants.h"
#include "osdep.h"
#define CHECKI     10	/* max interval to check for work to be done */
#define CLNREQI   180	/* interval to check for jobs that have died */
#define LBLBUFSZ  128   /* size of buffers to read labels */
#define OPRMSGSZ  257	/* maximum operator message/reply length */
#define	RBTFASTRI  60	/* fast retry interval for robotic operations */
#define PRTBUFSZ  180
#define REPBUFSZ 2800	/* must be >= max tape daemon reply size */
#define REQBUFSZ 1104	/* must be >= max tape daemon request size */
#define SMSGI      30	/* retry interval when sending operator messages */
#define TPMAGIC 0x141001
#define TPTIMEOUT   5	/* netread timeout while receiving a request */
#define UCHECKI    10	/* max interval to check for drive ready or oper cancel */

			/* label types */

#define AL	1
#define NL	2
#define SL	3
#define BLP	4
#define AUL	5

			/* supported densities */

#define	D800	0x1
#define	D1600	0x2
#define	D6250	0x3
#define	D38000	0x4
#define	D8200	0x5
#define	D8500	0x6
#define	D38KD	0x7
#define D2G	0x8
#define D6G	0x9
#define D10G	0xA
#define SFMT	0xB
#define SRAW	0xC
#define DDS	0xD
#define D20G	0xE
#define D25G	0xF
#define D35G	0x10
#define D50G	0x11
#define D40G	0x12
#define D60G	0x13
#define D100G	0x14
#define D200G	0x15
#define D110G	0x16
#define D160G   0x17
#define D300G	0x18
#define D400G	0x19
#define D500G	0x1A
#define IDRC	0x100
#define	D38KC	(D38000 | IDRC)
#define	D38KDC	(D38KD | IDRC)
#define	D8200C	(D8200 | IDRC)
#define	D8500C	(D8500 | IDRC)
#define D10GC	(D10G | IDRC)
#define DDSC	(DDS | IDRC)
#define D20GC	(D20G | IDRC)
#define D25GC	(D25G | IDRC)
#define D35GC	(D35G | IDRC)
#define D40GC	(D40G | IDRC)
#define D50GC	(D50G | IDRC)
#define D60GC	(D60G | IDRC)
#define D100GC	(D100G | IDRC)
#define D110GC	(D110G | IDRC)
#define D160GC  (D160G | IDRC)
#define D200GC	(D200G | IDRC)
#define D300GC	(D300G | IDRC)
#define D400GC	(D400G | IDRC)
#define D500GC	(D500G | IDRC)

#ifdef NOTRACE
#ifdef __STDC__
#define ENTRY(x) \
	{ \
	(void) strcpy (func, #x); \
	}
#else
#define ENTRY(x) \
	{ \
	(void) strcpy (func, "x"); \
	}
#endif
#define RETURN(x) \
	{ \
	return ((x)); \
	}
#else
#ifdef __STDC__
#define ENTRY(x) \
	{ \
	(void) strcpy (func, #x); \
	tplogit (func, "function entered\n"); \
	}
#else
#define ENTRY(x) \
	{ \
	(void) strcpy (func, "x"); \
	tplogit (func, "function entered\n"); \
	}
#endif
#define RETURN(x) \
	{ \
	if ((x) >= 0) \
		tplogit (func, "returns %d\n", (x)); \
	else \
		tplogit (func, "returns %d, serrno = %d\n", (x), serrno); \
	return ((x)); \
	}
#endif
#define UPPER(s) \
	{ \
	char * q; \
	for (q = s; *q; q++) \
		if (*q >= 'a' && *q <= 'z') *q = *q + ('A' - 'a'); \
	}
			/* tape daemon request types */

#define TPRSV 	0	/* reserve tape resources */
#define TPMOUNT	1	/* assign drive, ask for the tape to be mounted, check VOL1 */
#define TPSTAT	2	/* get tape status display */
#define TPRSTAT	3	/* get resource reservation status display */
#define TPCONF	4	/* configure tape drive up/down */
#define TPRLS	5	/* unload tape and release reservation */
#define UPDVSN	6	/* update vid, vsn and mount flag in tape drive table */
#define	TPKILL	8	/* mount killed by user */
#define	FREEDRV	9	/* free drive */
#define RSLT	11	/* switch to new drive */
#define UPDFIL	12	/* update blksize, cfseq, fid, lrecl, recfm in tpfil */
#define TPINFO	13	/* get info for a given mounted tape */
#define TPPOS	14	/* position tape and check HDR1/HDR2 */
#define DRVINFO	15	/* get info for a given drive */

			/* tape daemon reply types */

#define	MSG_OUT		0
#define	MSG_ERR		1
#define	MSG_DATA	2
#define	TAPERC		3

			/* rbtsubr return codes */

#define	RBT_OK		0	/* Ok or error should be ignored */
#define	RBT_NORETRY	1	/* Unrecoverable error (just log it) */
#define	RBT_SLOW_RETRY	2	/* Should release drive & retry in 600 seconds */
#define	RBT_FAST_RETRY	3	/* Should retry in 60 seconds */
#define	RBT_DMNT_FORCE	4	/* Should do first a demount force */
#define	RBT_CONF_DRV_DN	5	/* Should configure the drive down */
#define	RBT_OMSG_NORTRY	6	/* Should send a msg to operator and exit */
#define	RBT_OMSG_SLOW_R 7	/* Ops msg (nowait) + release drive + slow retry */
#define	RBT_OMSGR	8	/* Should send a msg to operator and wait */
#define	RBT_UNLD_DMNT	9	/* Should unload the tape and retry demount */
 
			/* tape daemon messages */
 
#define TP000	"TP000 - tape daemon not available on %s\n"
#define TP001	"TP001 - no response from tape daemon\n"
#define	TP002	"TP002 - %s error : %s\n"
#define TP003   "TP003 - illegal function %d\n"
#define TP004   "TP004 - error getting request, netread = %d\n"
#define TP005	"TP005 - cannot get memory\n"
#define TP006	"TP006 - invalid value for %s\n"
#define	TP007	"TP007 - fid is mandatory when TPPOSIT_FID\n"
#define TP008	"TP008 - %s not accessible\n"
#define TP009	"TP009 - could not configure %s: %s\n"
#define TP010	"TP010 - resources already reserved for this job\n"
#define TP011	"TP011 - too many tape users\n"
#define TP012	"TP012 - too many drives requested\n"
#define TP013	"TP013 - invalid device group name\n"
#define TP014	"TP014 - reserve not done\n"
#define TP015	"TP015 - drive with specified name/characteristics does not exist\n"
#define TP017	"TP017 - cannot use blp in write mode\n"
#define TP018	"TP018 - duplicate option %s\n"
#define TP020	"TP020 - mount tape %s(%s) %s on drive %s@%s for %s %d (%s) or reply cancel | drive name"
#define TP021	"TP021 - label type mismatch: request %s, tape is %s\n"
#define TP022	"TP022 - path exists already\n"
#define TP023	"TP023 - mount cancelled by operator: %s\n"
#define TP024	"TP024 - file %s does not exist\n"
#define TP025	"TP025 - bad label structure\n"
#define TP026	"TP026 - system error\n"
#define TP027	"TP027 - you are not registered in account file\n"
#define TP028	"TP028 - file not expired\n"
#define	TP029	"TP029 - pathname is mandatory\n"
#define	TP030	"TP030 - I/O error\n"
#define	TP031	"TP031 - no vid specified\n"
#define	TP032	"TP032 - config file (line %d): %s\n"
#define	TP033	"TP033 - drive %s@%s not operational"
#define	TP034	"TP034 - all entries for a given device group must be grouped together\n"
#define TP035	"TP035 - configuring %s %s\n"
#define	TP036	"TP036 - path is mandatory when rls flags is %d\n"
#define TP037	"TP037 - path %s does not exist\n"
#define TP038	"TP038 - pathname too long\n"
#define TP039	"TP039 - vsn mismatch: request %s, tape vsn is %s\n"
#define TP040	"TP040 - release pending\n"
#define	TP041	"TP041 - %s of %s on %s failed : %s"
#define	TP042	"TP042 - %s : %s error : %s\n"
#define	TP043	"TP043 - configuration line too long: %s\n"
#define TP044	"TP044 - fid mismatch: request %s, tape fid for file %d is %s\n"
#define TP045	"TP045 - cannot write file %d (only %d files are on tape)\n"
#define TP046	"TP046 - request too large (max. %d)\n"
#define	TP047	"TP047 - reselect server requested by operator\n"
#define	TP048	"TP048 - config postponed: %s currently assigned\n"
#define	TP049	"TP049 - option IGNOREEOI is not valid for label type al or sl\n"
#define	TP050	"TP050 - vid mismatch: %s on request, %s on drive\n"
#define	TP051	"TP051 - fatal configuration error: %s %s\n"
#if defined(_WIN32)
#define	TP052	"TP052 - WSAStartup unsuccessful\n"
#define	TP053	"TP053 - you are not registered in the unix group/passwd mapping file\n"
#endif
#define	TP054	"TP054 - tape not mounted or not ready\n"
#define	TP055	"TP055 - parameter inconsistency with TMS for vid %s: %s on request <-> %s in TMS\n"
#define	TP056	"TP056 - %s request by %d,%d from %s\n"
#define	TP057	"TP057 - drive %s is not free\n"
#define	TP058	"TP058 - no free drive\n"
#define	TP059	"TP059 - invalid reason\n"
#define	TP060	"TP060 - invalid combination of method and filstat\n"
#define	TP061	"TP061 - filstat value incompatible with read-only mode\n"
#define	TP062	"TP062 - tape %s to be prelabelled %s%s"
#define	TP063	"TP063 - invalid user %d\n"
#define	TP064	"TP064 - invalid method for this label type\n"
#define TP065	"TP065 - tape %s(%s) on drive %s@%s for %s %d has bad MIR | reply to acknowledge or cancel\n"

			/* tape daemon internal tables */

struct confq {		/* config queue */
	struct confq *next;
	struct confq *prev;
	int	ovly_pid;
	int	status;
	int	ux;		/* index in drive table tptab */
};
struct rlsq {		/* rls queue */
	struct rlsq *next;
	struct rlsq *prev;
	int	rpfd;
	int	unldcnt;	/* count of unload in progress for this rls request */
};
struct tpdev {		/* tape device description */
	char	dvn[CA_MAXDVNLEN+1];	/* character special device name */
	int	den;		/* density */
	dev_t	majmin;		/* major/minor device number */
};
struct tpdgrt {		/* device group reservation table entry */
	char	name[CA_MAXDGNLEN+1];	/* device group name */
	int	rsvd;		/* number reserved */
	int	used;		/* number used */
	int	wait;		/* 1 if waiting for a free drive */
};
struct tpdpdg {		/* drives per device group table (pointers in tptab) */
	char	name[CA_MAXDGNLEN+1];	/* device group name */
	struct tptab *first;	/* pointer to first drive in device group */
	struct tptab *last;	/* pointer to last drive in device group */
	struct tptab *next;	/* pointer to next drive in device group to be alloc. */
};
struct tpfil {		/* tape file description */
	char	path[CA_MAXPATHLEN+1];	/* pathname specified by user */

	int	blksize;	/* maximum block size */
	int	cfseq;		/* current file number */
	char	fid[CA_MAXFIDLEN+1];	/* file id */
	char	filstat;	/* CHECK_FILE, NEW_FILE or APPEND */
	int	fsec;		/* file section number */
	int	fseq;		/* file sequence number requested by user */
	unsigned char blockid[4];	/* for positionning with locate command */
	int	lrecl;		/* record length */
	int	Qfirst;		/* fseq of first file of a mapped volume */
	int	Qlast;		/* fseq of last file of a mapped volume */
	char	recfm[CA_MAXRECFMLEN+1];	/* record format: F, FB or U */
	int	retentd;	/* retention period in days */
	int	flags;		/* NOTRLCHK, IGNOREEOI... */
};
struct tprrt {		/* resource reservation table: one entry per jid */
	struct tprrt *prev;
	struct tprrt *next;
	uid_t	uid;
	char	user[CA_MAXUSRNAMELEN+1];
	int	jid;		/* process-group-id */
	int	unldcnt;	/* count of unload in progress for this jid */
	int	totrsvd;	/* total number of reserved resources */
	struct tpdgrt *dg;
};
struct tptab {		/* tape drive table */
	char	drive[CA_MAXUNMLEN+1];	/* drive name */
	char	dgn[CA_MAXDGNLEN+1];	/* device group name */
	char	devtype[CA_MAXDVTLEN+1];	/* device type */
	char	dvrname[7];	/* driver name */
	char	loader[CA_MAXRBTNAMELEN+1];	/* manual, robot, fhs, acs or lmcp */
	int	ux;		/* index in drive table tptab */
	int	devnum;		/* number of devices defined for this drive */
	struct tpdev *devp;	/* pointer to tape device descriptions */

	int	up;		/* drive status: down = 0, up = 1 */

	int	asn;		/* assign flag: assigned = 1 */
	time_t	asn_time;	/* timestamp of drive assignment */
	struct tpdev *cdevp;	/* pointer to tape device description in use */
	struct tpfil *filp;	/* pointer to tape file description */
	uid_t	uid;
	gid_t	gid;
	char	acctname[7];	/* uuu$gg */
	int	jid;		/* process group id or session id */

	int	mntovly_pid;	/* pid of mounttape overlay */

	char	vid[CA_MAXVIDLEN+1];
	char	vsn[CA_MAXVSNLEN+1];
	int	tobemounted;	/* 1 means tape to be mounted */
	int	lblcode;	/* label code: AL, NL, SL or BLP */
	int	mode;		/* WRITE_DISABLE or WRITE_ENABLE */
};

			/* interface for label processing */

struct devlblinfo {
	char	path[CA_MAXPATHLEN+1];	/* pathname */
	char	devtype[CA_MAXDVTLEN+1];	/* device type */
	int	dev1tm;		/* one or two tapemarks at end of data */
	int	rewritetm;	/* tapemark must be rewritten on Exabytes 8200 */
	int	lblcode;	/* label code: AL, NL, SL or BLP */
	int	flags;		/* options like NOTRLCHK */
	int	fseq;		/* file sequence number */
	char	vol1[81];	/* VOL1 label */
	char	hdr1[81];	/* HDR1 label */
	char	hdr2[81];	/* HDR2 label */
	char	uhl1[81];	/* UHL1 label */
};

#ifdef MONITOR

/* Prototype for Monitoring method, from Cmonit_tape_client.c */
EXTERN_C int DLL_DECL Cmonit_send_tape_status _PROTO((struct tptab *, 
						      int));

#endif

#endif
