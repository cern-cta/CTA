/*
 * $Id: stage.h,v 1.26 2000/09/21 07:29:29 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

			/* stage daemon constants and macros */

#ifndef __stage_h
#define __stage_h

/* ==================== */
/* Malloc Debug Library */
/* ==================== */
#ifdef USE_DMALLOC
#include <dmalloc.h>
#endif

#include "Castor_limits.h"
#include <osdep.h>
#include "serrno.h"     /* Contains ESTNACT etc... */

/* This macro returns TRUE is the file is an hpss one */
#define ISHPSS(xfile)   (strncmp (xfile, "/hpss/"  , 6) == 0 || strstr (xfile, ":/hpss/"  ) != NULL)

/* This macro returns TRUE is the host is an hpss one */
#define ISHPSSHOST(xfile) (strstr(xfile,"hpss") == xfile )

/* This macro returns TRUE is the file is a castor one */
#ifdef NSROOT
#define ISCASTOR(xfile) (strncmp (xfile, NSROOT "/", strlen(NSROOT) + 1) == 0 || strstr (xfile, ":" NSROOT "/") != NULL)
#else
#define ISCASTOR(xfile) 0
#endif

/* This macro returns TRUE is the host is a castor one */
#if (defined(NSHOST) && defined(NSHOSTPFX))
#define ISCASTORHOST(xfile) (strstr(xfile,NSHOST) == xfile || strstr(xfile,NSHOSTPFX) == xfile)
#else
#ifdef NSHOST
#define ISCASTORHOST(xfile) (strstr(xfile,NSHOST) == xfile)
#else
#ifdef NSHOSTPFX
#define ISCASTORHOST(xfile) (strstr(xfile,NSHOSTPFX) == xfile)
#else
#define ISCASTORHOST(xfile) 0
#endif
#endif
#endif

/* This macro returns the hpss file pointer, without hostname */
#define HPSSFILE(xfile)   strstr(xfile,"/hpss/")

/* This macro returns the castor file pointer, without hostname */
#define CASTORFILE(xfile) strstr(xfile,"/castor/")

#define STGTIMEOUT 10   /* Stager network timeout (seconds) */
#define DEFDGN "CART"	/* default device group name */
#ifdef MAXPATH
#undef MAXPATH
#endif
#define MAXPATH 80	/* maximum path length */
#define	MAXRETRY 5
#define	MAXVSN 3	/* maximum number of vsns/vids on a stage command */
#define PRTBUFSZ 1024
#define REPBUFSZ  512	/* must be >= max stage daemon reply size */
#define REQBUFSZ 20000	/* must be >= max stage daemon request size */
#define CHECKI	10	/* max interval to check for work to be done */
#define	RETRYI	60
#define STGMAGIC    0x13140701
#define STG	"stage"	/* service name in /etc/services */

#define SHIFT_ESTNACT 198 /* Old SHIFT value when nomorestage - remapped in send2stgd */

#define UPPER(s) \
	{ \
	char * q; \
	for (q = s; *q; q++) \
		if (*q >= 'a' && *q <= 'z') *q = *q + ('A' - 'a'); \
	}

			/* stage daemon request types */

#define STAGEIN		1
#define STAGEOUT	2
#define STAGEWRT	3
#define STAGEPUT	4
#define STAGEQRY	5
#define STAGECLR	6
#define	STAGEKILL	7
#define	STAGEUPDC	8
#define	STAGEINIT	9
#define	STAGECAT	10
#define	STAGEALLOC	11
#define	STAGEGET	12
#define	STAGEMIGPOOL	13
#define STAGEFILCHG	14

			/* stage daemon reply types */

#define	MSG_OUT		0
#define	MSG_ERR		1
#define	RTCOPY_OUT	2
#define	STAGERC		3
#define	SYMLINK		4
#define	RMSYMLINK	5

			/* -C, -E and -T options */

#include "Ctape_constants.h"
#include "rtcp_constants.h"

			/* stage states */

#define	NOTSTAGED	0
#define	WAITING_SPC	0x10	/* waiting space */
#define	WAITING_REQ	0x20	/* waiting on an other request */
#define	STAGED		0x30	/* stage complete */
#define	KILLED		0x40	/* stage killed */
#define	STG_FAILED	0x50	/* stage failed */
#define	PUT_FAILED	0x60	/* stageput failed */
#define	STAGED_LSZ	0x100	/* stage limited by size */
#define	STAGED_TPE	0x200	/* blocks with parity error have been skipped */
#define	CAN_BE_MIGR	0x400	/* file can be migrated */
#define	LAST_TPFILE	0x1000	/* last file on this tape */

			/* stage daemon messages */

#define STG00	"STG00 - stage daemon not available on %s\n"
#define STG01	"STG01 - no response from stage daemon\n"
#define STG02	"STG02 - %s : %s error : %s\n"
#define STG03   "STG03 - illegal function %d\n"
#define STG04   "STG04 - error reading request header, read = %d from %s (%s - %s)\n"
#define STG05	"STG05 - cannot get memory\n"
#define STG06	"STG06 - invalid value for option %s\n"
#define	STG07	"STG07 - pathname is mandatory\n"
#define STG08	"STG08 - incorrect pathname %s\n"
#define	STG09	"STG09 - fatal configuration error: %s %s\n"
#define	STG10	"STG10 - option G not allowed for user of group %s\n"
#define	STG11	"STG11 - invalid user: %s\n"
#define	STG12	"STG12 - vsn, vid or external_filename must be specified\n"
#define	STG13	"STG13 - duplicate option %s\n"
#define	STG14	"STG14 - too many vsns specified\n"
#define	STG15	"STG15 - parameter inconsistency with TMS: %s<->%s\n"
#define	STG16	"STG16 - extraneous parameter\n"
#define	STG17	"STG17 - option %s is invalid for %s\n"
#define	STG18	"STG18 - trailing dash in -q option is only valid for stagein\n"
#define	STG19	"STG19 - incorrect number of filenames specified\n"
#define	STG20	"STG20 - record length must be specified with recfm F\n"
#define	STG21	"STG21 - fseq string is too long\n"
#define	STG22	"STG22 - could not find corresponding stage request\n"
#define	STG23	"STG23 - %s is not accessible\n"
#define	STG24	"STG24 - pool %s is empty\n"
#define	STG25	"STG25 - %s name is missing\n"
#define	STG26	"STG26 - invalid description of %s %s\n"
#define	STG27	"STG27 - %s name %s is too long\n"
#define	STG28	"STG28 - missing or invalid value for DEFSIZE\n"
#define	STG29	"STG29 - no staging pool described\n"
#define	STG30	"STG30 - a default pool must be defined\n"
#define	STG31	"STG31 - staging host must be defined in configuration file\n"
#define	STG32	"STG32 - %s is not in the list of pools\n"
#define	STG33	"STG33 - %s: %s\n"
#define	STG34	"STG34 - program name must be stagein, stageout, stagewrt or stagecat\n"
#define	STG35	"STG35 - option(s) %s and %s are mutually exclusive\n"
#define	STG36	"STG36 - invalid group: %d\n"
#define	STG37	"STG37 - another request for same file is in progress\n"
#define	STG38	"STG38 - stage request too long\n"
#define	STG39	"STG39 - another stageinit is pending\n"
#define	STG40	"STG40 - another entry for same file exists already in catalog\n"
#define	STG41	"STG41 - %s %s for file %s on %s, return code %d\n\n"
#define	STG42	"STG42 - %s %s for file %s, return code %d\n\n"
#define	STG43	"STG43 - Retrying command, retry number %d\n\n"
#define	STG44	"STG44 - staging in afs directory is not supported\n"
#define	STG45	"STG45 - unable to allocate requested space\n"
#define	STG46	"STG46 - vid, linkname, internal or external filename must be specified\n"
#define	STG47	"STG47 - %s\n"
#define	STG48	"STG48 - the catalog %s seems to be corrupted\n"
#define	STG49	"STG49 - poolname mismatch: %s belongs to pool %s\n"
#define	STG50	"STG50 - poolname mismatch: %s does not belong to any pool\n"
#if defined(_WIN32)
#define	STG51	"STG51 - WSAStartup unsuccessful\n"
#define	STG52	"STG52 - you are not registered in the unix group/passwd mapping file\n"
#define	STG53	"STG53 - %s error %d\n"
#endif
#define	STG54	"STG54 - HSM hostname not specified\n"
#define	STG55	"STG55 - migrator name %s not defined\n"
#define	STG56	"STG56 - migration policy of migrator %s is not defined\n"
#if defined(vms)
#define	STG80	"STG80 - invalid GRPUSER entry (username missing) : %s\n"
#define	STG81	"STG81 - invalid GRPUSER entry (uid missing) : %s\n"
#define	STG82	"STG82 - invalid GRPUSER entry (gid missing) : %s\n"
#define	STG83	"STG83 - -G parameter is mandatory for VMS users\n"
#endif
#define	STG92	"STG92 - %s request by %s (%d,%d) from %s\n"
#define	STG93	"STG93 - removing link %s\n"
#define	STG94	"STG94 - creating link %s\n"
#define	STG95	"STG95 - %s cleared by %s\n"
#define	STG96	"STG96 - %s already staged, size = %ld (%.1f MB), nbaccess = %d\n"
#define	STG97	"STG97 - %s:%s staged by (%s,%s), server %s  unit %s  ifce %s  size %ld  wtim %d  ttim %d rc %d\n"
#define	STG98	"STG98 - %s\n"
#define	STG99	"STG99 - stage returns %d\n"
#define STG100  "STG100 - Database %s error (%s) at %s:%d\n"
#define STG101  "STG101 - HSM File %s previously staged under name %s. Catalog updated.\n"
#define STG102  "STG102 - Mixed %s HSM host with %s HSM filename: %s\n"

			/* stage daemon return codes */

#define	USERR	  1	/* user error */
#define	SYERR 	  2	/* system error */
#define	UNERR	  3	/* undefined error */
#define	CONFERR	  4	/* configuration error */
#define	LNKNSUP	189	/* symbolic links not supported on that platform */
#define	CLEARED	192	/* aborted by stageclr */
#define	BLKSKPD	193	/* blocks were skipped */
#define	TPE_LSZ	194	/* blocks were skipped, stageing limited by size */
#define	MNYPARI	195	/* stagein stopped: too many tape errors, but -E keep */
#define	REQKILD	196	/* request killed by user */
#define	LIMBYSZ	197	/* limited by size */
#define	ENOUGHF	199	/* enough free space */
#define COFFOK  200 /* successful stager exit code for -c off mode */

			/* stage daemon internal tables */

typedef char fseq_elem[7];
#if defined(_WIN32)
typedef long gid_t;
typedef long uid_t;
#endif

#if ! defined(vms)
struct stgcat_entry {		/* entry format in STGCAT table */
	int	blksize;	/* maximum block size */
	char	filler[2];
	char	charconv;	/* character conversion */
	char	keep;		/* keep data on disk after successful stagewrt */
	int	lrecl;		/* record length */
	int	nread;		/* number of blocks/records to be copied */
	char	poolname[CA_MAXPOOLNAMELEN+1];
	char	recfm[CA_MAXRECFMLEN+1];	/* record format */
	int	size;		/* size in Mbytes of data to be staged */
	char	ipath[(CA_MAXHOSTNAMELEN+MAXPATH)+1];	/* internal path */
	char	t_or_d;		/* 't' for tape/disk, 'd' for disk/disk */
	char	group[CA_MAXGRPNAMELEN+1];
	char	user[CA_MAXUSRNAMELEN+1];	/* login name */
	uid_t	uid;		/* uid or Guid */
	gid_t	gid;
#if defined(vms) || defined(_WIN32)
	int mask;
#else
	mode_t	mask;
#endif
	int	reqid;
	int	status;
	off_t	actual_size;
	time_t	c_time;
	time_t	a_time;
	int	nbaccesses;
	union {
	    struct {			/* tape specific info */
		char	den[CA_MAXDENLEN+1];	/* density */
		char	dgn[CA_MAXDGNLEN+1];	/* device group */
		char	fid[CA_MAXFIDLEN+1];	/* file id */
		char	filstat;	/* file status: new = 'n', old = 'o' */
		char	fseq[CA_MAXFSEQLEN+1];	/* file sequence number requested by user */
		char	lbl[CA_MAXLBLTYPLEN+1];	/* label type: al, nl, sl or blp */
		int	retentd;	/* retention period in days */
		char	tapesrvr[CA_MAXHOSTNAMELEN+1];	/* tape server */
		char	E_Tflags;	/* SKIPBAD, KEEPFILE, NOTRLCHK */
		char	vid[MAXVSN][CA_MAXVIDLEN+1];
		char	vsn[MAXVSN][CA_MAXVSNLEN+1];
	    } t;
	    struct {			/* info for disk file stageing */
		char	xfile[(CA_MAXHOSTNAMELEN+MAXPATH)+1];
		char	Xparm[23];
	    } d;
      struct {			/* Migrated files (non-CASTOR) */
		char	xfile[167];
	    } m;
	    struct {			/* HSM files (CASTOR) */
		char	xfile[167];
		char		server[CA_MAXHOSTNAMELEN+1];
		u_signed64	fileid;
	    } h;
	} u1;
};

struct stgpath_entry {		/* entry format in STGPATH table */
	int	reqid;
	char	upath[(CA_MAXHOSTNAMELEN+MAXPATH)+1];
};

struct waitf {
	int	subreqid;
	int	waiting_on_req;
	char	upath[(CA_MAXHOSTNAMELEN+MAXPATH)+1];
};

struct waitq {
	struct waitq *prev;
	struct waitq *next;
	char	pool_user[CA_MAXUSRNAMELEN+1];
	char	clienthost[CA_MAXHOSTNAMELEN+1];
	char	user[CA_MAXUSRNAMELEN+1];	/* login name */
	uid_t	uid;		/* uid or Guid */
	gid_t	gid;
	int	clientpid;
	int	copytape;
	int	Pflag;		/* stagealloc -P option */
	int	Upluspath;
	int	reqid;
	int	req_type;
	int	key;
	int	rpfd;
	int	ovl_pid;
	int	nb_subreqs;
	int	nbdskf;	/* >= nb_subreqs, because stagewrt may concatenate */
	int	nb_waiting_on_req;
	struct waitf *wf;
	int	nb_clnreq;
	char	waiting_pool[CA_MAXPOOLNAMELEN+1];	/* if waiting space */
	int	clnreq_reqid;		/* disk full reported by rtcopy */
	int	clnreq_rpfd;
	int	status;
	int	nretry;
	int	Aflag; /* Deferred allocation (path returned to RTCOPY after tape position) */
	int	Migrationflag; /* Determine if done under stage:st or not */
	int	concat_off_fseq; /* 0 or fseq just before the '-', like: 1-9,11- => concat_off_fseq = 11 */
};

struct migpolicy {
	u_signed64 data_mig_threshold;
	int	freespace_threshold;
	int	maxdrives;
	int	seg_allowed;
	int	time_interval;
	char	name[CA_MAXMIGPNAMELEN+1];
	int	tape_pool_index;
	char	**tape_pool;		/* Tape poolnames */
};
	
struct pool {
	char	name[CA_MAXPOOLNAMELEN+1];
	struct pool_element *elemp;
	int	defsize;
	int	minfree;
	char	gc[CA_MAXHOSTNAMELEN+MAXPATH+1];	/* garbage collector */
	int	no_file_creation;	/* Do not create empty file on stagein/out (for Objectivity DB) */
	int	nbelem;
	int	next_pool_elem;		/* next pool element to be used */
	u_signed64	capacity;		/* in 512 bytes blocks */
	u_signed64	free;			/* in 512 bytes blocks */
	int	ovl_pid;
	time_t	cleanreqtime;
	int	cleanstatus;	/* 0 = normal, 1 = just cleaned */
	char	migr_name[CA_MAXMIGRNAMELEN+1];
	struct migrator *migr;
};

struct migrator {
	char name[CA_MAXMIGRNAMELEN+1];
	/* Current Parameter of the migrator */
	int	mig_pid;
	time_t	migreqtime;
	int	nbfiles_canbemig;	/* number of files that can be migrated */
	u_signed64	space_canbemig;		/* total amount of data that can be migrated */
	/* Predefined policy parameters that decide if the migrator have to be launched */
	char	migp_name[CA_MAXMIGPNAMELEN+1];
	struct migpolicy *migp;
	int	nbpool;
	struct pool	**poolp;		/* poolnames */
};

struct pool_element {
	char	server[CA_MAXHOSTNAMELEN+1];
	char	dirpath[MAXPATH];
	long	capacity;	/* filesystem capacity in blocks */
	long	free;		/* number of free blocks */
	long	bsize;		/* block size */
};

struct sorted_ent {
	struct sorted_ent *next;
	struct sorted_ent *prev;
	struct stgcat_entry *stcp;
	struct stgpath_entry *stpp;
	double	weight;
};
#endif

#endif /* __stage_h */
