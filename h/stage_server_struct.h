/*
 * $Id: stage_server_struct.h,v 1.8 2002/06/13 05:40:04 jdurand Exp $
 */

#ifndef __stage_server_struct_h
#define __stage_server_struct_h

#include <pwd.h>                /* For uid_t, gid_t */
#include <sys/types.h>          /* For all common types */
#ifndef _WIN32
#include <sys/time.h>           /* For time_t */
#else
#include <time.h>               /* For time_t */
#endif
#include "osdep.h"              /* For u_signed64 */
#include "Castor_limits.h"      /* For CASTOR limits */
#include "Cns_api.h"
#include "stage_struct.h"
#include "stage_constants.h"    /* For our constants */

struct waitf {
	int	subreqid;
	int	waiting_on_req;
	u_signed64 size_to_recall;             /* Only used in read-mode (asyncrhoneous callbacks from RTCOPY) */
	u_signed64 hsmsize;                    /* Original file total size - Only used in read-mode (asyncrhoneous callbacks from RTCOPY) */
	int nb_segments;
	u_signed64 size_yet_recalled;
	char	upath[(CA_MAXHOSTNAMELEN+MAXPATH)+1];
	char	ipath[(CA_MAXHOSTNAMELEN+MAXPATH)+1]; /* Used to lie to the system when doing internal copy of CASTOR file */
};

struct waitq {
	struct waitq *prev;
	struct waitq *next;
	char	pool_user[CA_MAXUSRNAMELEN+1];
	char	clienthost[CA_MAXHOSTNAMELEN+1];
	char	req_user[CA_MAXUSRNAMELEN+1];	/* requestor login name */
	uid_t	req_uid;		/* uid or Guid */
	gid_t	req_gid;
	char	rtcp_user[CA_MAXUSRNAMELEN+1];	/* running login name */
	char	rtcp_group[CA_MAXGRPNAMELEN+1];	/* running group name */
	uid_t	rtcp_uid;		/* uid or Guid */
	gid_t	rtcp_gid;
	int	clientpid;
	u_signed64 uniqueid; /* Unique IDentifier given by stgdaemon (magic = STGMAGIC) or by API (CthreadID, magic = STGMAGIC2) */
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
	int	clnreq_waitingreqid; /* Gives exact reqid of the stcp that is in WAITING_SPC stage */
	int	status;
	int	forced_exit; /* Force the waitq to stop right now - Useful in case of fatal system error */
	int	nretry;
	int	noretry; /* Propagation of --noretry or STAGE_NORETRY flag */
	int	Aflag; /* Deferred allocation (path returned to RTCOPY after tape position) */
	int	concat_off_fseq; /* 0 or fseq just before the '-', like: 1-9,11- => concat_off_fseq = 11 */
	int	magic; /* Flag to tell which magic number in case of API output */
	int	api_out; /* Flag to tell if we have to send structure in output (API mode) */
#if defined(_WIN32)
	int openmode;  /* Used only to remember the openmode in entries in STAGEOUT|WAITING_NS state */
#else
	mode_t	openmode;  /* Used only to remember the openmode in entries in STAGEOUT|WAITING_NS state */
#endif
	int openflags;  /* Used only to remember the openflags in entries in STAGEOUT|WAITING_NS state */
	int	silent; /* Determine if done in silent mode or not */
	int use_subreqid; /* Says if we allow RTCOPY to do asynchroneous callback */
	int *save_subreqid; /* Array saying relation between subreqid and all wf at the beginning */
	int save_nbsubreqid; /* Save original number of entries */
	int last_rwcounterfs_vs_R; /* Last -R option value that triggered the rwcountersfs call */
	int nb_waiting_spc; /* See routine checkwaitingspc() in stgdaemon.c */
	int nb_found_spc; /* See routine checkwaitingspc() in stgdaemon.c */
	u_signed64 flags; /* API flags */
};

struct pool {
	char	name[CA_MAXPOOLNAMELEN+1];
	struct pool_element *elemp;
	u_signed64	defsize;
	int	gc_start_thresh;
	int	gc_stop_thresh;
	char	gc[CA_MAXHOSTNAMELEN+MAXPATH+1];	/* garbage collector */
	int	no_file_creation;	/* Do not create empty file on stagein/out (for Objectivity DB) */
	int	nbelem;
	int	next_pool_elem;		/* next pool element to be used */
	u_signed64	capacity;	/* Capacity in bytes */
	u_signed64	free;		/* Free space in bytes */
	int	ovl_pid;
	time_t	cleanreqtime;
	int	cleanstatus;	/* 0 = normal, 1 = just cleaned */
	char	migr_name[CA_MAXMIGRNAMELEN+1];
	struct migrator *migr;
	int	mig_start_thresh;
	int	mig_stop_thresh;
	u_signed64 mig_data_thresh;
	int	max_setretenp;	/* maximum value for explicit setting of retention period (days) */
	int	put_failed_retenp;	/* minimum value for put_failed retention period (days) */
	int	stageout_retenp;	/* minimum value for stageout retention period (days) */
	int	export_hsm;	/* exportable flag for HSM files */
};

struct predicates {
  int nbfiles_canbemig;            /* Files candidates for migration */
  u_signed64 space_canbemig;
  int nbfiles_delaymig;            /* Files postponed for being candidate to migration (see mintime_beforemigr) */
  u_signed64 space_delaymig;
  int nbfiles_beingmig;            /* Files currently being migrated */
  u_signed64 space_beingmig;
  int nbfiles_to_mig;              /* Used for logging purpose when forking a migrator */
  u_signed64 space_to_mig;
};

struct fileclass {
	char server[CA_MAXHOSTNAMELEN+1];
	struct Cns_fileclass Cnsfileclass;
	char last_tppool_used[CA_MAXPOOLNAMELEN+1];
	int  flag;                                      /* Flag preventing us to double count next member */
	int  streams;                                   /* Number of streams using this fileclass while migrating */
	int  orig_streams;                              /* Original number of streams using this fileclass while migrating */
	int  nfree_stream;                              /* Nb of expanded streams while migrating */
	int being_migr;                                 /* Number of files being migrated in this fileclass */
	int  ifree_stream;                              /* Internal counter used to dispatch to new streams */
};

struct migrator {
	char name[CA_MAXMIGRNAMELEN+1];
	int mig_pid;
	time_t migreqtime;
	time_t migreqtime_last_start;
	time_t migreqtime_last_end;
	int nfileclass;
	struct fileclass **fileclass;
	struct predicates *fileclass_predicates;
	struct predicates global_predicates;
};

struct pool_element {
	char	server[CA_MAXHOSTNAMELEN+1];
	char	dirpath[MAXPATH];
	u_signed64	capacity;	/* filesystem capacity in bytes */
	u_signed64	free;		/* free space in bytes */
	int nbreadaccess;       /* Number of known accesses in read mode */
	int nbwriteaccess;      /* Number of known accesses in write mode */
	time_t last_allocation; /* last known allocation timestamp */
	u_signed64	bsize;		/* filesystem block size in bytes */
	int flag;               /* Internal flag used to not double count elements when evaluating number of hosts in all the pools */
};

struct sorted_ent {
	struct sorted_ent *next;
	struct sorted_ent *prev;
	struct stgcat_entry *stcp;
	struct stgpath_entry *stpp;
	int scanned;            /* Flag telling if we yet scanned this entry */
	double	weight;
};

#endif /* __stage_server_struct_h */
