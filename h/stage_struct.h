/*
 * $Id: stage_struct.h,v 1.7 2002/04/30 12:18:16 jdurand Exp $
 */

#ifndef __stage_struct_h
#define __stage_struct_h

/* Definition of stage structures - common to API and daemon */
#include <pwd.h>                /* For uid_t, gid_t - supported on all platforms (c.f. win32 dir.) */
#include <sys/types.h>
#ifndef _WIN32
#include <sys/time.h>           /* For time_t */
#else
#include <time.h>               /* For time_t */
#endif
#include "Castor_limits.h"      /* For CASTOR limits */
#include "osdep.h"              /* For u_signed64 */
#include "stage_constants.h"

typedef char fseq_elem[7];

struct stgcat_entry {		/* entry format in STGCAT table */
	int	blksize;	/* maximum block size */
	char	filler[2];
	char	charconv;	/* character conversion */
	char	keep;		/* keep data on disk after successful stagewrt */
	int	lrecl;		/* record length */
	int	nread;		/* number of blocks/records to be copied */
	char	poolname[CA_MAXPOOLNAMELEN+1];
	char	recfm[CA_MAXRECFMLEN+1];	/* record format */
	u_signed64	size;		/* size in bytes of data to be staged */
	char	ipath[(CA_MAXHOSTNAMELEN+MAXPATH)+1];	/* internal path */
	char	t_or_d;		/* 't' for tape/disk, 'd' for disk/disk , 'm' for non-CASTOR HSM, 'h' for CASTOR HSM */
	char	group[CA_MAXGRPNAMELEN+1];
	char	user[CA_MAXUSRNAMELEN+1];	/* login name */
	uid_t	uid;		/* uid or Guid */
	gid_t	gid;
#if defined(_WIN32)
	int mask;
#else
	mode_t	mask;
#endif
	int	reqid;
	int	status;
	u_signed64	actual_size;
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
			int		retentd;	/* retention period in days */
			int		side;	/* size (for deviced like DVD accessed as if it was a tape request) */
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
			char	xfile[STAGE_MAX_HSMLENGTH+1];
	    } m;
	    struct {			/* HSM files (CASTOR) */
			char	xfile[STAGE_MAX_HSMLENGTH+1];
			char	server[CA_MAXHOSTNAMELEN+1];
			u_signed64	fileid;
			short	fileclass;
			char	tppool[CA_MAXPOOLNAMELEN+1];
			int		retenp_on_disk; /* Overwriten retention period on disk (-1 == default) */
			int		mintime_beforemigr; /* Overwriten mintime before migration (-1 == default) */
	    } h;
	} u1;
};

struct stgpath_entry {		/* entry format in STGPATH table */
	int	reqid;
	char	upath[(CA_MAXHOSTNAMELEN+MAXPATH)+1];
};

#endif /* __stage_struct_h */
