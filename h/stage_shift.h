/*
 * $Id: stage_shift.h,v 1.2 2000/01/10 06:57:30 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1998 by CERN/CN/PDP/DH
 * All rights reserved
 */

/*
 * static char sccsid[] = "@(#)stage.h	1.37 08/24/98 CERN IT-PDP/DM Jean-Philippe Baud";
 */

			/* stage daemon constants and macros */

#ifndef __stage_shift_h
#define __stage_shift_h

#define MAXFSEQ 15	/* maximum fseq string length for one disk file */
#define	MAXGRPNAMELEN	3
#ifdef MAXHOSTNAMELEN
#undef MAXHOSTNAMELEN
#endif
#define MAXHOSTNAMELEN  64
#ifdef MAXPATH
#undef MAXPATH
#endif
#define MAXPATH 80	/* maximum path length */
#define MAXPOOLNAMELEN	16
#define	MAXVSN 3	/* maximum number of vsns/vids on a stage command */

#if defined(_WIN32)
typedef long gid_t;
typedef long uid_t;
#endif

#if ! defined(vms)
struct stgcat_entry_old {		/* entry format in STGCAT table */
	int	blksize;	/* maximum block size */
	char	filler[2];
	char	charconv;	/* character conversion */
	char	keep;		/* keep data on disk after successful stagewrt */
	int	lrecl;		/* record length */
	int	nread;		/* number of blocks/records to be copied */
	char	poolname[MAXPOOLNAMELEN];
	char	recfm[4];	/* record format */
	int	size;		/* size in Mbytes of data to be staged */
	char	ipath[MAXHOSTNAMELEN+MAXPATH];	/* internal path */
	char	t_or_d;		/* 't' for tape/disk, 'd' for disk/disk */
	char	group[MAXGRPNAMELEN];
	char	user[15];	/* login name */
	uid_t	uid;		/* uid or Guid */
	gid_t	gid;
#if (defined(sun) && !defined(SOLARIS)) || defined(ultrix) || defined(vms) || defined(_WIN32)
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
		char	den[6];		/* density */
		char	dgn[9];		/* device group */
		char	fid[18];	/* file id */
		char	filstat;	/* file status: new = 'n', old = 'o' */
		char	fseq[MAXFSEQ];	/* file sequence number requested by user */
		char	lbl[4];		/* label type: al, nl, sl or blp */
		int	retentd;	/* retention period in days */
		char	tapesrvr[MAXHOSTNAMELEN];	/* tape server */
		char	E_Tflags;	/* SKIPBAD, KEEPFILE, NOTRLCHK */
		char	vid[MAXVSN][7];
		char	vsn[MAXVSN][7];
	    } t;
	    struct {			/* info for disk file stageing */
		char	xfile[MAXHOSTNAMELEN+MAXPATH];
		char	Xparm[23];
	    } d;
	    struct {			/* migrated files (HSM) */
		char	xfile[167];
	    } m;
	} u1;
};

struct stgpath_entry_old {		/* entry format in STGPATH table */
	int	reqid;
	char	upath[MAXHOSTNAMELEN+MAXPATH];
};
#endif /* ! defined(vms) */

#endif /* __stage_shift_h */
