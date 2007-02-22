/*
 * $Id: Ctape_api.h,v 1.25 2007/02/22 17:27:43 wiebalck Exp $
 */

/*
 * Copyright (C) 1994-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: Ctape_api.h,v $ $Revision: 1.25 $ $Date: 2007/02/22 17:27:43 $ CERN IT-PDP/DM Jean-Philippe Baud
 */

#ifndef _CTAPE_API_H
#define _CTAPE_API_H
#include "Ctape_constants.h"
#include "osdep.h"

typedef struct {
	unsigned long from_host;
	unsigned long to_tape;
	unsigned long from_tape;
	unsigned long to_host;
} COMPRESSION_STATS;
struct devinfo {	/* device characteristics */
	char	devtype[CA_MAXDVTLEN+1];
	int	bsr;		/* support backspace block */
	int	eoitpmrks;	/* number of tapemarks at EOI */
	int	fastpos;	/* use fast positionning because of directory */
	int	lddtype;	/* load display type */
	int	minblksize;
	int	maxblksize;
	int	defblksize;
	unsigned char	comppage;	/* compression page 0x0F or 0x10 */
	struct {
		short den;		/* density code */
		unsigned char code;	/* code to send to the drive */
	} dencodes[CA_MAXDENFIELDS];
};
struct dgn_rsv {		/* device group reservation */
	char	name[CA_MAXDGNLEN+1];
	int	num;
};
struct drv_status {		/* tape status reply entry */
	uid_t	uid;
	int	jid;		/* process-group-id */
	char	dgn[CA_MAXDGNLEN+1];	/* device group name */
	short	status;		/* drive status: down = 0, up = 1 */
	short	asn;		/* assign flag: assigned = 1 */
	time_t	asn_time;	/* timestamp of drive assignment */
	char	drive[CA_MAXUNMLEN+1];	/* drive name */
	int	mode;		/* WRITE_DISABLE or WRITE_ENABLE */
	char	lbltype[CA_MAXLBLTYPLEN+1];  /* label type: al, nl, sl or blp */
	int	tobemounted;	/* 1 means tape to be mounted */
	char	vid[CA_MAXVIDLEN+1];
	char	vsn[CA_MAXVSNLEN+1];
	int	cfseq;		/* current file sequence number */
};
struct dgn_rsv_status {		/* device group reservation status entry */
	char	name[CA_MAXDGNLEN+1];	/* device group name */
	int	rsvd;		/* number reserved */
	int	used;		/* number used */
};
struct rsv_status {		/* resource reservation status reply entry */
	uid_t	uid;
	int	jid;		/* process-group-id */
	int	count;		/* # of different device groups */
	struct dgn_rsv_status *dg;
};

			/* function prototypes */

EXTERN_C int DLL_DECL Ctape_config _PROTO((char *, int, int));
EXTERN_C struct devinfo DLL_DECL *Ctape_devinfo _PROTO((char *));
EXTERN_C int DLL_DECL Ctape_dmpend();
EXTERN_C int DLL_DECL Ctape_dmpfil _PROTO((char *, char *, int *, char *, int *, int *, int *, char *, u_signed64 *));
EXTERN_C int DLL_DECL Ctape_dmpinit _PROTO((char *, char *, char *, char *, char *, int, int, int, int, int, int, int, int));
EXTERN_C void DLL_DECL (*Ctape_dmpmsg) _PROTO((int, const char *, ...));
EXTERN_C int DLL_DECL Ctape_drvinfo _PROTO((char *, struct devinfo *));
EXTERN_C int DLL_DECL Ctape_errmsg _PROTO((char *, char *, ...));
EXTERN_C int DLL_DECL Ctape_info _PROTO((char *, int *, unsigned char *, char *, char *, char *, char *, int *, int *, char *));
EXTERN_C int DLL_DECL Ctape_kill _PROTO((char *));
EXTERN_C int DLL_DECL Ctape_label _PROTO((char *, char *, int, char *, char *, char *, char *, char *, int, int, int));
EXTERN_C int DLL_DECL Ctape_mount _PROTO((char *, char *, int, char *, char *, char *, int, char *, char *, int));
EXTERN_C int DLL_DECL Ctape_position _PROTO((char *, int, int, int, unsigned char *, int, int, int, char *, char *, char *, int, int, int, int));
EXTERN_C int DLL_DECL Ctape_reserve _PROTO((int, struct dgn_rsv *));
EXTERN_C int DLL_DECL Ctape_rls _PROTO((char *, int));
EXTERN_C int DLL_DECL Ctape_rstatus _PROTO((char *, struct rsv_status *, int, int));
EXTERN_C void DLL_DECL Ctape_seterrbuf _PROTO((char *, int));
EXTERN_C int DLL_DECL Ctape_status _PROTO((char *, struct drv_status *, int));
EXTERN_C int DLL_DECL send2tpd _PROTO((char *, char *, int, char *, int));

EXTERN_C int DLL_DECL get_compression_stats _PROTO((int, char *, char*, COMPRESSION_STATS *comp_stats));
EXTERN_C int DLL_DECL clear_compression_stats _PROTO((int, char *, char *));

EXTERN_C int DLL_DECL findpgrp();
EXTERN_C int DLL_DECL chkdirw _PROTO((char *));
EXTERN_C int DLL_DECL cvtden _PROTO((char *));
EXTERN_C int DLL_DECL read_pos _PROTO((int, char *, unsigned char *));
#if defined(_WIN32)
EXTERN_C int DLL_DECL skiptpff _PROTO((HANDLE, char *, int));
#else
EXTERN_C int DLL_DECL skiptpff _PROTO((int, char *, int));
#endif
#if defined(_WIN32)
EXTERN_C int DLL_DECL skiptpfb _PROTO((HANDLE, char *, int));
#else
EXTERN_C int DLL_DECL skiptpfb _PROTO((int, char *, int));
#endif
EXTERN_C int DLL_DECL gettperror _PROTO((int, char *, char **));
EXTERN_C int DLL_DECL rpttperror _PROTO((char *, int, char *, char *));
EXTERN_C int DLL_DECL readlbl _PROTO((int, char*, char *));
EXTERN_C int DLL_DECL writelbl _PROTO((int, char *, char*));
EXTERN_C void DLL_DECL ebc2asc _PROTO((char *, int));
EXTERN_C void DLL_DECL asc2ebc _PROTO((char *, int));
#if defined(_WIN32)
EXTERN_C int DLL_DECL wrttpmrk _PROTO((HANDLE, char *, int));
#else
EXTERN_C int DLL_DECL wrttpmrk _PROTO((int, char *, int));
#endif
#if defined(_WIN32)
EXTERN_C int DLL_DECL rwndtape _PROTO((HANDLE, char *));
#else
EXTERN_C int DLL_DECL rwndtape _PROTO((int, char *));
#endif
EXTERN_C int DLL_DECL usrmsg _PROTO((char *, char *, ...));
#endif
