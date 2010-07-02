/*
 * $Id: Ctape_api.h,v 1.33 2008/02/21 18:25:27 waldron Exp $
 */

/*
 * Copyright (C) 1994-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: Ctape_api.h,v $ $Revision: 1.33 $ $Date: 2008/02/21 18:25:27 $ CERN IT-PDP/DM Jean-Philippe Baud
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

EXTERN_C int Ctape_config (char *, int, int);
EXTERN_C struct devinfo *Ctape_devinfo (char *);
EXTERN_C int Ctape_dmpend();
EXTERN_C int Ctape_dmpfil (char *, char *, int *, char *, int *, int *, int *, char *, u_signed64 *);
EXTERN_C int Ctape_dmpinit (char *, char *, char *, char *, char *, int, int, int, int, int, int, int, int);
EXTERN_C void (*Ctape_dmpmsg) (int, const char *, ...);
EXTERN_C int Ctape_drvinfo (char *, struct devinfo *);
EXTERN_C int Ctape_errmsg (char *, char *, ...);
EXTERN_C int Ctape_info (char *, int *, unsigned char *, char *, char *, char *, char *, int *, int *, char *);
EXTERN_C int Ctape_kill (char *);
EXTERN_C int Ctape_label (char *, char *, int, char *, char *, char *, char *, char *, int, int, int);
EXTERN_C int Ctape_mount (char *, char *, int, char *, char *, char *, int, char *, char *, int);
EXTERN_C int Ctape_position (char *, int, int, int, unsigned char *, int, int, int, char *, char *, char *, int, int, int, int);
EXTERN_C int Ctape_reserve (int, struct dgn_rsv *);
EXTERN_C int Ctape_rls (char *, int);
EXTERN_C int Ctape_rstatus (char *, struct rsv_status *, int, int);
EXTERN_C void Ctape_seterrbuf (char *, int);
EXTERN_C int Ctape_status (char *, struct drv_status *, int);
EXTERN_C int send2tpd (char *, char *, int, char *, int);

/* tape/asc2ebc.c      */
EXTERN_C void asc2ebc      (char *, int);

/* tape/buildhdrlbl.c  */
EXTERN_C int buildhdrlbl   (char[], char[], char*, char*, int, int, int, char*, int, int, int, int);

/* tape/builduhl.c     */
EXTERN_C int builduhl      (char[], int, int, int, char*, char*, char*, char*);

/* tape/buildvollbl.c  */
EXTERN_C int buildvollbl   (char[], char*, int, char*);

/* tape/chkdirw.c      */
EXTERN_C int chkdirw       (char *);

/* tape/checkjobdied.c */
EXTERN_C int checkjobdied  (int[]);

/* tape/cvtden.c       */
EXTERN_C int cvtden        (char *);

/* tape/ebc2asc.c      */
EXTERN_C void ebc2asc      (char *, int);

/* tape/getcompstat.c  */
EXTERN_C int clear_compression_stats (int, char *, char *);
EXTERN_C int get_compression_stats   (int, char *, char*, COMPRESSION_STATS *comp_stats);

/* tape/getdrvstatus.c */
EXTERN_C int chkdriveready (int);
EXTERN_C int chkwriteprot  (int);

/* tape/tapealertcheck.c */
EXTERN_C int get_tape_alerts (int, char*, char*);

/* tape/findpgrp.c     */
EXTERN_C int findpgrp();

/* tape/inquiry.c      */
EXTERN_C int inquiry       (int, char*, unsigned char*);
EXTERN_C int inquiry80     (int, char*, unsigned char*);

/* tape/mircheck.c     */
EXTERN_C int is_mir_invalid_load (int, char *, char *);

/* tape/locate.c       */
EXTERN_C int locate        (int, char*, unsigned char*);
EXTERN_C int read_pos      (int, char *, unsigned char *);

/* tape/posittape.c    */
EXTERN_C int posittape     (int, char*, char*, int, int, int*, char*, int, int, int, int, int, int, int, char*, char*, char*, char*);

/* tape/rbtsubr.c      */
/* EXTERN_C int rbtdemount    (char*, char*, char*, char*, unsigned int, int ); */
EXTERN_C int acsmountresp();
EXTERN_C int rbtmount      (char*, int, char*, char*, int, char*);
EXTERN_C int rbtdemount    (char*, char*, char*, char*, unsigned int, int);
EXTERN_C int wait4acsfinalresp();

/* tape/readlbl.c      */
EXTERN_C int readlbl       (int, char*, char *);
EXTERN_C void closesmc();

/* tape/rwndtape.c     */
EXTERN_C int rwndtape      (int, char *);

/* tape/sendrep.c      */
EXTERN_C int sendrep       (int, int, ...);

/* tape/setdens.c      */
EXTERN_C int setdens       (int, char*, char*, int);

/* tape/skiptape.c     */
EXTERN_C int skiptpfb      (int, char *, int);
EXTERN_C int skiptpff      (int, char *, int);
#if defined(linux)
EXTERN_C int skiptpfff     (int, char*, int);
#endif

/* tape/tperror.c      */
EXTERN_C int gettperror    (int, char *, char **);
EXTERN_C int rpttperror    (char *, int, char *, char *);

/* tape/unldtape.c     */
EXTERN_C int unldtape      (int, char*);

/* tape/usrmsg.c       */
EXTERN_C int usrmsg        (char *, char *, ...);

/* tape/writelbl.c     */
EXTERN_C int writelbl      (int, char *, char*);

/* tape/wrttpmrk.c     */
EXTERN_C int wrttpmrk      (int, char *, int);

#endif
