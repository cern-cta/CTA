/*
 * $Id: Ctape_api.h,v 1.9 2000/02/16 06:50:53 baud Exp $
 */

/*
 * Copyright (C) 1994-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: Ctape_api.h,v $ $Revision: 1.9 $ $Date: 2000/02/16 06:50:53 $ CERN IT-PDP/DM Jean-Philippe Baud
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
struct dgn_rsv {		/* device group reservation */
	char	name[CA_MAXDGNLEN+1];
	int	num;
};
struct drv_status {		/* tape status reply entry */
	uid_t	uid;
	int	jid;		/* process-group-id */
	char	dgn[CA_MAXDGNLEN+1];	/* device group name */
	int	status;		/* drive status: down = 0, up = 1 */
	int	asn;		/* assign flag: assigned = 1 */
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

#if defined(__STDC__)
extern int Ctape_config(char *, int, int);
extern int Ctape_info(char *, int *, unsigned int *, char *, char *, char *,
	char *, int *, int *, char *);
extern int Ctape_kill(char *);
extern int Ctape_label(char *, char *, int, char *, char *, char *, char *,
	char *, int, int);
extern int Ctape_mount(char *, char *, int, char *, char *, char *, int,
	char *, char *, int);
extern int Ctape_position(char *, int, int, int, unsigned int, int, int,
	int, char *, char *, int, int, int, int);
extern int Ctape_reserve(int, struct dgn_rsv *);
extern int Ctape_rls(char *, int);
extern int Ctape_rstatus(char *, struct rsv_status *, int, int);
extern void Ctape_seterrbuf(char *, int);
extern int Ctape_status(char *, struct drv_status *, int);
#else
extern int Ctape_config();
extern int Ctape_info();
extern int Ctape_kill();
extern int Ctape_label();
extern int Ctape_mount();
extern int Ctape_position();
extern int Ctape_reserve();
extern int Ctape_rls();
extern int Ctape_rstatus();
extern void Ctape_seterrbuf();
extern int Ctape_status();
#endif
#endif
