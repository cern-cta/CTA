/*
 * $Id: vmgr_api.h,v 1.19 2000/06/13 14:50:42 baud Exp $
 */

/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: vmgr_api.h,v $ $Revision: 1.19 $ $Date: 2000/06/13 14:50:42 $ CERN IT-PDP/DM Jean-Philippe Baud
 */

#ifndef _VMGR_API_H
#define _VMGR_API_H
#include "osdep.h"
#include "vmgr_constants.h"
#include "vmgr_struct.h"

int *C__vmgr_errno();
#define vmgr_errno (*C__vmgr_errno())

#define	VMGR_LIST_BEGIN		0
#define	VMGR_LIST_CONTINUE	1
#define	VMGR_LIST_END		2

struct vmgr_api_thread_info {
	char *		errbufp;
	int		errbuflen;
	int		initialized;
	int		vm_errno;
};

typedef struct {
	int		fd;		/* socket for communication with server */
	int		eol;		/* end of list */
	int		index;		/* entry index in buffer */
	int		nbentries;	/* number of entries in buffer */
	char		*buf;		/* cache buffer for list entries */
} vmgr_list;

			/* function prototypes */

#if defined(__STDC__)
extern int vmgr_deletedenmap(const char *, char *, char *);
extern int vmgr_deletemodel(const char *, char *);
extern int vmgr_deletepool(const char *);
extern int vmgr_deletetape(const char *);
extern int vmgr_enterdenmap(const char *, char *, char *);
extern int vmgr_entermodel(const char *, char *, int, int);
extern int vmgr_enterpool(const char *, uid_t, gid_t);
extern int vmgr_entertape(const char *, char *, char *, char *, char *, char *, char *, char *, char *, char *, int);
extern int vmgr_gettape(const char *, u_signed64, const char *, char *, char *, char *, char *, char *, char *, int *, u_signed64 *);
extern struct vmgr_tape_denmap *vmgr_listdenmap(int, vmgr_list *);
extern struct vmgr_tape_media *vmgr_listmodel(int, vmgr_list *);
extern struct vmgr_tape_pool *vmgr_listpool(int, vmgr_list *);
extern struct vmgr_tape_info *vmgr_listtape(int, vmgr_list *);
extern int vmgr_modifymodel(const char *, char *, int, int);
extern int vmgr_modifypool(const char *, uid_t, gid_t);
extern int vmgr_modifytape(const char *, char *, char *, char *, char *, char *, char *, char *, char *, char *, int);
extern int vmgr_querymodel(const char *, char *, int *, int *);
extern int vmgr_querytape(const char *, char *, char *, char *, char *, char *, char *, char *, char *, char *, int *, int *, int *, int *, time_t *, time_t *, int *);
extern int vmgr_reclaim(const char *);
extern int vmgr_tpmounted(const char *, int);
extern int vmgr_updatetape(const char *, u_signed64, int, int, int);
#else
extern int vmgr_deletedenmap();
extern int vmgr_deletemodel();
extern int vmgr_deletepool();
extern int vmgr_deletetape();
extern int vmgr_enterdenmap();
extern int vmgr_entermodel();
extern int vmgr_enterpool();
extern int vmgr_entertape();
extern int vmgr_gettape();
extern struct vmgr_tape_denmap *vmgr_listdenmap();
extern struct vmgr_tape_media *vmgr_listmodel();
extern struct vmgr_tape_pool *vmgr_listpool();
extern struct vmgr_tape_info *vmgr_listtape();
extern int vmgr_modifymodel();
extern int vmgr_modifypool();
extern int vmgr_modifytape();
extern int vmgr_querymodel();
extern int vmgr_querytape();
extern int vmgr_reclaim();
extern int vmgr_tpmounted();
extern int vmgr_updatetape();
#endif
#endif
