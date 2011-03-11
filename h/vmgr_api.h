/*
 * $Id: vmgr_api.h,v 1.31 2003/10/29 12:17:23 baud Exp $
 */

/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: vmgr_api.h,v $ $Revision: 1.31 $ $Date: 2003/10/29 12:17:23 $ CERN IT-PDP/DM Jean-Philippe Baud
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

EXTERN_C int send2vmgr (int *, char *, int, char *, int);
EXTERN_C int vmgr_apiinit (struct vmgr_api_thread_info **);
EXTERN_C int vmgr_deletedenmap (const char *, char *, char *);
EXTERN_C int vmgr_deletedgnmap (const char *, char *);
EXTERN_C int vmgr_deletelibrary (const char *);
EXTERN_C int vmgr_deletemodel (const char *, char *);
EXTERN_C int vmgr_deletepool (const char *);
EXTERN_C int vmgr_deletetape (const char *);
EXTERN_C int vmgr_deltag (const char *);
EXTERN_C int vmgr_enterdenmap (const char *, char *, char *, int);
EXTERN_C int vmgr_enterdenmap_byte_u64 (const char *, char *, char *, const u_signed64);
EXTERN_C int vmgr_enterdgnmap (const char *, char *, char *);
EXTERN_C int vmgr_enterlibrary (const char *, int, int);
EXTERN_C int vmgr_entermodel (const char *, char *, int);
EXTERN_C int vmgr_enterpool (const char *, uid_t, gid_t);
EXTERN_C int vmgr_entertape (const char *, char *, char *, char *, char *, char *, char *, char *, char *, int, char *, int);
EXTERN_C int vmgr_errmsg (char *, char *, ...);
EXTERN_C int vmgr_gettag (const char *, char *);
EXTERN_C int vmgr_gettape (const char *, u_signed64, const char *, char *, char *, char *, char *, char *, char *, int *, int *, u_signed64 *);
EXTERN_C struct vmgr_tape_denmap *vmgr_listdenmap (int, vmgr_list *);
EXTERN_C struct vmgr_tape_denmap_byte_u64 *vmgr_listdenmap_byte_u64 (int, vmgr_list *);
EXTERN_C struct vmgr_tape_dgnmap *vmgr_listdgnmap (int, vmgr_list *);
EXTERN_C struct vmgr_tape_library *vmgr_listlibrary (int, vmgr_list *);
EXTERN_C struct vmgr_tape_media *vmgr_listmodel (int, vmgr_list *);
EXTERN_C struct vmgr_tape_pool *vmgr_listpool (int, vmgr_list *);
EXTERN_C struct vmgr_tape_info *vmgr_listtape (char *, char *, int, vmgr_list *);
EXTERN_C struct vmgr_tape_info_byte_u64 *vmgr_listtape_byte_u64 (char *, char *, int, vmgr_list *);
EXTERN_C int vmgr_modifylibrary (const char *, int, int);
EXTERN_C int vmgr_modifymodel (const char *, char *, int);
EXTERN_C int vmgr_modifypool (const char *, uid_t, gid_t);
EXTERN_C int vmgr_modifytape (const char *, char *, char *, char *, char *, char *, char *, char *, int);
EXTERN_C int vmgr_querylibrary (const char *, int *, int *, int *);
EXTERN_C int vmgr_querymodel (const char *, char *, int *);
EXTERN_C int vmgr_querypool (const char *, uid_t *, gid_t *, u_signed64 *, u_signed64 *);
EXTERN_C int vmgr_querytape (const char *, int, struct vmgr_tape_info *, char *);
EXTERN_C int vmgr_querytape_byte_u64 (const char *, int, struct vmgr_tape_info_byte_u64 *, char *);
EXTERN_C int vmgr_reclaim (const char *);
EXTERN_C int vmgr_seterrbuf (char *, int);
EXTERN_C int vmgr_settag (const char *, char *);
EXTERN_C int vmgr_tpmounted (const char *, int, int);
EXTERN_C int vmgr_updatetape (const char *, int, u_signed64, int, int, int);
EXTERN_C int vmgrcheck (char *, char *, char *, char *, char *, int, uid_t, gid_t);
EXTERN_C int vmgrchecki (char *, char *, char *, char *, char *, int, uid_t, gid_t, char *);
#endif
