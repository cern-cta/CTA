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

EXTERN_C int send2vmgr _PROTO((int *, char *, int, char *, int));
EXTERN_C int vmgr_apiinit _PROTO((struct vmgr_api_thread_info **));
EXTERN_C int vmgr_deletedenmap _PROTO((const char *, char *, char *));
EXTERN_C int vmgr_deletedgnmap _PROTO((const char *, char *));
EXTERN_C int vmgr_deletelibrary _PROTO((const char *));
EXTERN_C int vmgr_deletemodel _PROTO((const char *, char *));
EXTERN_C int vmgr_deletepool _PROTO((const char *));
EXTERN_C int vmgr_deletetape _PROTO((const char *));
EXTERN_C int vmgr_deltag _PROTO((const char *));
EXTERN_C int vmgr_enterdenmap _PROTO((const char *, char *, char *, int));
EXTERN_C int vmgr_enterdgnmap _PROTO((const char *, char *, char *));
EXTERN_C int vmgr_enterlibrary _PROTO((const char *, int, int));
EXTERN_C int vmgr_entermodel _PROTO((const char *, char *, int));
EXTERN_C int vmgr_enterpool _PROTO((const char *, uid_t, gid_t));
EXTERN_C int vmgr_entertape _PROTO((const char *, char *, char *, char *, char *, char *, char *, char *, char *, int, char *, int));
EXTERN_C int vmgr_errmsg _PROTO((char *, char *, ...));
EXTERN_C int vmgr_gettag _PROTO((const char *, char *));
EXTERN_C int vmgr_gettape _PROTO((const char *, u_signed64, const char *, char *, char *, char *, char *, char *, char *, int *, int *, u_signed64 *));
EXTERN_C struct vmgr_tape_denmap *vmgr_listdenmap _PROTO((int, vmgr_list *));
EXTERN_C struct vmgr_tape_dgnmap *vmgr_listdgnmap _PROTO((int, vmgr_list *));
EXTERN_C struct vmgr_tape_library *vmgr_listlibrary _PROTO((int, vmgr_list *));
EXTERN_C struct vmgr_tape_media *vmgr_listmodel _PROTO((int, vmgr_list *));
EXTERN_C struct vmgr_tape_pool *vmgr_listpool _PROTO((int, vmgr_list *));
EXTERN_C struct vmgr_tape_info *vmgr_listtape _PROTO((char *, char *, int, vmgr_list *));
EXTERN_C int vmgr_modifylibrary _PROTO((const char *, int, int));
EXTERN_C int vmgr_modifymodel _PROTO((const char *, char *, int));
EXTERN_C int vmgr_modifypool _PROTO((const char *, uid_t, gid_t));
EXTERN_C int vmgr_modifytape _PROTO((const char *, char *, char *, char *, char *, char *, char *, char *, int));
EXTERN_C int vmgr_querylibrary _PROTO((const char *, int *, int *, int *));
EXTERN_C int vmgr_querymodel _PROTO((const char *, char *, int *));
EXTERN_C int vmgr_querypool _PROTO((const char *, uid_t *, gid_t *, u_signed64 *, u_signed64 *));
EXTERN_C int vmgr_querytape _PROTO((const char *, int, struct vmgr_tape_info *, char *));
EXTERN_C int vmgr_reclaim _PROTO((const char *));
EXTERN_C int vmgr_seterrbuf _PROTO((char *, int));
EXTERN_C int vmgr_settag _PROTO((const char *, char *));
EXTERN_C int vmgr_tpmounted _PROTO((const char *, int, int));
EXTERN_C int vmgr_updatetape _PROTO((const char *, int, u_signed64, int, int, int));
EXTERN_C int vmgrcheck _PROTO((char *, char *, char *, char *, char *, int, uid_t, gid_t));
EXTERN_C int vmgrchecki _PROTO((char *, char *, char *, char *, char *, int, uid_t, gid_t, char *));
#endif
