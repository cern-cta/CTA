/*
 * $Id: vmgr_api.h,v 1.24 2001/01/31 12:41:14 baud Exp $
 */

/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: vmgr_api.h,v $ $Revision: 1.24 $ $Date: 2001/01/31 12:41:14 $ CERN IT-PDP/DM Jean-Philippe Baud
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

EXTERN_C int DLL_DECL vmgr_deletedenmap _PROTO((const char *, char *, char *));
EXTERN_C int DLL_DECL vmgr_deletemodel _PROTO((const char *, char *));
EXTERN_C int DLL_DECL vmgr_deletepool _PROTO((const char *));
EXTERN_C int DLL_DECL vmgr_deletetape _PROTO((const char *));
EXTERN_C int DLL_DECL vmgr_enterdenmap _PROTO((const char *, char *, char *));
EXTERN_C int DLL_DECL vmgr_entermodel _PROTO((const char *, char *, int, int));
EXTERN_C int DLL_DECL vmgr_enterpool _PROTO((const char *, uid_t, gid_t));
EXTERN_C int DLL_DECL vmgr_entertape _PROTO((const char *, char *, char *, char *, char *, char *, char *, char *, char *, char *, int));
EXTERN_C int DLL_DECL vmgr_gettape _PROTO((const char *, u_signed64, const char *, char *, char *, char *, char *, char *, char *, int *, u_signed64 *));
EXTERN_C struct vmgr_tape_denmap DLL_DECL *vmgr_listdenmap _PROTO((int, vmgr_list *));
EXTERN_C struct vmgr_tape_media DLL_DECL *vmgr_listmodel _PROTO((int, vmgr_list *));
EXTERN_C struct vmgr_tape_pool DLL_DECL *vmgr_listpool _PROTO((int, vmgr_list *));
EXTERN_C struct vmgr_tape_info DLL_DECL *vmgr_listtape _PROTO((char *, int, vmgr_list *));
EXTERN_C int DLL_DECL vmgr_modifymodel _PROTO((const char *, char *, int, int));
EXTERN_C int DLL_DECL vmgr_modifypool _PROTO((const char *, uid_t, gid_t));
EXTERN_C int DLL_DECL vmgr_modifytape _PROTO((const char *, char *, char *, char *, char *, char *, char *, char *, int));
EXTERN_C int DLL_DECL vmgr_querymodel _PROTO((const char *, char *, int *, int *));
EXTERN_C int DLL_DECL vmgr_querypool _PROTO((const char *, uid_t *, gid_t *, u_signed64 *, u_signed64 *));
EXTERN_C int DLL_DECL vmgr_querytape _PROTO((const char *, char *, char *, char *, char *, char *, char *, char *, char *, char *, int *, int *, int *, int *, time_t *, time_t *, int *));
EXTERN_C int DLL_DECL vmgr_reclaim _PROTO((const char *));
EXTERN_C int DLL_DECL vmgr_seterrbuf _PROTO((char *, int));
EXTERN_C int DLL_DECL vmgr_tpmounted _PROTO((const char *, int));
EXTERN_C int DLL_DECL vmgr_updatetape _PROTO((const char *, u_signed64, int, int, int));
#endif
