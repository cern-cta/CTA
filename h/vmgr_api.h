/*
 * $Id: vmgr_api.h,v 1.9 2000/02/10 11:14:28 jdurand Exp $
 */

/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: vmgr_api.h,v $ $Revision: 1.9 $ $Date: 2000/02/10 11:14:28 $ CERN IT-PDP/DM Jean-Philippe Baud
 */

#ifndef _VMGR_API_H
#define _VMGR_API_H
#include "vmgr_constants.h"
#include "osdep.h"

int *__vmgr_errno();
#define vmgr_errno (*__vmgr_errno())

struct vmgr_api_thread_info {
	char *		errbufp;
	int		errbuflen;
	int		initialized;
	int		vm_errno;
};

			/* function prototypes */

#if defined(__STDC__)
extern int vmgr_deletemodel(const char *, char *);
extern int vmgr_deletepool(const char *);
extern int vmgr_deletetape(const char *);
extern int vmgr_entermodel(const char *, char *, int, int);
extern int vmgr_enterpool(const char *, uid_t, gid_t);
extern int vmgr_entertape(const char *, char *, char *, char *, char *, char *, char *, char *, char *, char *);
extern int vmgr_gettape(const char *, u_signed64, const char *, char *, char *, char *, char *, char *, int *, unsigned int *);
extern int vmgr_modifymodel(const char *, char *, int, int);
extern int vmgr_modifypool(const char *, uid_t, gid_t);
extern int vmgr_modifytape(const char *, char *, char *, char *, char *, char *, char *, char *, char *, char *);
extern int vmgr_querymodel(const char *, char *, int *, int *);
extern int vmgr_querytape(const char *, char *, char *, char *, char *, char *, char *, char *, char *, char *, int *, int *, int *);
extern int vmgr_updatetape(const char *, u_signed64, int, int, int);
#else
extern int vmgr_deletemodel();
extern int vmgr_deletepool();
extern int vmgr_deletetape();
extern int vmgr_entermodel();
extern int vmgr_enterpool();
extern int vmgr_entertape();
extern int vmgr_gettape();
extern int vmgr_modifymodel();
extern int vmgr_modifypool();
extern int vmgr_modifytape();
extern int vmgr_querymodel();
extern int vmgr_querytape();
extern int vmgr_updatetape();
#endif
#endif
