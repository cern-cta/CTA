/*
 * $Id: Cupv_api.h,v 1.3 2006/12/05 14:00:43 riojac3 Exp $
 */

/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 */

#ifndef _CUPV_API_H
#define _CUPV_API_H
#include "osdep.h"
#include <sys/types.h>
#include "Cupv_constants.h"
#include "Cupv_struct.h"
#include "Cupv_util.h"

int *C__Cupv_errno();
#define Cupv_errno (*C__Cupv_errno())

#define	CUPV_LIST_BEGIN		0
#define	CUPV_LIST_CONTINUE	1
#define	CUPV_LIST_END		2

struct Cupv_api_thread_info {
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
} Cupv_entry_list;

int Cupv_apiinit(struct Cupv_api_thread_info **thip);
/* function prototypes */
EXTERN_C struct Cupv_userpriv *Cupv_list  (int, Cupv_entry_list *, struct Cupv_userpriv *);
EXTERN_C int Cupv_add (uid_t, gid_t, const char *, const char *, int);
EXTERN_C int Cupv_check (uid_t, gid_t, const char *, const char *, int);
EXTERN_C int Cupv_modify (uid_t, gid_t, const char *, const char *, const char *, const char *, int);
EXTERN_C int Cupv_delete (uid_t, gid_t, const char *, const char *);
EXTERN_C int Cupv_seterrbuf (char *, int);
#endif









