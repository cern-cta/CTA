/*
 * $Id: Cupv_util.h,v 1.2 2006/12/05 14:00:43 riojac3 Exp $
 */

/*
 * Copyright (C) 1999-2002 by CERN/IT/DS/HSM
 * All rights reserved
 */
 
/*
 */

#ifndef _CUPV_UTIL_H
#define _CUPV_UTIL_H
#include "osdep.h"
#include "Cupv_constants.h" 
#include "Cupv_struct.h"

/* Funtions fron Cupv_util.c */
int send2Cupv(int *socketp,char *reqp,int reql,char *user_repbuf,int user_repbuf_len);
int Cupv_errmsg(char *func, char *msg, ...);
EXTERN_C int  Cupv_strtoi (int *,char *,char **, int);
EXTERN_C void Cupv_util_time (time_t, char *);
EXTERN_C int Cupv_parse_privstring (char *);
EXTERN_C char* Cupv_build_privstring (int);
EXTERN_C int Cupv_getuid (const char *name);
EXTERN_C int Cupv_getgid (const char *name);
EXTERN_C char *Cupv_getuname (uid_t);
EXTERN_C char *Cupv_getgname (gid_t);

#endif






