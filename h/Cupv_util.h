/*
 * $Id: Cupv_util.h,v 1.2 2006/12/05 14:00:43 riojac3 Exp $
 */

/*
 * Copyright (C) 1999-2002 by CERN/IT/DS/HSM
 * All rights reserved
 */
 
/*
 * @(#)$RCSfile: Cupv_util.h,v $ $Revision: 1.2 $ $Date: 2006/12/05 14:00:43 $ CERN IT-DS/HSM Ben Couturier
 */

#ifndef _CUPV_UTIL_H
#define _CUPV_UTIL_H
#include "osdep.h"
#include "Cupv_constants.h" 
#include "Cupv_struct.h"

#define INC_PTR(ptr,n)		(ptr) = (char*)(ptr) + (n)

#define  WRITE_STR(ptr,str, first)     { if (!first) { \
                                     (void) strcpy((char*)(ptr),(char*)(STR_SEP)); INC_PTR(ptr,strlen(STR_SEP)); } \
                                        (void) strcpy((char*)(ptr),(char*)(str)); \
					  INC_PTR(ptr,strlen(str)); \
                                          first = 0; }
					

/* Funtions fron Cupv_util.c */
int send2Cupv(int *socketp,char *reqp,int reql,char *user_repbuf,int user_repbuf_len);
int Cupv_errmsg(char *func, char *msg, ...);
EXTERN_C int  Cupv_strtoi (int *,char *,char **, int);
EXTERN_C void Cupv_util_time (time_t, char *);
EXTERN_C int Cupv_parse_privstring (char *);
EXTERN_C void Cupv_build_privstring (int , char *);
EXTERN_C int Cupv_getuid (const char *name);
EXTERN_C int Cupv_getgid (const char *name);
EXTERN_C char *Cupv_getuname (uid_t);
EXTERN_C char *Cupv_getgname (gid_t);

#endif






