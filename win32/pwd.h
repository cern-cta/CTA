/*
 * @(#)pwd.h	1.1 08/14/97 CERN CN-SW/DC Frederic Hemmer
 */
 
/*
 * Copyright (C) 1993 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef __win32_pwd_h
#define __win32_pwd_h

#include <osdep.h>

struct passwd
{
	char* pw_name;
	char* pw_passwd;
	uid_t pw_uid;
	gid_t pw_gid;
	char* pw_gecos;
	char* pw_dir;
	char* pw_shell;
};

EXTERN_C struct passwd DLL_DECL *getpwent _PROTO((void));
EXTERN_C void          DLL_DECL  setpwent _PROTO((void));
EXTERN_C void          DLL_DECL  endpwent _PROTO((void));

EXTERN_C struct passwd DLL_DECL *getpwuid _PROTO((uid_t));
EXTERN_C struct passwd DLL_DECL *getpwnam _PROTO((char *));

#endif /* __win32_pwd_h */
