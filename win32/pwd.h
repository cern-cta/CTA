/*
 * $Id: pwd.h,v 1.7 2001/05/21 11:00:27 baud Exp $
 */

/*
 * @(#)$RCSfile: pwd.h,v $ $Revision: 1.7 $ $Date: 2001/05/21 11:00:27 $ CERN IT-PDP/DC Frederic Hemmer
 */
 
/*
 * Copyright (C) 1993-2001 by CERN/IT/PDP/DC
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

EXTERN_C char          DLL_DECL *cuserid _PROTO((char *));
EXTERN_C struct passwd DLL_DECL *fillpwent _PROTO((char *));
EXTERN_C gid_t         DLL_DECL  getgid _PROTO(());
EXTERN_C struct passwd DLL_DECL *getpwnam _PROTO((char *));
EXTERN_C struct passwd DLL_DECL *getpwuid _PROTO((uid_t));
EXTERN_C uid_t         DLL_DECL  getuid _PROTO(());
EXTERN_C gid_t         DLL_DECL  getegid _PROTO(());
EXTERN_C uid_t         DLL_DECL  geteuid _PROTO(());
EXTERN_C int           DLL_DECL  setuid _PROTO((uid_t));
EXTERN_C int           DLL_DECL  setgid _PROTO((gid_t));

#endif /* __win32_pwd_h */
