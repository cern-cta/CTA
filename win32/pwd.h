/*
 * @(#)pwd.h	1.1 08/14/97 CERN CN-SW/DC Frederic Hemmer
 */
 
/*
 * Copyright (C) 1993 by CERN/CN/SW/DC
 * All rights reserved
 */

typedef long gid_t;
typedef long uid_t;
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

struct passwd* getpwent(void);
void           setpwent(void);
void           endpwent(void);

struct passwd* getpwuid(uid_t user_Id);
struct passwd* getpwnam(char* user_Name);

