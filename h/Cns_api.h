/*
 * Cns_api.h,v 1.44 2004/03/03 08:50:30 bcouturi Exp
 */

/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)Cns_api.h,v 1.44 2004/03/03 08:50:30 CERN IT-PDP/DM Jean-Philippe Baud
 */

#ifndef _CNS_API_H
#define _CNS_API_H
#if defined(_WIN32)
#include <sys/utime.h>
#else
#include <utime.h>
#endif
#include "Cns_constants.h"
#include "osdep.h"

int *C__Cns_errno();
#define Cns_errno (*C__Cns_errno())

#define	CNS_LIST_BEGIN		0
#define	CNS_LIST_CONTINUE	1
#define	CNS_LIST_END		2

struct Cns_api_thread_info {
	u_signed64	cwd;		/* current HSM working directory */
	char *		errbufp;
	int		errbuflen;
	int		initialized;
	int		ns_errno;
	mode_t		mask;		/* current HSM umask */
	char		server[CA_MAXHOSTNAMELEN+1];	/* current HSM Name Server */
        /*  Authorization ID used by the API, otherwise it uses geteuid/getegid
	    In any case these uid/gid are only trusted if the clients has service credentials */
        int             use_authorization_id;
        uid_t           uid;
        gid_t           gid;
};

typedef struct {
	int		dd_fd;		/* socket for communication with server */
	u_signed64	fileid;
	int		bod;		/* beginning of directory */
	int		eod;		/* end of directory */
	int		dd_loc;		/* offset in buffer */
	int		dd_size;	/* amount of data in buffer */
	char		*dd_buf;	/* directory buffer */
} Cns_DIR;

struct Cns_direncomm {
	char		*comment;
	unsigned short	d_reclen;	/* length of this entry */
	char		d_name[1];
};

struct Cns_direnstat {
	u_signed64	fileid;
	mode_t		filemode;
	int		nlink;		/* number of files in a directory */
	uid_t		uid;
	gid_t		gid;
	u_signed64	filesize;
	time_t		atime;		/* last access to file */
	time_t		mtime;		/* last file modification */
	time_t		ctime;		/* last metadata modification */
	short		fileclass;	/* 1 --> experiment, 2 --> user */
	char		status;		/* ' ' --> online, 'm' --> migrated */
	unsigned short	d_reclen;	/* length of this entry */
	char		d_name[1];
};

struct Cns_direnstatc {
	u_signed64	fileid;
	mode_t		filemode;
	int		nlink;		/* number of files in a directory */
	uid_t		uid;
	gid_t		gid;
	u_signed64	filesize;
	time_t		atime;		/* last access to file */
	time_t		mtime;		/* last file modification */
	time_t		ctime;		/* last metadata modification */
	short		fileclass;	/* 1 --> experiment, 2 --> user */
	char		status;		/* ' ' --> online, 'm' --> migrated */
	char		*comment;
	unsigned short	d_reclen;	/* length of this entry */
	char		d_name[1];
};

struct Cns_direntape {
	u_signed64	parent_fileid;
	u_signed64	fileid;
	int		copyno;
	int		fsec;		/* file section number */
	u_signed64	segsize;	/* file section size */
	int		compression;	/* compression factor */
	char		s_status;	/* 'd' --> deleted */
	char		vid[CA_MAXVIDLEN+1];
	char		checksum_name[CA_MAXCKSUMNAMELEN+1];
	unsigned long		checksum;
	int		side;
	int		fseq;		/* file sequence number */
	unsigned char	blockid[4];	/* for positionning with locate command */
	unsigned short	d_reclen;	/* length of this entry */
	char		d_name[1];
};

struct Cns_fileclass {
	int 	classid;
	char	name[CA_MAXCLASNAMELEN+1];
	uid_t	uid;
	gid_t	gid;
	int	min_filesize;	/* in Mbytes */
	int	max_filesize;	/* in Mbytes */
	int	flags;
	int	maxdrives;
	int	max_segsize;	/* in Mbytes */
	int	migr_time_interval;
	int	mintime_beforemigr;
	int	nbcopies;
	int	retenp_on_disk;
	int	nbtppools;
	char	*tppools;
};

struct Cns_fileid {
	char		server[CA_MAXHOSTNAMELEN+1];
	u_signed64	fileid;
};

struct Cns_filestat {
	u_signed64	fileid;
	mode_t		filemode;
	int		nlink;		/* number of files in a directory */
	uid_t		uid;
	gid_t		gid;
	u_signed64	filesize;
	time_t		atime;		/* last access to file */
	time_t		mtime;		/* last file modification */
	time_t		ctime;		/* last metadata modification */
	short		fileclass;	/* 1 --> experiment, 2 --> user */
	char		status;		/* ' ' --> online, 'm' --> migrated */
};

typedef struct {
	int		fd;		/* socket for communication with server */
	int		eol;		/* end of list */
	int		offset;		/* offset in buffer */
	int		len;		/* amount of data in buffer */
	char		*buf;		/* cache buffer for list entries */
} Cns_list;

struct Cns_segattrs {
	int		copyno;
	int		fsec;		/* file section number */
	u_signed64	segsize;	/* file section size */
	int		compression;	/* compression factor */
	char		s_status;	/* 'd' --> deleted */
	char		vid[CA_MAXVIDLEN+1];
	int		side;
	int		fseq;		/* file sequence number */
	unsigned char	blockid[4];	/* for positionning with locate command */
	char		checksum_name[CA_MAXCKSUMNAMELEN+1];
	unsigned long		checksum;
};

			/* function prototypes */

EXTERN_C int DLL_DECL Cns_access _PROTO((const char *, int));
EXTERN_C int DLL_DECL Cns_apiinit _PROTO((struct Cns_api_thread_info **));
EXTERN_C int DLL_DECL Cns_chclass _PROTO((const char *, int, char *));
EXTERN_C int DLL_DECL Cns_chdir _PROTO((const char *));
EXTERN_C int DLL_DECL Cns_chmod _PROTO((const char *, mode_t));
EXTERN_C int DLL_DECL Cns_chown _PROTO((const char *, uid_t, gid_t));
EXTERN_C int DLL_DECL Cns_close _PROTO((int));
EXTERN_C int DLL_DECL Cns_closedir _PROTO((Cns_DIR *));
EXTERN_C int DLL_DECL Cns_creat _PROTO((const char *, mode_t));
EXTERN_C int DLL_DECL Cns_creatx _PROTO((const char *, mode_t, struct Cns_fileid *));
EXTERN_C int DLL_DECL Cns_delcomment _PROTO((const char *));
EXTERN_C int DLL_DECL Cns_deleteclass _PROTO((char *, int, char *));
EXTERN_C int DLL_DECL Cns_enterclass _PROTO((char *, struct Cns_fileclass *));
EXTERN_C int DLL_DECL Cns_errmsg _PROTO((char *, char *, ...));
EXTERN_C int DLL_DECL Cns_getcomment _PROTO((const char *, char *));
EXTERN_C char DLL_DECL *Cns_getcwd _PROTO((char *, int));
EXTERN_C int DLL_DECL Cns_getpath _PROTO((char *, u_signed64, char *));
EXTERN_C int DLL_DECL Cns_getsegattrs _PROTO((const char *, struct Cns_fileid *, int *, struct Cns_segattrs **));
EXTERN_C struct Cns_fileclass DLL_DECL *Cns_listclass _PROTO((char *, int, Cns_list *));
EXTERN_C struct Cns_direntape DLL_DECL *Cns_listtape _PROTO((char *, char *, int, Cns_list *));
EXTERN_C int DLL_DECL Cns_mkdir _PROTO((const char *, mode_t));
EXTERN_C int DLL_DECL Cns_modifyclass _PROTO((char *, int, char *, struct Cns_fileclass *));
EXTERN_C int DLL_DECL Cns_open _PROTO((const char *, int, mode_t));
EXTERN_C Cns_DIR DLL_DECL *Cns_opendir _PROTO((const char *));
EXTERN_C int DLL_DECL Cns_queryclass _PROTO((char *, int, char *, struct Cns_fileclass *));
EXTERN_C struct dirent DLL_DECL *Cns_readdir _PROTO((Cns_DIR *));
EXTERN_C struct Cns_direncomm DLL_DECL *Cns_readdirc _PROTO((Cns_DIR *));
EXTERN_C struct Cns_direnstat DLL_DECL *Cns_readdirx _PROTO((Cns_DIR *));
EXTERN_C struct Cns_direnstatc DLL_DECL *Cns_readdirxc _PROTO((Cns_DIR *));
EXTERN_C struct Cns_direntape DLL_DECL *Cns_readdirxt _PROTO((Cns_DIR *));
EXTERN_C int DLL_DECL Cns_rename _PROTO((const char *, const char *));
EXTERN_C int DLL_DECL Cns_replaceseg _PROTO((char *, u_signed64, struct Cns_segattrs *, struct Cns_segattrs *));
EXTERN_C int DLL_DECL Cns_updateseg_checksum _PROTO((char *, u_signed64, struct Cns_segattrs *, struct Cns_segattrs *));
EXTERN_C void DLL_DECL Cns_rewinddir _PROTO((Cns_DIR *));
EXTERN_C int DLL_DECL Cns_rmdir _PROTO((const char *));
EXTERN_C int DLL_DECL Cns_selectsrvr _PROTO((const char *, char *, char *, char **));
EXTERN_C int DLL_DECL Cns_setatime _PROTO((const char *, struct Cns_fileid *));
EXTERN_C int DLL_DECL Cns_setcomment _PROTO((const char *, char *));
EXTERN_C int DLL_DECL Cns_seterrbuf _PROTO((char *, int));
EXTERN_C int DLL_DECL Cns_setfsize _PROTO((const char *, struct Cns_fileid *, u_signed64));
EXTERN_C int DLL_DECL Cns_setsegattrs _PROTO((const char *, struct Cns_fileid *, int, struct Cns_segattrs *));
EXTERN_C int DLL_DECL Cns_stat _PROTO((const char *, struct Cns_filestat *));
EXTERN_C int DLL_DECL Cns_statx _PROTO((const char *, struct Cns_fileid *, struct Cns_filestat *));
EXTERN_C mode_t DLL_DECL Cns_umask _PROTO((mode_t));
EXTERN_C int DLL_DECL Cns_unlink _PROTO((const char *));
EXTERN_C int DLL_DECL Cns_utime _PROTO((const char *, struct utimbuf *));
EXTERN_C int DLL_DECL send2nsd _PROTO((int *, char *, char *, int, char *, int));
EXTERN_C int DLL_DECL Cns_setid _PROTO((uid_t, gid_t));
EXTERN_C int DLL_DECL Cns_getid _PROTO((uid_t *, gid_t *));
EXTERN_C int DLL_DECL Cns_unsetid _PROTO(());
#endif
