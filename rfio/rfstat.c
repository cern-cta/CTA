/*
 * $Id: rfstat.c,v 1.9 2006/04/28 14:01:56 gtaur Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rfstat.c,v $ $Revision: 1.9 $ $Date: 2006/04/28 14:01:56 $ CERN/IT/PDP/DM fhe";
#endif /* not lint */

#include <rfio_api.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <pwd.h>
#include <grp.h>
#if defined(_WIN32)
#include <winsock2.h>
#include <statbits.h>
#endif
#include <u64subr.h>
#include <RfioTURL.h>

static char *ftype="dbclps-";
static char *perm="rwx-";
static char *setid="ssx";
static char *stickynox="ST";

void report_mode(mode)
unsigned int mode;
{
	static char letters[11]="----------";
	register unsigned int  m;
	register char   c;
/*
 * File type
 */

	m = mode&S_IFMT;
	switch (m) {
		case    S_IFDIR: c = 'd'; break;
		case    S_IFCHR: c = 'c'; break;
		case    S_IFBLK: c = 'b'; break;
		case    S_IFREG: c = '-'; break;
		case    S_IFLNK: c = 'l'; break;
		case    S_IFSOCK: c = 's'; break;
#if defined(_WIN32)
		case	_S_IFIFO: c = 'p'; break;
#else
#if defined(__Lynx__)
		case    S_IFFIFO: c = 'p'; break;
#else
		case    S_IFIFO: c = 'p'; break;
#endif
#endif
		default :       c = '?';
	}
	letters[0]=c;
	if (mode&S_IREAD) letters[1]='r';
	if (mode&S_IWRITE) letters[2]='w';
	if (mode&S_ISUID)       {
		if (mode&S_ISVTX)       {
			if (mode&S_IEXEC)
				letters[3]='s';
			else
				letters[3]='S';
		} else  {
			if (mode&S_IEXEC)
				letters[3]='s';
			else
				letters[3]='-';
		}
	} else {
		if (mode&S_IEXEC)
			letters[3]='x';
		else
			letters[3]='-';
	}
	if (mode&S_IRGRP) letters[4]='r';
	if (mode&S_IWGRP) letters[5]='w';
	if (mode&S_ISGID)       {
		if (mode&S_ISVTX)       {
			if (mode&S_IXGRP)
				letters[6]='s';
			else
				letters[6]='S';
		} else  {
			if (mode&S_IXGRP)
				letters[6]='s';
			else
				letters[6]='-';
		}
		if (mode&S_IXGRP)
			letters[6]='s';
		else
			letters[6]='-';
	} else {
		if (mode&S_IXGRP)
			letters[6]='x';
		else
			letters[6]='-';
	}
	if (mode & S_IROTH) letters[7]='r';
	if (mode & S_IWOTH) letters[8]='w';
	if (mode & S_IXOTH) letters[9]='x';
	if (mode&S_ISVTX) letters[9]='t';



	fprintf(stdout,"Protection      : %s (%o)\n",letters,mode);
}


void report(buf)
struct stat64    *buf;
{
	struct passwd   *pwd;
	struct group    *grp;
	char tmpbuf[21];

	fprintf(stdout,"Device          : %x\n",(int) buf->st_dev);
	fprintf(stdout,"Inode number    : %d\n",(int) buf->st_ino);
#if !defined(_WIN32)
	fprintf(stdout,"Nb blocks       : %d\n",(int) buf->st_blocks);
#endif /* _WIN32 */
	report_mode(buf->st_mode);
	fprintf(stdout,"Hard Links      : %d\n",(int) buf->st_nlink);
	if ((pwd = getpwuid(buf->st_uid)) == NULL)      {
		fprintf(stdout,"Uid             : %d (UNKNOWN)\n",(int) buf->st_uid);
	}
	else    {
		fprintf(stdout,"Uid             : %d (%s)\n",(int) buf->st_uid,pwd->pw_name);
	}
	if ((grp = getgrgid(buf->st_gid)) == NULL)      {
		fprintf(stdout,"Gid             : %d (UNKNOWN)\n",(int) buf->st_gid);
	}
	else    {
		fprintf(stdout,"Gid             : %d (%s)\n",(int) buf->st_gid,grp->gr_name);
	}
	fprintf(stdout,"Size (bytes)    : %s\n", u64tostr(buf->st_size, tmpbuf, 0));
	fprintf(stdout,"Last access     : %s",ctime(&buf->st_atime));
	fprintf(stdout,"Last modify     : %s",ctime(&buf->st_mtime));
	fprintf(stdout,"Last stat. mod. : %s",ctime(&buf->st_ctime));
}

main(argc,argv)
int     argc;
char    **argv;
{
	struct stat64     buf;
#if defined(_WIN32)
	WSADATA wsadata;
#endif
        RfioTURL_t turl;
        int ret;
        char* path=argv[1];
        ret= rfioTURLFromString(path,&turl);
        if(ret!=-1){path=turl.rfioPath;}

	if (argc != 2)  {
		fprintf(stderr,"usage: %s <file_path>\n",path);
		exit(1);
	}
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, "WSAStartup unsuccessful\n");
		exit (2);
	}
#endif
	

	if (rfio_stat64(path,&buf) < 0) {
		rfio_perror(path);
#if defined(_WIN32)
		WSACleanup();
#endif
		exit(1);
	}
	report(&buf);
#if defined(_WIN32)
	WSACleanup();
#endif
	exit(0);
}
