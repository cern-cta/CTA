/*
 * Copyright (C) 1990-1998 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)rfstatfs.c	1.9 06/05/98  CERN CN-SW/DC Felix Hassine";
#endif /* not lint */

#define RFIO_KERNEL 1
#if !defined(_WIN32)
#include <sys/param.h>
#if defined(SOLARIS)
#include <sys/types.h>
#include <sys/statvfs.h>
#endif
#if defined(_AIX) || defined(__Lynx__)
#include <sys/types.h>
#endif
#if defined(sgi) || (defined( _AIX ) && defined( _IBMR2) ) || defined(__Lynx__)
#include <sys/statfs.h>
#endif
#if (defined(_AIX) && defined(_IBMESA)) || (defined(__osf__) && defined(__alpha))
#include <sys/mount.h>
#endif
#if (defined( sun ) && !defined(SOLARIS)) || defined( hpux ) || defined(linux)
#include <sys/vfs.h>
#endif
#if defined(ultrix)
#include <sys/types.h>
#include <sys/param.h>
#include <sys/mount.h>
#endif
#endif
#include <rfio.h>

int rfstatfs(path, statfsbuf )
char *path ;
struct rfstatfs *statfsbuf ;
{
	int status = 0   ;

#if defined(_WIN32)
	DWORD bps, nfc, spc, tnc;
	char *p, *rootpath;
	char pathbuf[256];
#else
#if defined( ultrix )
        struct fs_data fsbuffer;
#else
#if defined( SOLARIS )
	static struct statvfs fsbuffer ;
#else
        static struct statfs fsbuffer;
#endif
#endif
#endif
#if defined(__osf__) && defined(__alpha)
            if ( statfs(path,&fsbuffer,(int)sizeof(struct statfs)) < 0) {
                status = -1;
            }
#endif
#if defined( cray ) || defined( apollo ) || defined( sgi )
            if ( statfs(path,&fsbuffer,(int)sizeof(struct statfs),(int)0) < 0) {
                status = -1;
            }
#endif
#if ( defined( sun ) && !defined(SOLARIS) ) || defined( _AIX ) || defined( hpux ) || defined(linux)
            if ( statfs(path,&fsbuffer) < 0 ) {
                status = -1;
            }
#endif
#if defined( ultrix )
            if ( statfs(path,&fsbuffer) < 1 ) {
                status = -1;
            }
#endif
#if defined ( SOLARIS )
	    if (statvfs (  path, &fsbuffer ) < 0) {
		status = -1;
	    }
#endif
#if defined(__Lynx__)
            if ( statfs(path,&fsbuffer,sizeof(struct statfs),0) < 0 ) {
                status = -1;
            }
#endif
#if defined(_WIN32)
	    if (*(path+1) == ':') {		/* drive name */
		pathbuf[0] = *path;
		pathbuf[1] = *(path+1);
		pathbuf[2] = '\\';
		pathbuf[3] = '\0';
		rootpath = pathbuf;
	    } else if (*path == '\\' && *(path+1) == '\\') {	/* UNC name */
		if ((p = strchr (path+2, '\\')) == 0)
		    return (-1);
		if (p = strchr (p+1, '\\'))
		    strncpy (pathbuf, path, p-path+1);
		else {
		    strcpy (pathbuf, path);
		    strcat (pathbuf, "\\");
		}
		rootpath = pathbuf;
	    } else
		rootpath = NULL;
	    if (GetDiskFreeSpace (rootpath, &spc, &bps, &nfc, &tnc) == 0) {
		status = -1;
	    }
#endif

        /*
         * Affecting variables
         */

	if  ( status == 0 ) {
#if defined( ultrix )
           statfsbuf->bsize = (long)1024;
           statfsbuf->totblks = (long)fsbuffer.fd_req.btot;
           statfsbuf->freeblks = (long)fsbuffer.fd_req.bfreen;
           statfsbuf->freenods = (long)fsbuffer.fd_req.gfree ;
           statfsbuf->totnods = (long)fsbuffer.fd_req.gtot ;
#endif
#if defined( sgi ) || defined( __Lynx__ )
           statfsbuf->freeblks = (long)fsbuffer.f_bfree;
           statfsbuf->bsize = (long)fsbuffer.f_bsize ;
           statfsbuf->totblks = (long)fsbuffer.f_blocks;
           statfsbuf->totnods = (long)fsbuffer.f_files ;
           statfsbuf->freenods = (long)fsbuffer.f_ffree ;
#endif
#if defined(SOLARIS)
           statfsbuf->freeblks = (long)fsbuffer.f_bfree;
           statfsbuf->bsize = (long)fsbuffer.f_frsize ;
           statfsbuf->totblks = (long)fsbuffer.f_blocks;
           statfsbuf->totnods = (long)fsbuffer.f_files ;
           statfsbuf->freenods = (long)fsbuffer.f_ffree ;
#endif
#if (defined( sun ) && !defined(SOLARIS) ) || defined( hpux ) || defined( _AIX ) || defined(linux)
           statfsbuf->totblks = (long)fsbuffer.f_blocks;
           statfsbuf->freeblks = (long)fsbuffer.f_bavail;
           statfsbuf->totnods = (long)fsbuffer.f_files ;
           statfsbuf->freenods = (long)fsbuffer.f_ffree ;
           statfsbuf->bsize = (long)fsbuffer.f_bsize;
#endif
#if defined(__osf__) && defined(__alpha)
           statfsbuf->totblks = (long)fsbuffer.f_blocks;
           statfsbuf->freeblks = (long)fsbuffer.f_bavail;
           statfsbuf->totnods = (long)fsbuffer.f_files ;
           statfsbuf->freenods = (long)fsbuffer.f_ffree ;
           statfsbuf->bsize = (long)fsbuffer.f_fsize ;
#endif
#if defined(_WIN32)
           statfsbuf->totblks = (long)tnc;
           statfsbuf->freeblks = (long)nfc;
           statfsbuf->totnods = (long)-1;
           statfsbuf->freenods = (long)-1;
           statfsbuf->bsize = (long)(spc * bps);
#endif
 	}
	return status ;

}

