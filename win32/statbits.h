/*
 * @(#)$RCSfile: statbits.h,v $ $Revision: 1.1 $ $Date: 2000/09/01 07:57:33 $ Olof Barring CERN IT-PDP/DM
 */

/*
 * Copyright (C) 2000 by CERN IT-PDP/DM
 * All rights reserved
 */

/*
 * statbits.h - define non-existing file access macros for win32
 */
#ifndef STATBITS_H
#define STATBITS_H

#ifndef __S_IFMT
#define __S_IFMT        0170000    /* These bits determine file type */
#endif

#ifndef __S_IFDIR
#define __S_IFDIR       0040000    /* Directory */
#endif

#ifndef __S_IFCHR
#define __S_IFCHR       0020000    /* Character device */
#endif

#ifndef __S_IFIFO
#define __S_IFIFO       0010000    /* FIFO */
#endif

#ifndef __S_IFBLK
#define __S_IFBLK       0060000    /* Block device */
#endif

#ifndef __S_IFLNK
#define __S_IFLNK       0120000    /* Symbolic link */
#endif

#ifndef __S_IFSOCK
#define __S_IFSOCK      0140000    /* Socket */
#endif

#ifndef __S_ISUID
#define __S_ISUID       04000      /* Set user ID on execution */
#endif

#ifndef __S_ISGID
#define __S_ISGID       02000      /* Set group ID on execution */
#endif

#ifndef __S_ISVTX
#define __S_ISVTX       01000      /* Save swapped text after use (sticky) */
#endif

#ifndef __S_IREAD
#define __S_IREAD       0400       /* Read by owner */
#endif

#ifndef __S_IWRITE
#define __S_IWRITE      0200       /* Write by owner */
#endif

#ifndef __S_IEXEC
#define __S_IEXEC       0100       /* Execute by owner */
#endif

#ifndef S_IFIFO
#define S_IFIFO         __S_IFIFO
#endif

#ifndef S_IFBLK
#define S_IFBLK         __S_IFBLK
#endif

#ifndef S_IFLNK
#define S_IFLNK         __S_IFLNK
#endif

#ifndef S_IFSOCK
#define S_IFSOCK        __S_IFSOCK
#endif

#ifndef S_ISUID
#define S_ISUID         __S_ISUID
#endif

#ifndef S_ISGID
#define S_ISGID         __S_ISGID
#endif

#ifndef S_IRUSR
#define S_IRUSR         __S_IREAD
#endif

#ifndef S_IWUSR
#define S_IWUSR         __S_IWRITE
#endif

#ifndef S_IXUSR
#define S_IXUSR         __S_IEXEC
#endif

#ifndef S_IRWXU
/* Read, write and execute by owner */
#define S_IRWXU         (__S_READ|__S_IWRITE|__S_IEXEC)
#endif

#ifndef S_IRGRP
#define S_IRGRP         (S_IRUSR >> 3)       /* Read by group */
#endif

#ifndef S_IWGRP
#define S_IWGRP         (S_IWUSR >> 3)       /* Write by group */
#endif

#ifndef S_IXGRP
#define S_IXGRP         (S_IXUSR >> 3)       /* Execute by group */
#endif

#ifndef S_IRWXG
/* Read, write and execute by group */
#define S_IRWXG         (S_IRWXU >> 3)
#endif

#ifndef S_IROTH
#define S_IROTH         (S_IRGRP >> 3)       /* Read by others */
#endif

#ifndef S_IWOTH
#define S_IWOTH         (S_IWGRP >> 3)       /* Write by others */
#endif

#ifndef S_IXOTH
#define S_IXOTH         (S_IXGRP >> 3)       /* Execute by others */
#endif

#ifndef S_IRWXO
/* Read, write and execute by others */
#define S_IRWXO         (S_IRWXG >> 3)
#endif

#ifndef __S_ISTYPE
#define __S_ISTYPE(mode, mask)   (((mode) & __S_IFMT) == (mask))
#endif

#ifndef S_ISDIR
#define S_ISDIR(mode)    __S_ISTYPE((mode), __S_IFDIR)
#endif

#ifndef S_ISCHR
#define S_ISCHR(mode)    __S_ISTYPE((mode), __S_IFCHR)
#endif

#ifndef S_ISBLK
#define S_ISBLK(mode)    __S_ISTYPE((mode), __S_IFBLK)
#endif

#ifndef S_ISFIFO
#define S_ISFIFO(mode)    __S_ISTYPE((mode), __S_IFIFO)
#endif

#endif /* STATBITS_H */
