/*
** $Id: mtio_add.h,v 1.1 2009/08/06 15:28:04 wiebalck Exp $
*/

/*
** An xroot wrapper for file commands used by rtcpd.
** We need it to be able to use any compilation flags specific only for xroot
*/ 

#ifndef _RTCP_XROOT_H
#define _RTCP_XROOT_H

#include <sys/stat.h>

extern int        rtcp_xroot_stat(const char *, struct stat *);
extern int        rtcp_xroot_open(const char *, int , int);
extern long long  rtcp_xroot_lseek(int, long long , int);
extern int        rtcp_xroot_close(int);
extern long long  rtcp_xroot_write(int, const void *, unsigned long long);
extern long long  rtcp_xroot_read(int, void *, unsigned long long);

#endif
