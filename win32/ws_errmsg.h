/*
 * $Id: ws_errmsg.h,v 1.3 1999/12/09 13:47:58 jdurand Exp $
 */

/*
 * @(#)ws_errmsg.h	1.1 12/11/98 CERN IT-PDP/IP Aneta Baran
 */

/*
 * Copyright (C) 1990-1998 by CERN/IT/PDP/IP
 * All rights reserved
 */


#ifndef WS_ERRMSG_H
#define WS_ERRMSG_H
#if defined(_WIN32)
#include <winsock2.h>
#include <stdio.h>

extern char *ws_errmsg[];
char *geterr(void);
int ws_perror (char *s);
char *ws_strerr (int err_no);

#endif	/* WIN32 */
#endif	/* WS_ERRMSG_H */
