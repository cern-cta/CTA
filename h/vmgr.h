/*
 * $Id: vmgr.h,v 1.1 2005/03/17 10:12:16 obarring Exp $
 */

/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: vmgr.h,v $ $Revision: 1.1 $ $Date: 2005/03/17 10:12:16 $ CERN IT-PDP/DM Jean-Philippe Baud
 */

#ifndef _VMGR_H
#define _VMGR_H

			/* volume manager constants */

#include "vmgr_constants.h"
#include "osdep.h"
#define VMGR_MAGIC	0x766D6701
#define VMGR_MAGIC2	0x766D6702
#define VMGR_LISTTIMEOUT 300	/* timeout while waiting for the next list sub-req */
#define VMGR_TIMEOUT	5	/* netread timeout while receiving a request */
#define	MAXRETRY 5
#define	RETRYI	60
#define LISTBUFSZ 3960
#define LOGBUFSZ 1024
#define PRTBUFSZ  180
#define REPBUFSZ 3964	/* must be >= max volume manager reply size */
#define REQBUFSZ  558	/* must be >= max volume manager request size */

			/* volume manager request types */

#define VMGR_DELTAPE	 0
#define VMGR_ENTTAPE	 1
#define VMGR_GETTAPE	 2
#define VMGR_MODTAPE	 3
#define VMGR_QRYTAPE	 4
#define VMGR_UPDTAPE	 5
#define VMGR_SHUTDOWN	 6
#define VMGR_DELMODEL	 7
#define VMGR_ENTMODEL	 8
#define VMGR_MODMODEL	 9
#define VMGR_QRYMODEL	 10
#define VMGR_DELPOOL	 11
#define VMGR_ENTPOOL	 12
#define VMGR_MODPOOL	 13
#define VMGR_QRYPOOL	 14
#define VMGR_TPMOUNTED	 15
#define VMGR_DELDENMAP	 16
#define VMGR_ENTDENMAP	 17
#define VMGR_LISTDENMAP	 18
#define VMGR_LISTMODEL	 19
#define VMGR_LISTPOOL	 20
#define VMGR_LISTTAPE	 21
#define VMGR_ENDLIST	 22
#define VMGR_RECLAIM	 23
#define VMGR_DELLIBRARY  24
#define VMGR_ENTLIBRARY  25
#define VMGR_LISTLIBRARY 26
#define VMGR_MODLIBRARY  27
#define VMGR_QRYLIBRARY  28
#define VMGR_DELDGNMAP	 29
#define VMGR_ENTDGNMAP	 30
#define VMGR_LISTDGNMAP	 31
#define VMGR_DELTAG	 32
#define VMGR_GETTAG	 33
#define VMGR_SETTAG	 34
#define VMGR_MODWEIGHT   35
#define VMGR_QRYWEIGHT   36
#define VMGR_LISTWEIGHT  37

			/* volume manager reply types */

#define	MSG_ERR		1
#define	MSG_DATA	2
#define	VMGR_RC		3
#define	VMGR_IRC	4

			/* volume manager messages */

#define VMG00	"VMG00 - volume manager not available on %s\n"
#define	VMG02	"VMG02 - %s error : %s\n"
#define VMG03   "VMG03 - illegal function %d\n"
#define VMG04   "VMG04 - error getting request, netread = %d\n"
#define	VMG09	"VMG09 - fatal configuration error: %s %s\n"
#define	VMG23	"VMG23 - %s is not accessible\n"
#define VMG46	"VMG46 - request too large (max. %d)\n"
#if defined(_WIN32)
#define	VMG52	"VMG52 - WSAStartup unsuccessful\n"
#define	VMG53	"VMG53 - you are not registered in the unix group/passwd mapping file\n"
#endif
#define VMG64	"VMG64 - parameter inconsistency with VMGR for vid %s: %s on request <-> %s in VMGR\n"
#define	VMG92	"VMG92 - %s request by %d,%d from %s\n"
#define	VMG98	"VMG98 - %s\n"
#endif
