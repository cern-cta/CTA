/*
 * $Id: Cns.h,v 1.3 2006/05/11 12:38:38 felixehm Exp $
 */

/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: Cns.h,v $ $Revision: 1.3 $ $Date: 2006/05/11 12:38:38 $ CERN IT-PDP/DM Jean-Philippe Baud
 */

#ifndef _CNS_H
#define _CNS_H

			/* name server constants */

#include "Cns_constants.h"
#include "osdep.h"
#if defined(NSTYPE_LFC)
#include "lfc.h"
#elif defined(NSTYPE_DPNS)
#include "dpns.h"
#else
#define CNS_SCE "CNS"
#define CNS_SVC "cns"
#define CNS_HOME_ENV "CASTOR_HOME"
#define CNS_HOST_ENV "CNS_HOST"
#define CNS_PORT_ENV "CNS_PORT"
#endif
#define CNS_MAGIC	0x030E1301
#define CNS_MAGIC2	0x030E1302
#define CNS_MAGIC3	0x030E1303
#define CNS_MAGIC4	0x030E1304
#define CNS_DIRTIMEOUT	300	/* timeout while waiting for the next dir sub-req */
#define CNS_TIMEOUT	5	/* netread timeout while receiving a request */
#define CNS_TRANSTIMEOUT	60	/* timeout while waiting for the next req in transaction */
#define	MAXRETRY 5
#define	RETRYI	60
#define DIRBUFSZ 4096
#define LISTBUFSZ 4096
#define LOGBUFSZ 1024
#define PRTBUFSZ  180
#define REPBUFSZ 4100	/* must be >= max name server reply size */
#define REQBUFSZ 2854	/* must be >= max name server request size */

			/* name server request types */

#define CNS_ACCESS	 0
#define CNS_CHDIR	 1
#define CNS_CHMOD	 2
#define CNS_CHOWN	 3
#define CNS_CREAT	 4
#define CNS_MKDIR	 5
#define CNS_RENAME	 6
#define CNS_RMDIR	 7
#define CNS_STAT	 8
#define CNS_UNLINK	 9
#define CNS_OPENDIR	10
#define CNS_READDIR	11
#define CNS_CLOSEDIR	12
#define CNS_OPEN	13
#define CNS_CLOSE	14
#define CNS_SETATIME	15
#define CNS_SETFSIZE	16
#define CNS_SHUTDOWN	17
#define CNS_GETSEGAT	18
#define CNS_SETSEGAT	19
#define CNS_LISTTAPE	20
#define CNS_ENDLIST	21
#define CNS_GETPATH	22
#define CNS_DELETE	23
#define CNS_UNDELETE	24
#define CNS_CHCLASS	25
#define CNS_DELCLASS	26
#define CNS_ENTCLASS	27
#define CNS_MODCLASS	28
#define CNS_QRYCLASS	29
#define CNS_LISTCLASS	30
#define CNS_DELCOMMENT	31
#define CNS_GETCOMMENT	32
#define CNS_SETCOMMENT	33
#define CNS_UTIME	34
#define CNS_REPLACESEG	35
#define CNS_UPDATESEG_CHECKSUM	36
#define CNS_GETACL	37
#define CNS_SETACL	38
#define CNS_LCHOWN	39
#define CNS_LSTAT	40
#define CNS_READLINK	41
#define CNS_SYMLINK	42
#define CNS_ADDREPLICA	43
#define CNS_DELREPLICA	44
#define CNS_LISTREPLICA	45
#define CNS_STARTTRANS	46
#define CNS_ENDTRANS	47
#define CNS_ABORTTRANS	48
#define CNS_LISTLINKS	49
#define CNS_SETFSIZEG	50
#define CNS_STATG	51
#define CNS_STATR	52
#define CNS_SETPTIME	53
#define CNS_SETRATIME	54
#define CNS_SETRSTATUS	55
#define CNS_ACCESSR	56
#define CNS_LISTREP4GC	57
#define CNS_LISTREPLICAX 58
#define CNS_STARTSESS	59
#define CNS_ENDSESS	60
#define CNS_DU		61
#define CNS_GETGRPID	62
#define CNS_GETGRPNAM	63
#define CNS_GETIDMAP	64
#define CNS_GETUSRID	65
#define CNS_GETUSRNAM	66
#define CNS_MODGRPMAP	67
#define CNS_MODUSRMAP	68
#define CNS_RMGRPMAP	69
#define CNS_RMUSRMAP	70
#define CNS_GETLINKS	71
#define CNS_GETREPLICA	72
#define CNS_ENTGRPMAP	73
#define CNS_ENTUSRMAP	74
#define CNS_REPLACETAPECOPY 75

			/* name server reply types */

#define	MSG_ERR		1
#define	MSG_DATA	2
#define	CNS_RC		3
#define	CNS_IRC		4
#define	MSG_LINKS	5
#define	MSG_REPLIC	6
#define	MSG_REPLICP	7

			/* name server messages */

#define NS000	"NS000 - name server not available on %s\n"
#define	NS002	"NS002 - %s error : %s\n"
#define NS003   "NS003 - illegal function %d\n"
#define NS004   "NS004 - error getting request, netread = %d\n"
#define	NS009	"NS009 - fatal configuration error: %s %s\n"
#define	NS023	"NS023 - %s is not accessible\n"
#define NS046	"NS046 - request too large (max. %d)\n"
#if defined(_WIN32)
#define	NS052	"NS052 - WSAStartup unsuccessful\n"
#define	NS053	"NS053 - you are not registered in the unix group/passwd mapping file\n"
#endif
#define	NS092	"NS092 - %s request by %s (%d,%d) from %s\n"
#define	NS098	"NS098 - %s\n"
#endif
