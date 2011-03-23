/*
 * $Id: Cns.h,v 1.20 2009/06/30 15:04:59 waldron Exp $
 */

/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef _CNS_H
#define _CNS_H

                        /* name server constants */

#include "Cns_constants.h"
#include "osdep.h"

#define CNS_SCE                 "CNS"
#define CNS_SVC                 "cns"
#define CNS_SEC_SVC             "cns_sec"
#define CNS_HOME_ENV            "CASTOR_HOME"
#define CNS_HOST_ENV            "CNS_HOST"
#define CNS_PORT_ENV            "CNS_PORT"
#define CNS_SPORT_ENV           "CNS_SEC_PORT"
#define CNS_CONNTIMEOUT_ENV     "CNS_CONNTIMEOUT"
#define CNS_CONRETRY_ENV        "CNS_CONRETRY"
#define CNS_CONRETRYINT_ENV     "CNS_CONRETRYINT"

#define CNS_MAGIC               0x030E1301
#define CNS_MAGIC2              0x030E1302
#define CNS_MAGIC3              0x030E1303
#define CNS_MAGIC4              0x030E1304
#define CNS_MAGIC5              0x030E1305
#define CNS_MAGIC6              0x030E1306
#define CNS_MAGIC7              0x030E1307
#define CNS_DIRTIMEOUT          300    /* timeout while waiting for the next dir sub-req */
#define CNS_TIMEOUT             5      /* netread timeout while receiving a request */
#define CNS_TRANSTIMEOUT        60     /* timeout while waiting for the next req in transaction */
#define MAXRETRY                5
#define RETRYI                  60
#define DIRBUFSZ                4096
#define LISTBUFSZ               4096
#define LOGBUFSZ                4096
#define PRTBUFSZ                180
#define REPBUFSZ                28100  /* must be >= max name server reply size */
#define REQBUFSZ                26854  /* must be >= max name server request size */

                        /* name server request types */

#define CNS_ACCESS               0
#define CNS_CHDIR                1
#define CNS_CHMOD                2
#define CNS_CHOWN                3
#define CNS_CREAT                4
#define CNS_MKDIR                5
#define CNS_RENAME               6
#define CNS_RMDIR                7
#define CNS_STAT                 8
#define CNS_UNLINK               9
#define CNS_OPENDIR             10
#define CNS_READDIR             11
#define CNS_CLOSEDIR            12
#define CNS_OPEN                13 /* deprecated */
#define CNS_CLOSE               14
#define CNS_SETATIME            15
#define CNS_SETFSIZE            16
#define CNS_SHUTDOWN            17 /* deprecated */
#define CNS_GETSEGAT            18
#define CNS_SETSEGAT            19
#define CNS_LISTTAPE            20
#define CNS_ENDLIST             21
#define CNS_GETPATH             22
#define CNS_DELETE              23
#define CNS_UNDELETE            24
#define CNS_CHCLASS             25
#define CNS_DELCLASS            26
#define CNS_ENTCLASS            27
#define CNS_MODCLASS            28
#define CNS_QRYCLASS            29
#define CNS_LISTCLASS           30
#define CNS_DELCOMMENT          31
#define CNS_GETCOMMENT          32
#define CNS_SETCOMMENT          33
#define CNS_UTIME               34
#define CNS_REPLACESEG          35
#define CNS_UPDATESEG_CHECKSUM  36
#define CNS_GETACL              37
#define CNS_SETACL              38
#define CNS_LCHOWN              39
#define CNS_LSTAT               40
#define CNS_READLINK            41
#define CNS_SYMLINK             42
#define CNS_ADDREPLICA          43 /* removed */
#define CNS_DELREPLICA          44 /* removed */
#define CNS_LISTREPLICA         45 /* removed */
#define CNS_STARTTRANS          46
#define CNS_ENDTRANS            47
#define CNS_ABORTTRANS          48
#define CNS_LISTLINKS           49
#define CNS_SETFSIZEG           50
#define CNS_STATG               51
#define CNS_STATR               52 /* removed */
#define CNS_SETPTIME            53 /* removed */
#define CNS_SETRATIME           54 /* removed */
#define CNS_SETRSTATUS          55 /* removed */
#define CNS_ACCESSR             56 /* removed */
#define CNS_LISTREP4GC          57 /* removed */
#define CNS_LISTREPLICAX        58 /* removed */
#define CNS_STARTSESS           59
#define CNS_ENDSESS             60
#define CNS_DU                  61
#define CNS_GETGRPID            62 /* removed */
#define CNS_GETGRPNAM           63 /* removed */
#define CNS_GETIDMAP            64 /* removed */
#define CNS_GETUSRID            65 /* removed */
#define CNS_GETUSRNAM           66 /* removed */
#define CNS_MODGRPMAP           67 /* removed */
#define CNS_MODUSRMAP           68 /* removed */
#define CNS_RMGRPMAP            69 /* removed */
#define CNS_RMUSRMAP            70 /* removed */
#define CNS_GETLINKS            71
#define CNS_GETREPLICA          72 /* removed */
#define CNS_ENTGRPMAP           73 /* removed */
#define CNS_ENTUSRMAP           74 /* removed */
#define CNS_REPLACETAPECOPY     75
#define CNS_LASTFSEQ            76
#define CNS_BULKEXIST           77
#define CNS_TAPESUM             79
#define CNS_PING                82
#define CNS_SETFSIZECS          83
#define CNS_STATCS              84
#define CNS_UPDATEFILE_CHECKSUM 85
#define CNS_UPDATESEG_STATUS    86
#define CNS_DROPSEGS            87
#define CNS_UNLINKBYVID         88
#define CNS_DELSEGBYCOPYNO      89
#define CNS_OPENX               90

                        /* name server reply types */

#define MSG_ERR   1
#define MSG_DATA  2
#define CNS_RC    3
#define CNS_IRC   4
#define MSG_LINKS 5

                        /* name server messages */

#define NS000 "NS000 - name server not available on %s\n"
#define NS002 "NS002 - %s error : %s\n"
#define NS003 "NS003 - illegal function %d\n"
#define NS009 "NS009 - fatal configuration error: %s %s\n"
#endif
