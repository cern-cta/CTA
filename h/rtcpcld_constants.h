/*
 * Copyright (C) 2003 by CERN IT-ADC
 * All rights reserved
 */


/*
 * rtcpcld_constants.h,v 1.2 2004/04/20 16:12:56 CERN IT-ADC Olof Barring
 */

/*
 * rtcpcld.h - rtcopy client daemon specific definitions
 */

#ifndef RTCPCLD_CONSTANTS_H
#define RTCPCLD_CONSTANTS_H

#define RTCPCLD_CATPOLL_TIMEOUT (30)   /* Timeout between two polls on the catalogue */
#define RTCPCLD_VDQMPOLL_TIMEOUT (600) /* Timeout between two polls on a VDQM request */
#define RTCPCLD_RECALLER_CMD "recaller"
#define RTCPCLD_MIGRATOR_CMD "migrator"
#define TAPEERRORHANDLER_CMD "TapeErrorHandler"

#ifndef RTCPCLD_NOTIFY_PORT
#define RTCPCLD_NOTIFY_PORT (5050)     /* rtcpclientd notification (UDP) port */
#endif /* RTCPCLD_NOTIFY_PORT */

#ifndef RTCPCLD_NOTIFY_HOST
#define RTCPCLD_NOTIFY_HOST "lxshare003d" /* rtcpclientd host */
#endif /* RTCPCLD_NOTIFY_HOST */


#ifndef RTCPCLD_FACILITY
#define RTCPCLD_FACILITY "rtcpcld"
#endif /* RTCPCLD_FACILITY */

#ifndef MIGRATOR_FACILITY
#define MIGRATOR_FACILITY "migrator"
#endif /* MIGRATOR_FACILITY */

#ifndef RECALLER_FACILITY
#define RECALLER_FACILITY "recaller"
#endif /* RECALLER_FACILITY */

#ifndef MIGHUNTER_FACILITY
#define MIGHUNTER_FACILITY "MigHunter"
#endif /* MIGHUNTER_FACILITY */

#ifndef TAPEERRORHANDLER_FACILITY
#define TAPEERRORHANDLER_FACILITY "TapeErrorHandler"
#endif /* TAPEERRORHANDLER_FACILITY */

#ifndef MIGRATOR_RETRY_POLICY_NAME
#define MIGRATOR_RETRY_POLICY_NAME "migratorRetryPolicy.pl"
#endif /* MIGRATOR_RETRY_POLICY_NAME */

#ifndef RECALLER_RETRY_POLICY_NAME
#define RECALLER_RETRY_POLICY_NAME "recallerRetryPolicy.pl"
#endif /* RECALLER_RETRY_POLICY_NAME */

#define RTCPCLD_NOTIFYTIMEOUT (30)

#define RTCPCLD_GETTAPE_RETRIES (3)
#define RTCPCLD_GETTAPE_RETRY_TIME (60)
#define RTCPCLD_GETTAPE_ENOSPC_RETRY_TIME (3600)

/*
 * The segment copy number is a single digit number in the
 * name server database table. Thus, allowed values are [0,9]
 */
#define VALID_COPYNB(X) (X>=0 && X<10)

#endif /* RTCPCLD_CONSTANTS_H */

