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
#define RTCPCLD_TPERRCHECK_INTERVAL (600) /* Timeout between two checks for failed segments */
#define RTCPCLD_VDQMPOLL_TIMEOUT (600) /* Timeout between two polls on a VDQM request */
#define RTCPCLD_RECALLER_CMD "recaller"
#define RTCPCLD_MIGRATOR_CMD "migrator"
#define TAPEERRORHANDLER_CMD "tperrhandler"

#ifndef RTCPCLD_NOTIFY_PORT
#define RTCPCLD_NOTIFY_PORT (5050)     /* rtcpclientd notification (UDP) port */
#endif /* RTCPCLD_NOTIFY_PORT */

#ifndef RTCPCLD_NOTIFY_HOST
#define RTCPCLD_NOTIFY_HOST "lxshare003d" /* rtcpclientd host */
#endif /* RTCPCLD_NOTIFY_HOST */

#ifndef TAPEERRORHANDLER_FACILITY
#define TAPEERRORHANDLER_FACILITY "tperrhandler"
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

#endif /* RTCPCLD_CONSTANTS_H */

