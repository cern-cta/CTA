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
#define RTCPCLD_VIDWORKER_CMD "VidWorker"

#ifndef RTCPCLD_NOTIFY_PORT
#define RTCPCLD_NOTIFY_PORT (5050)     /* rtcpclientd notification (UDP) port */
#endif /* RTCPCLD_NOTIFY_PORT */

#ifndef RTCPCLD_NOTIFY_HOST
#define RTCPCLD_NOTIFY_HOST "lxshare003d" /* rtcpclientd host */
#endif /* RTCPCLD_NOTIFY_HOST */

#define RTCPCLD_NOTIFYTIMEOUT (30)
#endif /* RTCPCLD_CONSTANTS_H */

