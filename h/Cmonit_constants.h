/*
 * Copyright (C) 2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* 
 * Header for the CASTOR monitoring module  
 */

#ifndef _MONITORCONST_H_INCLUDED_
#define _MONITORCONST_H_INCLUDED_


/* ---------------------------------------
 * COMMON FOR ALL MONITORING
 * --------------------------------------- */

/*  #define NOMOREMONITOR "/etc/operator/nomoremonitor"  */
/*  #define MONITOR_HOST "pcitds02.cern.ch"  */

#define MONITOR_PORT 15001
#define MONITOR_SERVICE "cmon"
#define CLIENT_REQUEST_LISTEN_PORT 20001
#define CLIENT_REQUEST_SERVICE "cmon_serv"

#define ENV_MONITOR_HOST "MONITOR_HOST"
#define ENV_MONITOR_PORT "MONITOR_PORT"
#define ENV_CLIENT_REQUEST_PORT "MONITOR_CLIENT_PORT"

#define CONF_MONITOR_CATEGORY "MONITOR"
#define CONF_MONITOR_HOST "HOST"
#define CONF_MONITOR_PORT "PORT"
#define CONF_MONITOR_CLIENT_PORT "CLIENT_PORT"

#define CMONIT_MAX_PKT_NB 10

#define CA_MAXUDPPLEN 8000
#define CA_MAXCLIUDPPLEN 4000
#define MAXPARAMLEN 100

#define CMONIT_TAPE_SENDMSG_PERIOD  10 /* In seconds */
/*  #define CMONIT_FTRANSFER_SENDMSG_PERIOD 3 */


#define CMONIT_CLIENT_POOL_SIZE 5  /* Nb of threads in the pool processing client requests */
#define CMONIT_CLIENT_REQUEST_PROCESS_TIMEOUT 1  

/* Period with which to scan the stagers to detect put failed amongst others */
#define CMONIT_STAGER_SCAN_PERIOD 1800  /* In seconds */
/*  #define CMONIT_STAGER_SEND_PERIOD 30 */ /* In seconds */

/* ---------------------------------------
 * CONSTANTS FOR MESSAGES DAEMON -> CMONIT
 * --------------------------------------- */

/* Magic bloc for the Monitoring messages */
#define MREC_MAGIC 76556

/* Values for the different types of monitoring records */
#define MREC_DISK 0
#define MREC_TAPE 1
#define MREC_STAGER 2
#define MREC_RTCOPY 3
#define MREC_FTRANSFER 4

/* Values for the different subtypes for stager messages */
#define STYPE_POOL 1
#define STYPE_MIGRATOR 2

/* ---------------------------------------
 * CONSTANTS FOR MESSAGES CLIENT <-> CMONIT
 * --------------------------------------- */

/* Magic bloc for the Monitoring messages */
#define MCLI_MAGIC "CMONIT1"

/* Values for the different types of monitoring records */
#define MCLI_TAPE_STATUS_REQ  "TAPE_STATUS"
#define MCLI_TAPESERVER_STATUS_REQ  "TAPESERVER_STATUS"
#define MCLI_BAD_REQ  "BAD_REQUEST"
#define MCLI_BAD_PARAM  "BAD_PARAM"
#define MCLI_SHUTDOWN  "SHUTDOWN"
#define MCLI_STAGER_REQ "STAGER_STATUS"
#define MCLI_STAGER_LIST_REQ "STAGER_LIST"
#define MCLI_STAGER_DETAIL_REQ "STAGER_DETAIL"


/*  #define MCLI_STAGER_STATUS_REQ  "STG_STATUS" */
/*  #define MCLI_FS_STATUS_REQ  "FS_STATUS" */

#define START_STAGER "SS"
#define END_STAGER "ES"
#define START_POOL "SP"
#define END_POOL "EP"
#define START_MIGRATOR "SM"
#define END_MIGRATOR "EM"


#endif /*  _MONITORCONST_H_INCLUDED_ */










