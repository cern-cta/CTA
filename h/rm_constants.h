/* $Id: rm_constants.h,v 1.9 2005/01/25 15:25:25 jdurand Exp $ */

#ifndef __rm_constants_h

#include "osdep.h"
#include "stage_constants.h" /* For STAGE PORT */
#include "Cuuid.h"           /* For CUUID_STRING_LEN */

#ifdef RM_PRTBUFSZ
#undef RM_PRTBUFSZ
#endif
#define RM_PRTBUFSZ ONE_MB

#ifdef RMMAGIC
#undef RMMAGIC
#endif
#define RMMAGIC 0x14140701

#ifdef RM_REPBUFSZ
#undef RM_REPBUFSZ
#endif
#define RM_REPBUFSZ  ONE_MB	/* must be >= max rm reply size */

#ifdef  MSG_OUT
#undef  MSG_OUT
#endif
#define	MSG_OUT		0

#ifdef  MSG_ERR
#undef  MSG_ERR
#endif
#define	MSG_ERR		1

#ifdef RMRC
#undef RMRC
#endif
#define	RMRC 3

#ifdef RMJOB
#undef RMJOB
#endif
#define	RMJOB 4

#ifdef RMNODE
#undef RMNODE
#endif
#define	RMNODE 5

#ifdef RMTIMEOUT
#undef RMTIMEOUT
#endif
#define RMTIMEOUT 5 /* Network timeout */

#ifdef RMCONNECTRETRYINT /* Number of second between each client -> rmmaster connect retry */
#undef RMCONNECTRETRYINT
#endif
#define RMCONNECTRETRYINT 10

#ifdef RMCONNECTMAXRETRY    /* Number of client -> rmmater connect retries */
#undef RMCONNECTMAXRETRY
#endif
#define RMCONNECTMAXRETRY 360

#ifdef RMRETRYINT /* Number of second between each client -> rmmaster dial retry */
#undef RMRETRYINT
#endif
#define RMRETRYINT 10

#ifdef RMMAXRETRY    /* Number of client -> rmmater dial retries */
#undef RMMAXRETRY
#endif
#define RMMAXRETRY 360

#ifdef RMMUTEXTIMEOUT
#undef RMMUTEXTIMEOUT
#endif
#define RMMUTEXTIMEOUT 30 /* General mutex timeout - for a technical reason, has to be > RFIO_CTRL_TIMEOUT (20) */

#ifdef RMDTIMEOUT
#undef RMDTIMEOUT
#endif
#define RMDTIMEOUT 2 /* Network timeout rmmaster <-> user */

#ifdef RM_STAGER_HOST_DEFAULT_FREQUENCY
#undef RM_STAGER_HOST_DEFAULT_FREQUENCY
#endif
#define RM_STAGER_HOST_DEFAULT_FREQUENCY 60

#ifdef RM_STAGER_PORT_DEFAULT_FREQUENCY
#undef RM_STAGER_PORT_DEFAULT_FREQUENCY
#endif
#define RM_STAGER_PORT_DEFAULT_FREQUENCY 60

#ifdef RM_OS_DEFAULT_FREQUENCY
#undef RM_OS_DEFAULT_FREQUENCY
#endif
#define RM_OS_DEFAULT_FREQUENCY 60

#ifdef RM_ARCH_DEFAULT_FREQUENCY
#undef RM_ARCH_DEFAULT_FREQUENCY
#endif
#define RM_ARCH_DEFAULT_FREQUENCY 60

#ifdef RM_RAM_DEFAULT_FREQUENCY
#undef RM_RAM_DEFAULT_FREQUENCY
#endif
#define RM_RAM_DEFAULT_FREQUENCY 10

#ifdef RM_MEM_DEFAULT_FREQUENCY
#undef RM_MEM_DEFAULT_FREQUENCY
#endif
#define RM_MEM_DEFAULT_FREQUENCY 60

#ifdef RM_SWAP_DEFAULT_FREQUENCY
#undef RM_SWAP_DEFAULT_FREQUENCY
#endif
#define RM_SWAP_DEFAULT_FREQUENCY 10

#ifdef RM_PROC_DEFAULT_FREQUENCY
#undef RM_PROC_DEFAULT_FREQUENCY
#endif
#define RM_PROC_DEFAULT_FREQUENCY 60

#ifdef RM_SPACE_DEFAULT_FREQUENCY
#undef RM_SPACE_DEFAULT_FREQUENCY
#endif
#define RM_SPACE_DEFAULT_FREQUENCY 60

#ifdef RM_IORATE_DEFAULT_FREQUENCY
#undef RM_IORATE_DEFAULT_FREQUENCY
#endif
#define RM_IORATE_DEFAULT_FREQUENCY 10

#ifdef RM_LOAD_DEFAULT_FREQUENCY
#undef RM_LOAD_DEFAULT_FREQUENCY
#endif
#define RM_LOAD_DEFAULT_FREQUENCY 10

#ifdef RM_IFCE_DEFAULT_FREQUENCY
#undef RM_IFCE_DEFAULT_FREQUENCY
#endif
#define RM_IFCE_DEFAULT_FREQUENCY 60

#ifdef RM_STATE_DEFAULT_FREQUENCY
#undef RM_STATE_DEFAULT_FREQUENCY
#endif
#define RM_STATE_DEFAULT_FREQUENCY 60

#ifdef RM_FSSTATE_DEFAULT_FREQUENCY
#undef RM_FSSTATE_DEFAULT_FREQUENCY
#endif
#define RM_FSSTATE_DEFAULT_FREQUENCY 5

#ifdef RM_MAXTASK_DEFAULT_FREQUENCY
#undef RM_MAXTASK_DEFAULT_FREQUENCY
#endif
#define RM_MAXTASK_DEFAULT_FREQUENCY 60

#ifdef RM_STATE_DEFAULT
#undef RM_STATE_DEFAULT
#endif
#define RM_STATE_DEFAULT "Auto"

#ifdef RM_FSSTATE_DEFAULT
#undef RM_FSSTATE_DEFAULT
#endif
#define RM_FSSTATE_DEFAULT "Available"

#ifdef RM_MAXTASK_DEFAULT
#undef RM_MAXTASK_DEFAULT
#endif
#define RM_MAXTASK_DEFAULT 0

#ifdef RMMASTER_HOST
#undef RMMASTER_HOST
#endif
#define RMMASTER_HOST "localhost"

#ifdef RMSTAGER_HOST
#undef RMSTAGER_HOST
#endif
#define RMSTAGER_HOST "localhost"

#ifdef RMMASTER_SERVICE
#undef RMMASTER_SERVICE
#endif
#define RMMASTER_SERVICE "rmmaster"

#ifdef RMMASTER_PROTO
#undef RMMASTER_PROTO
#endif
#define RMMASTER_PROTO "udp"

#ifdef RMMASTER_PORT                   /* rmmaster port for rmnode->rmmaster UDP communication */
#undef RMMASTER_PORT
#endif
#define RMMASTER_PORT 15003

#ifdef RMMASTER_WIKI_PORT              /* rmmaster port for maui<->rmmaster TCP communication */
#undef RMMASTER_WIKI_PORT
#endif
#define RMMASTER_WIKI_PORT 15004

#ifdef RMSTAGER_PORT                      /* stager port */
#undef RMSTAGER_PORT
#endif
#define RMSTAGER_PORT STAGE_PORT

#ifdef RMMASTER_USER_PORT                /* rmmaster port for user<->rmmaster TCP communication */
#undef RMMASTER_USER_PORT
#endif
#define RMMASTER_USER_PORT 15007

#ifdef RMNODE_PORT                      /* rmnode port for unicity check */
#undef RMNODE_PORT
#endif
#define RMNODE_PORT 15009

#define CRM_IFCONF_MAX 16 /* Max nb of network interfaces */
#define CRM_IFCONF_MAXNAMELEN 16 /* Max length of an interface */

#define RM_OSNAME  0x0001
#define RM_ARCH    0x0002
#define RM_RAM     0x0004
#define RM_SWAP    0x0008
#define RM_SPACE   0x0010
#define RM_PROC    0x0020
#define RM_IFCE    0x0040
#define RM_LOAD    0x0080
#define RM_MEM     0x0100
#define RM_IORATE  0x0200
#define RM_STAGERHOST  0x0400
#define RM_STAGERPORT  0x0800
#define RM_STATE   0x1000
#define RM_MAXTASK 0x2000
#define RM_FSSTATE 0x4000

#define RM_NODEINFO 1

#define RM_UNKNOWN_MODE -1
#define RM_LSF_MODE 0
#define RM_MAUI_MODE 1

#ifndef CA_MAXDOMAINNAMELEN
#define CA_MAXDOMAINNAMELEN CA_MAXHOSTNAMELEN
#endif

/* Maui checksum */
#define MAX_CKSUM_ITERATION 4
#define CKSUM_SEED "255"

/* Purge frequency (seconds) */
#define RM_JOB_PURGE_FREQUENCY (5*60)

/* Job Id (uuid) length */
#define RM_JOBIDLEN CUUID_STRING_LEN

/* API methods */
#define RM_USERENTERJOB        1
#define RM_USERRUNJOB          2
#define RM_USERMODIFYJOB       3
#define RM_USERDELETEJOB       4
#define RM_USERGETJOBS         5
#define RM_USERGETNODES        6
#define RM_USERKILLJOB         7
#define RM_ADMINNODE           8

/* API flags */
#define RM_ALL                 0x1

#define RM_CHILD_ENTER_MODE    0
#define RM_CHILD_MODIFY_MODE   1
#define RM_CHILD_DELETE_MODE   2
#define RM_CHILD_QUERY_MODE    4

#define RM_O_RDONLY            1
#define RM_O_WRONLY            2
#define RM_O_RDWR              3

#ifndef CA_MAXOSNAMELEN
#define CA_MAXOSNAMELEN 1023
#endif

#ifndef RM_MAXFEATURELEN
#define RM_MAXFEATURELEN 1023
#endif

#ifndef RM_MAXPARTITIONLEN
#define RM_MAXPARTITIONLEN 1023
#endif

#ifndef CA_MAXARCHNAMELEN
#define CA_MAXARCHNAMELEN 1023
#endif

#ifndef CA_MAXSTATUSLEN
#define CA_MAXSTATUSLEN 10
#endif

#ifndef CA_MAXFSSTATUSLEN
#define CA_MAXFSSTATUSLEN 9
#endif

#ifndef RM_MAXUSRNAMELEN
#define RM_MAXUSRNAMELEN 20
#endif

#ifndef RM_MAXGRPNAMELEN
#define RM_MAXGRPNAMELEN 20
#endif

#ifndef RM_MAXACCOUNTNAMELEN
#define RM_MAXACCOUNTNAMELEN 20
#endif

#define LSF_MEM_UNIT ONE_MB
#define LSF_RAM_UNIT ONE_MB
#define LSF_SWAP_UNIT ONE_MB
#define LSF_SPACE_UNIT ONE_MB

/* Number of second until a node is declared in unknown state */
/* if no update at all has been received */
/* Put 0 (digit zero) to disable this feature */
#define NODE_TIMEOUT 300

/* Until "Unknown" stops being transient in moab there are few */
/* candidates for node's state. Draining or Down is one of them */
#define NODE_TIMEOUT_STATE "Down"

/* Number of seconds between jobs survey iteration */
#define JOBS_SURVEY_INTERVAL 30

/* Number of seconds between jobs ping */
#define JOB_PING_INTERVAL 300

/* Number of seconds between nodes survey iteration */
#define NODES_SURVEY_INTERVAL 30

/* Number of thread in the survey pool - this will be the maximum number of concurrent calls to */
/* remote's kill -0. Do not put that value to hight - there is a chance that rmmaster will create */
/* as many remote connection as there is place in that pool */
#define NODES_SURVEY_POOLSIZE 10

#endif /* __rm_constants_h */
