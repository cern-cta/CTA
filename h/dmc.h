/*
 * $Id: dmc.h,v 1.3 2006/12/14 13:42:18 itglp Exp $
 */

/*
 * Copyright (C) 1997-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */
/* @(#)$RCSfile: dmc.h,v $ $Revision: 1.3 $ $Date: 2006/12/14 13:42:18 $ CERN IT-PDP/DM  Olof Barring */

/*
 * CERN interface to DEC Media Robot Driver (MRD) library.
 * dmc.h - CERN specific definitions for driving Dec SCSI-2 Media Changer (dmc)
 */

/******************************************************************************/
/*                                                                            */
/* Implementation details:                                                    */
/*    - Data structures for inventory are general and therefore complicated.  */
/*      They allow for several media changers to be driven by the same server */
/*      (unlikely to be used). They allow for an easy way to hash over VIDs   */
/*      and hence a two way entry to the inventory: either by cartrige name   */
/*      (*EMPTY for empty elements) or by element address (slot number, etc.).*/
/*      Duplicated VIDs (like *EMPTY) are chained so that no elements are lost*/
/*      in the hashing. A VID is always associated to the same DMCcart_t      */
/*      structure (until it is ejected) - moving a cartrige is done by        */
/*      swapping pointer in the inventory. Thus the history of each VID is    */
/*      maintained (nb. moves, nb. mounts, last move, last mount, home        */
/*      position).                                                            */
/*    - Some macros for crude tracebacks are included to facilitate error     */
/*      tracing. Could be redefined to null when code is stable to reduce the */
/*      log-output.                                                           */
/*    - In addition to the MRD status codes a few internal return codes are   */
/*      defined. They are needed in case a request is inconsistent with the   */
/*      local inventory (NO_SUCH_LOADER, NO_SUCH_DRIVE, DESTINATION_NOT_EMPTY,*/
/*      VOLUME_NOT_FOUND, SOURCE_EMPTY) or to bind several MRD status codes   */
/*      to one retry operation (UPDATE_INVENTORY).                            */
/*    - Sockets are used for client-server communication. listen backlog limit*/
/*      should probably be increased to allow for queued requests when server */
/*      is stalled (e.g. door opened).                                        */
/*    - There are two security issues with the current implementation:        */
/*      - no check on the caller (except that the node name is logged).       */
/*      - server specific requests (DMC_HOLD, DMC_RELEASE and DMC_SHUTDOWN)   */
/*        should probably be protected but no such mechanism is currently     */
/*        implemented.                                                        */
/*                                                                            */
/*    - To improve the operator access in very busy environments, a second    */
/*      is defined for exclusive operator use. This port, which defaults to   */
/*      DMC_OP_PORT, is given higher priority in dmc_wait_client. If dmcserv  */
/*      is called with the -p (port) option the operator port is defined to   */
/*      be the specified port + 1. Although this second port was added after  */
/*      tests in real production and its implementation is a bit clumsy with  */
/*      several code replications, the code was not beautified to use a two   */
/*      entry port array because this would decrease the readability (the     */
/*      opertator port definitions now all end with a _op). The files         */
/*      concerned are dmc.h, dmc_init_network.c and dmc_wait_client.c .       */
/*                                                                            */
/******************************************************************************/

#ifndef _DMC_H
#define _DMC_H
#include <errno.h>
#if defined(DMCSERV) || defined(DMCMGR)
#include <serrno.h>
#include <log.h>
#else
#include "serrno.h"
#include "log.h"
#endif
#include <time.h>

/*
 * Define the geography
 */
#define NB_SILOS 5
#define ROBOT_NAMES "/dev/mc8",""
#define DRIVE_NAMES "dlr03","dlr02","dlr01","dlr13","dlr12","dlr11",\
                    "dlr23","dlr22","dlr21","dlr33","dlr32","dlr31","dlr43","dlr42","dlr41"
#define CLN_SLOTS   {0,1,264,265,528,529,792,793,1056,1057,-1}     /* Protected CLN slots */
#define CLN_PREFIX  "CLN"       /* Prefix of cleaning cartridges */
#define USE_DYNAMIC_INVENTORY 1

/*
 * Names and limits
 */
#ifndef LOGFILE
#ifdef DEBUG
#define LOGFILE "/tmp/testmru.log"
#else
#define LOGFILE "/usr/spool/robot/log"
#endif /*DEBUG */
#endif /*LOGFILE */
#define ACCEPT_RETRY_MAX   5    /* Number of accept() max retries       */
#define ACCEPT_RETRY_DELAY 5    /* Delay between accept() retries       */
#define DMC_NAME "tl820"
#define DMC_PROTO "tcp"
#ifdef SOMAXCONN
#define DMC_BACKLOG SOMAXCONN
#else
#define DMC_BACKLOG 5
#endif
#define DMC_PORT 8888
#define DMC_OP_PORT 8889
#ifndef DEBUG
#define DMC_HOST "shd34"
#endif
#define DMC_ADDRESS_OFFSET 0    /* Address offset for MRU calls (slot 0 == 1 ...etc.) */

#define C_MAGIC 0x10aa5b
#define S_MAGIC 0x10aa5c

#define SORTED -1     /* Extra element_type for DMC_PRTINV requests */

/*
 * Request types
 */

#define DMC_NULL      0         /* Do nothing request */
#define DMC_MOUNT     1         /* Mount request */
#define DMC_UNMOUNT   2         /* Unmount request */
#define DMC_EXPORT    3         /* Export tape request */
#define DMC_IMPORT    4         /* Import tape request */
#define DMC_INVENTORY 5         /* Rebuild local inventory request */
#define DMC_PRTINV    6         /* Print inventory request */
#define DMC_MOVE      7         /* Move VID to specified empty destination */
#define DMC_HOME      8         /* Return VID to its home position */
#define DMC_PARTITION 9         /* Partition individual groups of slots */
#define DMC_PING      96        /* Ping request */
#define DMC_HOLD      97        /* Hold DMC server (accept no more requests except DMC_RELEASE) */
#define DMC_RELEASE   98        /* Release DMC server */
#define DMC_SHUTDOWN  99        /* Shut down DMC server request*/

/*
 * Macros for testing that a numbered element (slot, drive or port) Y is within
 * the limits for the robot numbered X. Example of usage:
 *   if ( VALID_SLOT(robot_nb,slot_nb) ) { ... }
 */
#define VALID_SLOT(X,Y) (inventory != NULL && X>=0 && \
			 X<inventory[0].nb_robots && Y>=0 && Y<inventory[X].nb_slots)
#define VALID_DRIVE(X,Y) (inventory != NULL && X>=0 && \
			  X<inventory[0].nb_robots && Y>=0 && Y<inventory[X].nb_drives)
#define VALID_PORT(X,Y) (inventory != NULL && X>=0 && \
			 X<inventory[0].nb_robots && Y>=0 && Y<inventory[X].nb_ports)

/*
 * Upper and lower case casts
 */

#define UPPERCASE(a) \
{char *_c; \
  for (_c=a; *_c != '\0'; _c++)  *_c=toupper(*_c); \
}

#define LOWERCASE(a) \
{char *_c; \
  for (_c=a; *_c != '\0'; _c++)  *_c=tolower(*_c); \
}

/*
 * Some macros to implement a primitive procedure traceback mechanism.
 * Useful only when one procedure = one source file. Use as follows:
 * UNWIND_START should be called in the end of the declaration section of
 * the procedure (but before the execution entry point). The Return macro 
 * should replace the normal return. STACK_TRACE should be called in the
 * signal catcher or the exit routine (if exit through Die). Call-stack
 * will bail out and STACK_TRACE will cease to work (but not cause any crash) 
 * if stack deepth larger than 20.
 */

#define INIT_UNWIND int _dmc_stack_pointer_ = 0; char *_dmc_stack_[20];
#define UNWIND_START \
  static char _dmc_name_[] = __FILE__; \
  extern int _dmc_stack_pointer_; \
  extern char *_dmc_stack_[20];\
  if ( _dmc_stack_pointer_ < 20 && _dmc_stack_pointer_ >= 0 ) \
  _dmc_stack_[_dmc_stack_pointer_++] = &_dmc_name_[0];
#define UNWIND_END _dmc_stack_pointer_--;
#define STACK_TRACE \
{ int _dmc_sp_; \
  log(LOG_ERR,"Symbolic traceback at %s line %d\n",__FILE__,__LINE__); \
  for (_dmc_sp_ = 0; _dmc_sp_<_dmc_stack_pointer_; _dmc_sp_++) \
  {log(LOG_ERR,"\t%d\t%s\n",_dmc_sp_,_dmc_stack_[_dmc_sp_]);}}
#define Return(A) {UNWIND_END return(A);}

/*
 * Return codes
 */

#define NO_SUCH_LOADER -10
#define NO_SUCH_DRIVE  -11
#define DESTINATION_NOT_EMPTY -12
#define VOLUME_NOT_FOUND -13
#define UPDATE_INVENTORY -14
#define SOURCE_EMPTY -15
#define INVALID_OPERATION -16
#define ROBOT_FULL -17
#define SERVER_HOLD -18
#define COMM_ERROR -19

struct DMCcart {
  char *vid;                      /* VID of cartrige. *EMPTY if element is empty */
  struct DMCcart *next;           /* Next doublet element. Should only happen for *EMPTY */
  int robot_nb;                   /* Inventory index of the robot holding this cartridge */
  int partition_nb;               /* Partition to which this VID belongs */
  int current_type;               /* Current element type (SLOT, DRIVE or PORT) */
  int home_type;                  /* Home element type (SLOT, DRIVE or PORT) */
  int current_position;           /* Current position (slot, drive or port number) */
  int home_position;              /* Home position (slot, drive or port number) */
  int usage;                      /* Total number of operations on this VID */
  int load_usage;                 /* Total number of times this VID has been loaded */
  time_t start_time;              /* Start time for this volume */
  time_t last_used;               /* Last time this VID was operated */
  time_t last_load;               /* Last time this VID was successfully loaded on a drive */
} DMCcart;
typedef struct DMCcart DMCcart_t;

typedef struct {
  int nb_robots;                  /* Total number of robots */
  char *robot_name;               /* Name or this robot, e.g. mc54 */
  int nb_slots;                   /* Number of slots in robot */
  int nb_drives;                  /* Number of drives in robot */
  int nb_ports;                   /* Number of ports in robot */
  int nb_transports;              /* Number of transport elements */
  DMCcart_t **slot;               /* Array of cartrige structures indexed by slot number */
  DMCcart_t **drive;              /* Array of cartrige structures indexed by drive number */
  DMCcart_t **port;               /* Array of cartrige structures indexed by port number */
} DMCinventory_t;

typedef struct {
  int magic;
  int reqtype;
  char host[16];
  int jid;
  char loader[16];                 /* 
				    * Loader name begins with a "d" and
				    * the follows the special file name 
				    * followed by a "," and finally the
				    * drive name, e.g. dmc54,dlr42 for
				    * /dev/mc54 special file for robot
				    * and drive dlr42.
				    */
  char vid[7];                     /* vid requested (if vid addressing is used).*/
  int robot_nb;                    /* Robot number (if element addressing is used). */
  int element_type;                /* Element type (if element addressing is used). E.g. SLOT */
  int element_nb;                  /* Element number (if elemente addressing is used).*/
  short cartridge_side;            /* used if supported by robot */
} DMCrequest_t;

typedef struct {
  int magic;
  int status;
  int log_info_l;
  char *log_info;
#if !defined(__osf__) && !defined(__alpha)
  char *fill[2];
#endif
} DMCreply_t;

typedef struct {
  int s;                         /* client socket                  */
  int s_op;                      /* operator socket                */
  int ns;                        /* to use socket from accept      */
  char *host;                    /* remote host name               */
  struct sockaddr_in *addr;      /* inet socket address of client  */
  int *addrlen;                  /* size of addr                   */
  struct sockaddr_in *addr_op;   /* inet socket address of operator*/
  int *addrlen_op;               /* size of addr_op                */
} DMCnetwork_t;

typedef struct {
  char *afstr;
  char *typestr;
  char *portstr;
} DMCoptions_t;

/*
 * Prototypes
 */
#if defined(__STDC__) 
extern int dmc_build_inventory(DMCinventory_t **);
extern int dmc_destroy_inventory(DMCinventory_t **);
extern int dmc_rehash(DMCinventory_t *);
extern DMCcart_t *dmc_find_vid(DMCinventory_t *, char *);
extern DMCcart_t *dmc_nearest_empty(DMCinventory_t *, char *);
extern int dmc_move_vid(DMCinventory_t *, char *, DMCcart_t *);
extern int dmc_swap_position(DMCinventory_t *,DMCcart_t *, DMCcart_t *);
extern int dmc_mount(DMCinventory_t *, DMCrequest_t *, DMCreply_t *);
extern int dmc_unmount(DMCinventory_t *, DMCrequest_t *, DMCreply_t *);
extern int dmc_wait_client(DMCnetwork_t *);
extern int dmc_init_network(DMCnetwork_t **, DMCoptions_t *);
extern int dmc_init_options(int, char **, DMCoptions_t **);
extern int dmc_init_handlers();
extern int dmc_alrm(int sig);
extern DMCrequest_t *dmc_from_client(DMCnetwork_t *);
extern int dmc_to_client(DMCnetwork_t *,DMCreply_t *);
extern void dmc_shutdown(DMCnetwork_t *);
extern int dmc_print_content(DMCinventory_t *, DMCrequest_t *, DMCreply_t *);
extern int dmc_home(DMCinventory_t *, DMCrequest_t *, DMCreply_t *);
extern int dmc_move(DMCinventory_t *, DMCrequest_t *, DMCreply_t *);
extern int dmc_eject(DMCinventory_t *, DMCrequest_t *, DMCreply_t *);
extern int dmc_partition(DMCinventory_t *, DMCrequest_t *, DMCreply_t *);
extern int dmc_save(DMCrequest_t *);
extern DMCrequest_t *dmc_restore();
extern int dmc_check_port(DMCinventory_t *);
extern int dmc_message(DMCreply_t *, char *, ... );
extern int dmc_rbterr(int, int, int *);
#else /* !defined(__STDC__) */
extern int dmc_build_inventory();
extern int dmc_destroy_inventory();
extern int dmc_rehash();
extern DMCcart_t *dmc_find_vid();
extern DMCcart_t *dmc_nearest_empty();
extern int dmc_move_vid();
extern int dmc_swap_position();
extern int dmc_mount();
extern int dmc_unmount();
extern int dmc_wait_client();
extern int dmc_init_network();
extern int dmc_init_options();
extern int dmc_init_handlers();
extern int dmc_alrm();
extern DMCrequest_t *dmc_from_client();
extern int dmc_to_client();
extern void dmc_shutdown();
extern int dmc_print_content();
extern int dmc_home();
extern int dmc_move();
extern int dmc_eject();
extern int dmc_partition();
extern int dmc_save();
extern DMCrequest_t *dmc_restore();
extern int dmc_check_port();
extern int dmc_message();
extern int dmc_rbterr();
#endif /* __STDC__ */
#endif /* _DMC_H */
