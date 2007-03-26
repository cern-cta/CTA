/*
 *  Copyright (C) 1996-2003 by CERN/IT/PDP/DM
 *  All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: tpusage.c,v $ $Revision: 1.9 $ $Date: 2007/03/26 12:14:53 $ CERN CN-PDP/DM Claire Redmond/Andrew Askew/Olof Barring"; 
#endif /* not lint */

#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#ifdef _WIN32
#include <winsock2.h>
#endif
#include "Cpwd.h"
#if ! (defined(_IBMR2) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux))
#define INIT       register char *sp = instring;
#define GETC()     (*sp++)
#define PEEKC()    (*sp)
#define UNGETC(c)  (--sp)
#define RETURN(c)  return(c)
#endif
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include "Cregexp.h"
#include <time.h>
#include <Cgetopt.h>
#include <osdep.h>
#include <sacct.h>
#include <rfio.h>
#include <net.h>
#include "serrno.h"
#if defined(VDQM)
#include <rtcp_api.h>           /* To get CLIST macro's */
#include <vdqm_api.h>
#endif /* VDQM */

#define MAXDGP 20		/* Maximum number of device groups */
#define MAXSERVS 40		/* Maximum number of servers */
#define MAXVOLLEN 7		/* Current max length of volume lablel */
#define NUMDIVS 60		/* Number of divisions required for graph */


/* Macro to swap byte order */

#define swap_it(a) { int __a; \
	swab((char *)&a, (char *)&__a, sizeof(a)); \
	a = ((unsigned int)__a << 16) | ((unsigned int)__a >> 16); }

/* ************************************************************************** */

#if (!defined(_IBMR2)) && (!defined(hpux)) && (!(defined(__osf__) && defined(__alpha))) || defined(linux)
extern char *loc1, *loc2;	/* Regular expression character position vars */
#endif

int total_mounts[MAXDGP];	/* Total number of mounts made */
int total_requests[MAXDGP];	/* Total number of requests made */
int total_user_requests[MAXDGP];
int total_user_mounts[MAXDGP];
int stdin_flag = 0;		/* Standard input flag */
int server_failed = 0;	        /* Flag set if an open or read has failed */
int max_acctreclen = 0;
int count=0;			/* Counter */ 

static char devgrps[MAXDGP][CA_MAXDGNLEN+1]; /* Array storing device group names */

struct devstats {		/* Structure to store unit information */
  time_t assigned;		/* Time unit assigned */
  time_t confdown;		/* Time unit goes down */
  char dgn[9];			/* Device group name */
  time_t max2mount;		/* Maximum time taken to mount */
  time_t min2mount;		/* Minimum time taken to mount */
  int min_setflag;		/* Set if a minimum time has been allocated */
  int nbdowns;			/* Number of times unit goes down */
  int nbmounted;		/* Number actually mounted */
  int nbmounts;			/* Number of times unit is mounted */
  int nbreqs;			/* Number of times unit is requested */
  time_t time2mount;		/* Time taken to mount */
  long timedown;		/* Total time unit is down */
  long timeused;		/* Total time unit is in use */
  char snm[9];                  /* Server name */
  char drive[9];		/* Unit name */
  struct devstats *next;	/* Pointer to next record */
};
struct devstats *devlist = NULL;

struct dev_usage {		/* Structure storing device usage information */
  char dgn[9];			/* Device group name */
  int last_util[NUMDIVS];	/* Last utilisation per timeslot */
  int max_util[NUMDIVS];	/* Maximum utilisation per timeslot */
  int unit_util[NUMDIVS];	/* Utilisation per timeslot */
  struct dev_usage *next;	/* Pointer to next record */
};
struct dev_usage *dev_use = NULL;

struct grpstats {		/* Structure to store group statistics */
  gid_t gid;			/* Group id */
  int nb[MAXDGP];		/* Number of mounts */
  struct grpstats *next;	/* Pointer to next record */
};
struct grpstats *grp_mounts = NULL;
struct grpstats *grp_requests = NULL;

struct userstats{		/* Structure to store user statistics */
  uid_t uid;			/* ID of the user */
  gid_t gid;			/* Group id */
  int nb[MAXDGP];		/* Number of mounts */
  struct userstats *next;	/* Pointer to next record */
  struct userstats *prev;	/* Pointer to previous record */
};
struct userstats *users_mounts=NULL;
struct userstats *users_requests=NULL;

struct job_ids {		/* Structure to store list of job ids */
  int jid;			/* Job id */
  char server[CA_MAXHOSTNAMELEN+1];/* Server name */
  struct job_ids *previous;	/* Pointer to previous record */
  struct job_ids *next;		/* Pointer to next record */
};
struct job_ids *jobid = NULL;
struct job_ids *lastjd = NULL;

struct numq {			/* Structure storing queue length information */
  char devname[CA_MAXDGNLEN+1];	/* Device group name, eg: CART, 8500 */
  int global_maxq;		/* Max global q length for each device group */
  int lastval[NUMDIVS];		/* Array to store last value per timeslot */
  int maxq;			/* Max q length for specified device group */
  int timeslot[NUMDIVS];	/* Max queue length per timeslot per server */
  int total_Q[NUMDIVS];		/* Total queue length if global request */
  struct numq *next;		/* Pointer to next record */
};
struct numq *first = NULL;
struct numq *new = NULL;
struct numq *current = NULL;

#if defined(VDQM)
typedef struct DrvList {
  vdqmDrvReq_t drv;
  struct DrvList *next;
  struct DrvList *prev;
} DrvList_t;
DrvList_t *drvlist = NULL;
#endif /* VDQM */

struct servlist {		/* Linked list of server names */
  char servname[CA_MAXHOSTNAMELEN+1];/* Server name */
  struct servlist *next;	/* Pointer to next record */
};
static struct servlist *devgrpserv[MAXDGP]; /* Array of pointers to device */
				/* group names */

struct serverqlength {		/* Structure storing the current queue length */
				/* for a device group and server */
  char devname[CA_MAXDGNLEN+1];	/* Device group name, eg: CART, 8500 */
  char servname[CA_MAXHOSTNAMELEN+1];/* Server name */
  int qlength;			/* Current q length for this dev group/server */
  struct serverqlength *next;	/* Pointer to next record */
};
struct serverqlength *fsql = NULL;

struct summary {		/* Structure of summary information on mounts */
  int mounts[MAXDGP];		/* Total number of tapes mounted */
  int norm[MAXDGP];		/* Total number of normal mounts */
  int reselect[MAXDGP];		/* Total mounts for reselect */
  int wrong_ring[MAXDGP];	/* Total mounts for wrong ring */
  int wrong_vsn[MAXDGP];	/* Total mounts for wrong vsn */
};


/*
** Prototypes
*/
int usage( char* );
#if defined(VDQM)
int get_drvlist();
#endif
int create_devicelist( char[][] );
int create_serverlist( char[][], int );

int chk_devgrp( char*, int );
int chk_serv( char*, char[][], int );
int chk_vol( char*, Cregexp_t ** );

int create_reqserverlist( int, char[], char[][], int );
int getacctrec( int, struct accthdr*, char*, int* );
int swap_fields( struct accttape* );
int graph_divs( time_t, time_t, int*, time_t[] );
int enter_data( struct numq*, struct accttape*, time_t[], time_t, time_t, char* );
int ti_vals( struct numq * );

int adddevinfo( struct accttape*, time_t, char* );
int add_gph_info( struct accttape *, time_t, time_t[], char[], time_t );
int add_queue_info( struct accttape*, int, char[], time_t[], time_t, time_t, char* );
int add_suminfo( struct accttape*, int, struct summary* );
int addtpmnt( struct accttape*, int ); 

int print_devusage( time_t, time_t, time_t[], int );
int print_mountdetails( time_t, time_t, int, char, char[] );
int print_tpentry( struct accttape*, time_t, char[], int, Cregexp_t*, char* );
int print_queue( time_t, time_t, struct numq*, time_t[], char[], char[], int );
int printsorted_list( struct userstats*, int );
int print_time_interval( time_t, time_t );

int print_summary( time_t, time_t, int, struct summary*, char[] );
int print_time_details( time_t, time_t );

int set_util_timeslots( struct dev_usage* );
int set_q_timeslots( struct numq* );


/* ************************************************************************** */
void set_max_acctreclen() {
  if ( sizeof(struct acctrfio) > max_acctreclen ) 
      max_acctreclen = sizeof(struct acctrfio);
  if ( sizeof(struct accttape) > max_acctreclen ) 
      max_acctreclen = sizeof(struct accttape);
  if ( sizeof(struct acctrtcp) > max_acctreclen ) 
      max_acctreclen = sizeof(struct acctrtcp);
  if ( sizeof(struct acctrtcp_timing) > max_acctreclen )
      max_acctreclen = sizeof(struct acctrtcp_timing);
  if ( sizeof(struct acctstage) > max_acctreclen ) 
      max_acctreclen = sizeof(struct acctstage);
}
 
int main(argc, argv)
int argc;
char **argv;
{
  char acctdir[80];		/* Accounting directory name and path */
  char acctfile[80];		/* Accounting file name and path */
  char acctfiles[MAXSERVS][80];	/* Accounting file names and paths */
  char acctfile2[80];		/* Input accounting file */
  char acctweek[80];		/* Week number - optional */
  struct accthdr accthdr;	/* An accthdr record */
  struct accthdr accthdrs[MAXSERVS]; /* accthdr records */
  char buf[1204];		/* Input buffer */
  char bufs[MAXSERVS][1204];	/* Input buffers */
  int c = 0;			/* Temp command line switch variable */
  time_t cvt_datime();		/* Date function */
  char devgroup[CA_MAXDGNLEN+1];/* Device group name */
  int dev_found = 0;		/* Flag to indicate if given device is valid */
  int Dflag = 0;		/* If set device utilisation requested */
  int div = 0;			/* Size of time intervals for graphs */
  time_t divisions[NUMDIVS];	/* Actual time divisions for graphs */
  int eflag = 0;		/* If set endtime given */
  time_t endtime = 0;		/* Time to end reading records */
  int errflg = 0;		/* Error flag */
  int fd_acct = 0;		/* File pointer */
  int fd_accts[MAXSERVS];	/* File pointers */
  int finished = 0;		/* Used as boolean block end condition */
  int finished1 = 0;		/* Used as boolean block end condition */
  int finished2 = 0;		/* Used as boolean block end condition */
  int gflag = 0;		/* If set device group given */
  int i = 0;			/* Counter */
  int Lflag = 0;		/* Set for detailed list of mounted volumes */
  static struct Coptions longopts[] = {
	{"acctdir", REQUIRED_ARGUMENT, 0, 'A'},
	{0, 0, 0, 0}
  };
  int Mflag = 0;		/* If set mount request details requested */
  int mflag = 0;		/* If set successful mount details requested */
  int nrec = 0;			/* Number of records read */
  int num_devs = 0;		/* Total number of valid device groups */
  int numservs = 0;		/* Counter for number of servers to be read */
  int num_serv_errors = 0;	/* Number of server failures */
  int Qflag = 0;		/* If set queue length info requested */
  struct accttape *rp = NULL;	/* Pointer to accttape record */
  char servers[MAXSERVS][9];	/* Full server list */
  char server_errors[MAXSERVS][9]; /* List of servers on which open failed */
  int servers_failed[MAXSERVS];	/* Flag set if an open has failed */
  char serv_name[CA_MAXHOSTNAMELEN+1];/* Server name */
  int Sflag = 0;		/* If set server is specified */
  int sflag = 0;		/* If set start time given */
  int smallest_time_server = 0;	/* Smallest current accthdr timestamp server */
				/* index */
  int smallest_time = 0;	/* Smallest current accthdr timestamp */
  time_t starttime = 0;		/* Time to start reading records from */
  struct summary *sumstats = NULL; /* Summary structure */
  int swapped = 0;		/* If set to 1 then byte order swapped */
  int swappeds[MAXSERVS];	/* If set to 1 then byte order swapped */
  int Tflag = 0;		/* Time to mount request */
  char reqserv_list[MAXSERVS][9]; /* List of servers to be accessed */
  int uflag = 0;		/* Informations on the most active user wanted*/
  int Vflag = 0;		/* Set if list of specific volume requested */
  int vflag = 0;		/* Open and processing messages wanted */
  Cregexp_t *expstruct = NULL;		/* Regular expression structure */
  int wflag = 0;		/* If set week number given */
  int Yflag = 0;		/* Summary information requested */

/* Set acctfile = NULL and create list of valid device groups */

  acctdir[0] = '\0';
  acctfile[0] = '\0';
  acctfile2[0] = '\0';
  acctweek[0] = '\0';
  buf[0] = '\0';
  set_max_acctreclen();

  devgroup[0] = '\0';		/* Make sure that devgroup is set null so */
				/* that we know if it is used or not */
  serv_name[0] = '\0';		/* Make sure that devgroup is set null so */
				/* that we know if it is used or not */
#if defined(VDQM)
  if ( get_drvlist() == -1 ) {
      fprintf(stderr,"Failed to get drive list from VDQM\n");
      exit(1);
  }
#endif /* VDQM */

  num_devs = create_devicelist(devgrps);

/* Read in command line arguments and set flags accordingly */

  if ( argc > 1 ) printf("Run specifications: ");
  Copterr = 1;
  Coptind = 1;
  while ((c = Cgetopt_long(argc, argv, "De:f:g:LMmQS:s:TuV:vw:Y", longopts, NULL)) != EOF) {
    switch (c) {
      case 'A':			/* Accounting directory */
                 strcpy(acctdir, Coptarg);
                 break;
      case 'D':			/* Device Utilization */
                 Dflag++;
                 printf("Device utilisation");
                 break;
      case 'e':			/* End time */
                 if ((endtime = cvt_datime(Coptarg)) < 0) {
                   fprintf(stderr, "Incorrect time value %s\n", Coptarg);
                   errflg++;
                 }
                 else {
                   eflag++;
                   printf("End time=%s", Coptarg);
                 }
                 break;
      case 'f':			/* Accounting file */
                 strcpy(acctfile2, Coptarg);
                 if (strcmp(acctfile, "stdin") == 0) {
                   stdin_flag++;
                   printf(" Acct file=stdin");
                 }
                 else {
                   printf("Acct file=%s", Coptarg);
                 }
                 break;
      case 'g':			/* Device group */
                 if ((dev_found = chk_devgrp(Coptarg, num_devs)) == 0) {
                   fprintf(stderr, "Invalid device group %s\n", Coptarg);
                   errflg++;
                 }
                 else {
                   strcpy(devgroup, Coptarg);
                   gflag++;
                   printf("Devgroup=%s", Coptarg);
                 }
                 break;
      case 'L':			/* List mounted volumes */
                 Lflag++;
                 printf("List of volumes");
                 break;
      case 'M':			/* Number of mount requests made */
                 Mflag++;
                 printf("Mount statistics");
                 break;
      case 'm':			/* Number of successful mounts made */
                 mflag++;
                 printf("Successful mount statistics");
                 break;
      case 'Q':			/* Number of requests in the queue */
                 Qflag++;
                 printf("Queue lengths");
                 break;
      case 'S':			/* Server name */
                 numservs = create_serverlist(servers, num_devs);
                 if ((chk_serv(Coptarg, servers, numservs)) == 0) {
                   fprintf(stderr, "Invalid server name %s\n", Coptarg);
                   errflg++;
                 }
                 else {
                   strcpy(serv_name, Coptarg);
                   Sflag++;
                   printf("Tape server=%s", Coptarg);
                 }
                 break;
      case 's':			/* Start time */
                 if ((starttime = cvt_datime(Coptarg)) < 0) {
                   fprintf(stderr, "Incorrect time value %s\n", Coptarg);
                   errflg++;
                 }
                 else {
                   sflag++;
                   printf("Start time=%s", Coptarg);
                 }
                 break;
      case 'T':			/* General time information */
                 Tflag++;
                 printf("Device statistics");
                 break;
      case 'u':			/* Details of the most active user for each tape */      
                 uflag++;
                 printf("User statistics");
                 break;		 
      case 'V':			/* List of specific volume details */
                 if ((chk_vol(Coptarg, &expstruct)) == 0)
                   errflg++;
                 else {
                   Vflag++;
                   printf("Volumes search pattern=%s", Coptarg);
                 }
                 break;
      case 'v':			/* Processing status messages requested */
                 vflag++;
                 printf("Processing status messages");
                 break;
      case 'w':			/* Week number */
                 strcpy(acctweek, Coptarg);
                 wflag++;
                 printf("Week=%s", Coptarg);
                 break;
      case 'Y':			/* Summary information */
                 Yflag++;
                 printf("Summary information");
                 break;
      case '?':
                 errflg++;
    }
    if ( Coptind < argc ) printf(", ");
  }
  if (argc > Coptind) {
    fprintf(stderr, "Additional parameters given\n");
    errflg++;
  }


/* Make sure flags do not conflict or are incompatible */

  if (Lflag && Vflag) {
    Lflag = 0;
  }

  if (Qflag && stdin_flag) {
    fprintf(stderr, "-Q option cannot be run with stdin input\n");
    errflg++;
  }


/* If no options are given, default a Y flag */

  if (!Dflag && !Lflag && !Mflag && !mflag && !Qflag && !Tflag && !uflag && !Vflag && !Yflag) {
    Yflag++;
    printf(" Summary information\n");
  } else printf("\n");



/* If the error flag has been set, quit after usage */

  if (errflg) {
    usage(argv[0]);
    if (expstruct != NULL) free(expstruct);
    exit(1);
  }


/* Finish input comments with a new line and display version information if */
/* processing of status messages was requested. */

  printf("\n");
  if (vflag) {
    fprintf(stderr, "Version information:\n\n %s\n\n", sccsid);
  }


/* If a server has been specified copy server name and accounting file path */
/* into acctfile if a server not specified, build up list of all servers */

  if (Sflag) {
    strcpy(reqserv_list[0], serv_name);
    numservs = 1;
  }
  else if (stdin_flag) {
    numservs = 1;
  }
  else {
    numservs = create_reqserverlist(dev_found, devgroup, reqserv_list,
                                    num_devs);
  }


/* If summary information has been asked for then allocate memory for a */
/* sumstats record */

  if (Yflag) {
    if (sumstats == NULL) {
      sumstats = (struct summary *)calloc(1, sizeof(struct summary));
    }
  }


/* If the start and end times have not been specified, read all files, and */
/* set them to the earliest and latest times of relevant records */
/* respectively. If mount details have been requested then collate relevant */
/* information on this pass. If an open fails then store the server name and */
/* continue. */

  if (!starttime || !endtime || Mflag || mflag || Yflag || Tflag || Lflag
                             || uflag || Vflag || (Dflag && stdin_flag)) {
    for (i = 0; i < numservs; i++) {
      server_failed = 0;
      if (!stdin_flag) {
        if (! *acctdir) {
          strcpy(acctfile, reqserv_list[i]);
          strcat(acctfile, ":");
          if (acctfile2[0] != '\0') {
            strcat(acctfile, acctfile2);
          }
          else {
            strcat(acctfile, ACCTFILE);
          }
        } else {
          strcpy(acctfile, acctdir);
          strcat(acctfile, "/");
          strcat(acctfile, reqserv_list[i]);
          strcat(acctfile, "/sacct");
        }
        if (wflag) {
          strcat(acctfile, ".");
          strcat(acctfile, acctweek);
        }
        if (vflag) {
          fprintf(stderr, "Opening file: %s\n", acctfile);
        }
        if ((fd_acct = rfio_open(acctfile, O_RDONLY)) < 0) {
          fprintf(stderr, "%s: open error: %s\n", acctfile, rfio_serror());
          server_failed = 1;
          if (Mflag || mflag || Yflag || Tflag || uflag || Lflag || Vflag) {
            strcpy (server_errors[num_serv_errors], reqserv_list[i]);
            num_serv_errors++;
          }
        }
      }

      if (vflag) {
        if (stdin_flag) {
          fprintf(stderr, "Processing <stdin>\n");
        }
        else {
          fprintf(stderr, "Processing file: %s\n", acctfile);
        }
      }

      nrec = 0;
      swapped = 0;
      if (!server_failed) {
        while (getacctrec(fd_acct, &accthdr, buf, &swapped)) {
          if (accthdr.package != ACCTTAPE) {
            continue;
          }
          nrec++;
          if (!starttime && nrec == 1) {
            starttime = accthdr.timestamp;
          }
          if (!endtime && nrec == 1) {
            endtime = accthdr.timestamp;
          }
          if (accthdr.timestamp < starttime) { 
            if (!sflag) {
              starttime = accthdr.timestamp;
            }
            else {
              continue;
            }
          }
          if (accthdr.timestamp > endtime) { 
            if (!eflag) {
              endtime = accthdr.timestamp;
            }
            else {
              break;
            }
          }
          rp = (struct accttape *)buf;
          if (((Dflag && stdin_flag) || Tflag || Mflag || mflag || Yflag
                                     || uflag || Vflag || Lflag) && swapped) {
            swap_fields(rp);
            swapped = 0;
          }

          if ((Mflag || uflag) && rp->subtype == TP2MOUNT) {
            if (addtpmnt(rp, num_devs)) {
              errflg++;
            }
          }
          if (mflag && rp->subtype == TPMOUNTED) {
            if (addtpmnt(rp, num_devs)) {
              errflg++;
            }
          }
          if (((Dflag && stdin_flag) || Tflag ) &&
              (rp->subtype == TPASSIGN || rp->subtype == TPFREE ||
               rp->subtype == TPCONFUP || rp->subtype == TPCONFDN ||
               rp->subtype == TPMOUNTED || rp->subtype == TP2MOUNT)) {
            if (dev_found) { 
              if (strcmp(rp->dgn, devgroup) == 0) {
                adddevinfo(rp, accthdr.timestamp,reqserv_list[i]);
              }
            }
            else {
              adddevinfo(rp, accthdr.timestamp,reqserv_list[i]);
            }
          }
          if (Yflag && (rp->subtype == TP2MOUNT || rp->subtype == TPMOUNTED)) {
            if (add_suminfo(rp, num_devs, sumstats)) {
              errflg++;
            }
          }
          if ((Lflag || Vflag) && (rp->subtype == TPMOUNTED)) {
            print_tpentry(rp, accthdr.timestamp, devgroup, Lflag, expstruct,reqserv_list[i]);
          }
        }
	if ( server_failed ) {
	  fprintf(stderr,"Read error on account file %s\n",acctfile);
	  server_failed = 0;
	}
        if (!stdin_flag) {
          rfio_close(fd_acct);
        }
      }
    }
  }


/* For graph options, determine the time divisions for the graph intervals, */
/* then for each server reread all the files and store the relevant */
/* information. */

  if ((Qflag || Dflag) && !stdin_flag) {
    graph_divs(starttime, endtime, &div, divisions);
    for (i = 0; i < numservs; i++) {
      servers_failed[i] = 0;
      strcpy(acctfiles[i], reqserv_list[i]);
      strcat(acctfiles[i], ":");
      if (acctfile2[0] != '\0') {
        strcat(acctfiles[i], acctfile2);
      }
      else {
        strcat(acctfiles[i], ACCTFILE);
      }
      if (wflag) {
        strcat(acctfiles[i], ".");
        strcat(acctfiles[i], acctweek);
      }

      if (vflag) {
        fprintf(stderr, "Opening file: %s\n", acctfiles[i]);
      }

      if ((fd_accts[i] = rfio_open(acctfiles[i], O_RDONLY)) < 0) {
        fprintf(stderr, "%s: open error: %s\n", acctfiles[i], rfio_serror());
        strcpy(server_errors[num_serv_errors], reqserv_list[i]);
        num_serv_errors++;
        servers_failed[i] = 1;
      }

      swappeds[i] = 0;

      if (!servers_failed[i]) {
        if (vflag) {
          fprintf(stderr, "Processing file:%s\n", acctfiles[i]);
        }
        finished = 0;
        while (!finished) {
          if ((getacctrec(fd_accts[i], &accthdrs[i], bufs[i], &swappeds[i])) == 0) {
            rfio_close(fd_accts[i]); 
            servers_failed[i] = 1;
            finished = 1;
          }
          else {
            if (accthdrs[i].timestamp > endtime) {
              rfio_close(fd_accts[i]); 
              servers_failed[i] = 1;
              finished = 1;
            }
            if ((accthdrs[i].package == ACCTTAPE) &&
                (accthdrs[i].timestamp >= starttime)) {
              finished = 1;
            }
          }
        }
      }
    }
    finished1 = 0;
    while (!finished1) {
      smallest_time_server = -1;
      smallest_time = endtime + 1;
      for (i = 0; i < numservs; i++) {
        if (servers_failed[i] == 0) {
          if (accthdrs[i].timestamp < smallest_time) {
            smallest_time_server = i;
            smallest_time = accthdrs[i].timestamp;
          }
        }
      }
      if (smallest_time_server == -1) {
        finished1 = 1;
      }
      else {
        rp = (struct accttape *)bufs[smallest_time_server];
       	if (swappeds[smallest_time_server]) {
          swap_fields(rp);
          swappeds[smallest_time_server] = 0;
        }
        if (Dflag && (rp->subtype == TPASSIGN || rp->subtype == TPFREE ||
                      rp->subtype == TPCONFUP || rp->subtype == TPCONFDN)) {
          if (dev_found) {
            if (strcmp(rp->dgn, devgroup) == 0) {
              if (!Tflag) {
                adddevinfo(rp, accthdrs[smallest_time_server].timestamp,reqserv_list[smallest_time_server]);
              }
              if (rp->subtype == TPASSIGN || rp->subtype == TPFREE) {
                add_gph_info(rp, accthdrs[smallest_time_server].timestamp,
                             divisions, reqserv_list[smallest_time_server],
                             endtime);
              }
            }
          }
          else {
            if (!Tflag) {
              adddevinfo(rp, accthdrs[smallest_time_server].timestamp,reqserv_list[smallest_time_server]);
            }
            if (rp->subtype == TPASSIGN || rp->subtype == TPFREE) {
              add_gph_info(rp, accthdrs[smallest_time_server].timestamp,
                           divisions, reqserv_list[smallest_time_server],
                           endtime);
            }
          }
        }
        if (Qflag) {
          add_queue_info(rp, dev_found, devgroup, divisions,
                         accthdrs[smallest_time_server].timestamp, endtime,
                         reqserv_list[smallest_time_server]);
        }
        finished2 = 0;
        while (!finished2) {
          if ((getacctrec(fd_accts[smallest_time_server],
               &accthdrs[smallest_time_server], bufs[smallest_time_server],
               &swappeds[smallest_time_server])) == 0) {
            rfio_close(fd_accts[smallest_time_server]);
            servers_failed[smallest_time_server] = 1;
            finished2 = 1;
          }
          else {
            if (accthdrs[smallest_time_server].timestamp > endtime) {
              rfio_close(fd_accts[smallest_time_server]);
              servers_failed[smallest_time_server] = 1;
              finished2 = 1;
            }
            if ((accthdrs[smallest_time_server].package == ACCTTAPE) &&
                (accthdrs[smallest_time_server].timestamp >= starttime)) {
              finished2 = 1;
            }
          }
        }
      }
    }
  }

  if (vflag) {
    fprintf(stderr, "Input processing complete\n");
  }


/* Output the results */

  if (Dflag) {
    print_devusage(starttime, endtime, divisions, stdin_flag);
  }
  if (Mflag) {
    print_mountdetails(starttime, endtime, num_devs, 'M', devgroup);
  }
  if (mflag) {
    print_mountdetails(starttime, endtime, num_devs, 'm', devgroup);
  }
  if (Qflag) {
    print_queue(starttime, endtime, first, divisions, devgroup, serv_name, num_devs);
  }
  if (Tflag) {
    print_time_details(starttime, endtime);
  }
  if (uflag) {
    print_mountdetails(starttime, endtime, num_devs, 'u', devgroup);
  }
  if (Yflag) {
    print_summary(starttime, endtime, num_devs, sumstats, devgroup);
  }
  if (num_serv_errors > 0 ) {
    printf("\n\n\n");
    for (i = 0; i < num_serv_errors; i++) {
      printf("Server %s failed, no information read\n", server_errors[i]);
    }
  }
  if (expstruct != NULL) free(expstruct);
  if (errflg) {
    exit (2);
  }
  else {
    exit (0);
  }
}

/* ************************************************************************** */


/* Function to store device usage and statistics information */

int adddevinfo(rp, timestamp,servname)
struct accttape *rp;
time_t timestamp;
char *servname;
{
  int found_dgn = 0;		/* Flag set if device group found */
  int found_drive = 0;		/* Flag set if drive found */
  struct devstats *dp = NULL;	/* Pointer to record of type devstats */
  struct devstats *prev = NULL;	/* Pointer to record of type devstats */
  struct dev_usage *du = NULL;	/* Pointer to record of type dev_usage */
  struct dev_usage *duprev = NULL; /* Pointer to record of type dev_usage */
 

/* Start at begining of dev_usage list and search for matching record. If not */
/* found create a new record, set all the time slots to -1 (indicates no */
/* entry for timeslot) and input the relevant details. */

  du = dev_use;
  while (du) {
    if (strcmp(rp->dgn, du->dgn) == 0) {
      found_dgn = 1;
      break;
    }
    duprev = du;
    du = du->next;
  }
  if (!found_dgn) {
    du = (struct dev_usage *)calloc(1, sizeof(struct dev_usage));
    set_util_timeslots(du);
    if (dev_use == NULL) {
      dev_use = du;
    }
    else {
      duprev->next = du;
    }
    strcpy(du->dgn, rp->dgn);
  }


/* Start at beginning of devstats list and search for matching record. If not */
/* found create a new record and input the relevant details. */
 
  dp = devlist;
  while (dp) {
    if (strcmp (rp->drive, dp->drive) == 0) {
      found_drive = 1;
      break;
    }
    prev = dp;
    dp = dp->next;
  }

  if (!found_drive) {
    dp = (struct devstats *)calloc (1, sizeof(struct devstats));
    if (devlist == NULL) {
      devlist = dp;
    }
    else {
      prev->next = dp;
    }
    strcpy(dp->drive, rp->drive);
    strcpy(dp->dgn, rp->dgn);
    strcpy(dp->snm,servname);
  }

  if (rp->subtype == TPASSIGN) {
    dp->assigned = timestamp;
    dp->nbmounts++;
  }
  else if (rp->subtype == TPFREE) {
    if (dp->assigned) {
      dp->timeused += (timestamp - dp->assigned);
    }
    dp->assigned = 0;
  } 
  else if (rp->subtype == TPCONFUP) {
    if (dp->confdown) {
      dp->timedown += (timestamp - dp->confdown);
    }
    dp->confdown = 0;
  } 
  else if (rp->subtype == TPCONFDN) {
    dp->nbdowns++;
    dp->confdown = timestamp;
  }
  else if (rp->subtype == TP2MOUNT) {
    dp->nbreqs++;
  }
  else if (rp->subtype == TPMOUNTED) {
    if (dp->assigned > 0) {
      dp->time2mount += (timestamp - dp->assigned);
      if ((timestamp - dp->assigned) > dp->max2mount) {
        dp->max2mount = (timestamp - dp->assigned);
      }
      if (!dp->min_setflag) {
        dp->min2mount = (timestamp - dp->assigned);
        dp->min_setflag++;
      }
      else if ((timestamp - dp->assigned) < dp->min2mount) {
        dp->min2mount = (timestamp - dp->assigned);
      }
      dp->nbmounted++;
    }
  }
  return(0);
}

/* ************************************************************************** */


/* Function to add graph information for device usage */

int add_gph_info(rp, timestamp, divisions, server_name, endtime)
time_t timestamp, endtime, divisions[];
struct accttape *rp;
char server_name[CA_MAXHOSTNAMELEN+1];
{
  struct dev_usage *dptr = NULL;	/* Pointer to dev_usage record */
  int i = 0;				/* Counter */
  int current_timeslot = 0;		/* Timeslot of current record */
  struct job_ids *jd = NULL;		/* Pointer to job_id record */
  struct job_ids *jd_prev = NULL;
  struct job_ids *jd_next = NULL;


/* Search divisions array for the timeslot of the current record */

  for (i = 0; i < NUMDIVS; i++) {
    if ((timestamp / 60) >= divisions[i] && (timestamp / 60) < divisions[i + 1]) {
      current_timeslot = i;
      break;
    }
    else if (i == (NUMDIVS - 1) && (timestamp / 60) >= divisions[i] &&
             (timestamp / 60) <= endtime) {
      current_timeslot = i;
      break;
    }
  }


/* Set dev_usage pointer (dptr) to the record matching the current accttape */
/* record */

  dptr = dev_use;
  while (dptr) {
    if (strcmp(rp->dgn, dptr->dgn) != 0) {
      dptr = dptr->next;
    }
    else {
      break;
    }
  }


/* If no entry in the unit utilisation table for the current timeslot set the */
/* 3 timeslot values */

  if (current_timeslot != 0 && dptr->unit_util[current_timeslot] == -1) {
    for (i = (current_timeslot - 1); i > -1; i--) {
      if (dptr->last_util[i] > -1) {
        dptr->unit_util[current_timeslot] = dptr->last_util[i];
        i = -1;
      }
    }
    if (dptr->unit_util[current_timeslot] == -1) {
      dptr->unit_util[current_timeslot] = 0;
    }
    dptr->last_util[current_timeslot] = dptr->unit_util[current_timeslot];
    dptr->max_util[current_timeslot] = dptr->unit_util[current_timeslot];
  }


/* If TPASSIGN; create a job_id record and increment unit_util for current */
/* timeslot */
/* If TPFREE; match to a job_id record and decrement unit_util for current */
/* timeslot */
/* If TPFREE and no match found; ignore the record */

  jd = jobid;
  if (rp->subtype == TPASSIGN) {
    if (jd == NULL) {
      jd = (struct job_ids *)calloc(1, sizeof(struct job_ids));
      jd->previous = NULL;
      jd->jid = rp->jid;
      strcpy(jd->server, server_name);
      jd->next = NULL;
      jobid = lastjd = jd;
    }
    else {
      jd = (struct job_ids *)calloc(1, sizeof(struct job_ids));
      jd->previous = lastjd;
      jd->jid = rp->jid;
      strcpy(jd->server, server_name);
      jd->next = NULL;
      lastjd->next = jd;
      lastjd = jd;
    }

    if (dptr->unit_util[current_timeslot] == -1) {
      dptr->unit_util[current_timeslot] = 1;
    }
    else {
      dptr->unit_util[current_timeslot] = dptr->unit_util[current_timeslot] + 1;
    }
  }

  else if (rp->subtype == TPFREE) {
    while (jd) {
      if (strcmp(jd->server, server_name) == 0) {
        if (jd->jid == rp->jid) {
          if (current_timeslot == 0 &&
              dptr->unit_util[current_timeslot] == -1) {
            dptr->unit_util[current_timeslot] = 0;
          }
          else if (dptr->unit_util[current_timeslot] > 0) {
            dptr->unit_util[current_timeslot] =
              dptr->unit_util[current_timeslot] - 1;
          }

          if (jd->previous == NULL) {
            jobid = jd->next;
          }
          else { 
            jd_prev = jd->previous;
            jd_prev->next = jd->next;
          }
          if (jd->next == NULL) {
            lastjd = jd->previous;
          }
          else {
            jd_next = jd->next;
            jd_next->previous = jd->previous;
          }
          free (jd);
          break;
        }
      }
      jd = jd->next;
    }
  }


/* Set the last_util for timeslot to the current unit_util and if unit util */
/* is greater than the max_utilisation for the timeslot then set the latter */
/* to equal it */

  dptr->last_util[current_timeslot] = dptr->unit_util[current_timeslot];
  if (dptr->unit_util[current_timeslot] > dptr->max_util[current_timeslot]) {
    dptr->max_util[current_timeslot] = dptr->unit_util[current_timeslot];
  }
  return(0);
}

/* ************************************************************************** */


/* Function to get the queue information */
 
int add_queue_info(rp, devfound, devgroup, divisions, timestamp, endtime, servername)
struct accttape *rp;
int devfound;
char devgroup[];
time_t divisions[];
time_t timestamp;
time_t endtime;
char *servername;
{
  int found = 0;	/* Flag to indicate if relevant record is found */


/* If a device has been specified enter the relevant data */

  if (devfound && rp->subtype == TPDGQ && (strcmp(devgroup, rp->dgn)) == 0) {
    if (first == NULL) {
      first = (struct numq *)calloc(1, sizeof(struct numq));
      set_q_timeslots(first);
      first->next = NULL;
    }
    enter_data(first, rp, divisions, timestamp, endtime, servername);
  }


/* If device is not specified create records for each device found in the */
/* input file */

  if (!devfound && rp->subtype == TPDGQ) {
    current = first;
    if (first == NULL) {
      first = (struct numq *)calloc(1, sizeof(struct numq));
      set_q_timeslots(first);
      enter_data(first, rp, divisions, timestamp, endtime, servername);
      first->next = NULL;
    }
    else {
      while (current != NULL) {
        found = strcmp(current->devname, rp->dgn);
        if (found == 0) {
          enter_data(current, rp, divisions, timestamp, endtime, servername);
          break;
        }
        if (found != 0 && current->next == NULL) {
          new = (struct numq *)calloc(1, sizeof(struct numq));
          set_q_timeslots(new);
          enter_data(new, rp, divisions, timestamp, endtime, servername);
          new->next = NULL;
          current->next = new;
        }
        current = current->next;
      }
    }
  }
  return(0);
}

/* ************************************************************************** */


/* Function to enter summary information */

int add_suminfo(rp, num_devs, sumstats)
struct accttape *rp;
int num_devs;
struct summary *sumstats;
{
  int i = 0;
  static char lastdgn[10];

  for (i = 0; i < num_devs; i++) {
    if (strcmp(rp->dgn, devgrps[i]) == 0) {
      break;
    }
  }
  if (i == num_devs) {
    if ( strcmp(lastdgn,rp->dgn) ) 
      fprintf (stderr, "invalid dgn: %s\n", rp->dgn);
    strcpy(lastdgn,rp->dgn);
    return (1);
  }

  if (rp->subtype == TP2MOUNT) {
    switch (rp->reason) {
      case TPM_NORM:
                      sumstats->norm[i]++;
                      break;
      case TPM_WNGR:
                      sumstats->wrong_ring[i]++;
                      break;
      case TPM_WNGV:
                      sumstats->wrong_vsn[i]++;
                      break;
      case TPM_RSLT:
                      sumstats->reselect[i]++;
                      break;
      default:
                      fprintf(stderr, "Invalid mount reason: %d\n", rp->reason);
    }
  }
  else {
    sumstats->mounts[i]++;
  }
  return (0);
}

/* ************************************************************************** */


/* Function to enter group and user mount information */

int addtpmnt(rp, num_devs)
struct accttape *rp;
int num_devs;
{
  int i = 0;				/* Counter */
  int found = 0;			/* Set if matching record found */
  struct grpstats *gp = NULL;		/* Pointer to grpstats record */
  struct grpstats *prev = NULL;		/* Pointer to grpstats record */
  struct userstats *usr = NULL;		/* Pointer to userstats record */
  struct userstats *previous = NULL;	/* Pointer to userstats record */

/* ******************************************************************************************** */

/* Find matching record or create a new one */

  if (rp->subtype == TP2MOUNT) {
    usr = users_requests;
  }
  else if (rp->subtype == TPMOUNTED) {
    usr = users_mounts;
  }

  while (usr) {
    if (rp->uid == usr->uid && rp->gid == usr->gid) {
      found = 1;
      break;
    }
    previous = usr;
    usr = usr->next;
  }

  if (!found) {
    usr = (struct userstats *)calloc(1, sizeof(struct userstats));
    if (rp->subtype == TP2MOUNT) {
      if (users_requests == NULL) {
        users_requests = usr;
        users_requests->next = NULL;
        users_requests->prev = NULL;
      }
      else {
        usr->next = NULL;
        usr->prev = previous;
        previous->next = usr;
      }
    }
    else if (rp->subtype == TPMOUNTED) {
      if (users_mounts == NULL) {
        users_mounts = usr;
        users_mounts->next = NULL;
      }
      else {
        usr->next = NULL;
        previous->next = usr;
      }
    }

    usr->gid = rp->gid;
    usr->uid = rp->uid;
  }
/* ******************************************************************************************** */
/* Find matching record or create a new one */

  found=0;

  if (rp->subtype == TP2MOUNT) {
    gp = grp_requests;
  }
  else if (rp->subtype == TPMOUNTED) {
    gp = grp_mounts;
  }

  while (gp) {
    if (rp->gid == gp->gid) {
      found = 1;
      break;
    }
    prev = gp;
    gp = gp->next;
  }

  if (!found) {
    gp = (struct grpstats *)calloc(1, sizeof(struct grpstats));
    if (rp->subtype == TP2MOUNT) {
      if (grp_requests == NULL) {
        grp_requests = gp;
        grp_requests->next = NULL;
      }
      else {
        gp->next = NULL;
        prev->next = gp;
      }
    }
    else if (rp->subtype == TPMOUNTED) {
      if (grp_mounts == NULL) {
        grp_mounts = gp;
        grp_mounts->next = NULL;
      }
      else {
        gp->next = NULL;
        prev->next = gp;
      }
    }
    gp->gid = rp->gid;
  }


/* Enter data in relevant record */

  for (i = 0; i < num_devs; i++) {
    if (strcmp(rp->dgn, devgrps[i]) == 0) {
      break;
    }
  }
  if (i == num_devs) {
    fprintf(stderr, "Invalid dgn: %s\n", rp->dgn);
    return(1);
  } 
  else {
    gp->nb[i]++;
    usr->nb[i]++;
    if (rp->subtype == TP2MOUNT) {
      total_requests[i]++;
    }
    else if (rp->subtype == TPMOUNTED) {
      total_mounts[i]++;
    }
  }
  return(0);
}

/* ************************************************************************** */
/* Function to sort users from the most active to the less*/ 



int sort_usrstats(num_devs,device_group_counter)
{
  struct userstats *usr = NULL;		    /* Pointer to userstats record */
  struct userstats *current_max = NULL;
  int finished_sorting = 0;
  struct userstats *sortedlist_start = NULL;
  struct userstats *sortedlist = NULL;
  int element_count;


  usr = users_requests;                 
  if (usr == NULL) {
    fprintf(stderr, "User list empty\n");
    return(1);
  }

  usr = users_requests;
  while (usr != NULL) {
    element_count++;
    usr = usr->next;
}

  while (!finished_sorting) {
    usr = users_requests;
    current_max = NULL;

    /* Finding the max in the list */
    while (usr != NULL) {
      if (current_max == NULL) {
        current_max = usr;
      } else {
        if (usr->nb[device_group_counter]>current_max->nb[device_group_counter]) {
          current_max = usr;
        }
      }
      usr = usr->next;
    }
    if (current_max == NULL) {
      break;
    }

    /* Removing the element from the first list */
    if (current_max->next == NULL) {
      if (current_max->prev == NULL) {
        /* One element list */
        users_requests = NULL;
        finished_sorting = 1;
      } else {
        /* Max is last element in list */
        current_max->prev->next = NULL;
      }
    } else {
      /* In this case, the max is NOT the last element in list */
      if (current_max->prev == NULL) {
        /* First element of the list */
        users_requests = current_max->next;
        current_max->next->prev = NULL;
      } else {
        /* Max is a normal element in list */
        current_max->prev->next = current_max->next;
        current_max->next->prev = current_max->prev;
      }		
    }
    
    /* Now adding the element to the new list */
    if (sortedlist == NULL) {
      sortedlist = current_max;
      sortedlist_start = current_max;
      current_max->next = NULL;
      current_max->prev = NULL;	
    } else {
      sortedlist->next = current_max;
      current_max->next = NULL;
      current_max->prev = sortedlist;
      sortedlist = sortedlist->next;			
    }
  } 
  printsorted_list(sortedlist_start,device_group_counter);

  /* Now resetting users_requests */
  users_requests = sortedlist_start;

  return(0);
}

 /* ********************************************************************* */  
/* Function to print the result of the sorted list*/
  
int printsorted_list(sortedlist_start,device_group_counter)
     struct userstats *sortedlist_start;
     int device_group_counter;
{
  
  struct group *gr = NULL;				
  struct passwd *pw = NULL;
  struct userstats *sortedlist;
  int i=0;
  sortedlist = sortedlist_start;
  {
    if (sortedlist && sortedlist->nb[device_group_counter])
      printf("\n\nDevice group name: %-8s\n\n",devgrps[device_group_counter]);
    while (i < 20 && sortedlist && sortedlist->nb[device_group_counter]) {
      
      if ((pw = getpwuid(sortedlist->uid))) {
        printf("%-10s ", pw->pw_name);
      } else {
        printf("%-10d ", sortedlist->uid);
      }
      if ((gr = getgrgid(sortedlist->gid))) {
        printf("%-8s ", gr->gr_name);
      } else  {
        printf("%-8d ", sortedlist->gid);
      }
      printf(" %d \n", sortedlist->nb[device_group_counter]);
      sortedlist=sortedlist->next;
      i++;
    }
    return (0);
  }
}	
/* *************************************************************************** */
/* Function to validate device group */

int chk_devgrp(arg, num_devs)
char *arg;
int num_devs;
{
  int i = 0;			/* Counter */
  int found = 0;		/* Flag set if valid */


/* Cycle through device group list and if command line argument is valid */
/* set flag */

  for (i = 0; i < num_devs; i++) {
    if (strcmp(arg, devgrps[i]) == 0){
      found = 1;
      break;
    }
  }
  return found;
}

/* ************************************************************************** */


/* Function to validate server name */

int chk_serv(arg, list, numservs)
char *arg;
char list[MAXSERVS][9];
int numservs;
{
  int i = 0;			/* Counter */
  int found = 0;		/* Flag set if valid */


/* Cycle through server list and set flag if command line argument is valid */

  for (i = 0; i < numservs; i++) {
    if (strcmp (arg, list[i]) == 0){
      found = 1;
      break;
    }
  }
  return found;
}

/* ************************************************************************** */


/* Function to validate volume vid */

int chk_vol(arg, expstruct)
char *arg;
Cregexp_t **expstruct;
{
  int ok = 1;			/* Flag unset if not valid */

  if ((*expstruct = Cregexp_comp(arg)) == NULL) {
    fprintf(stderr, "Volume vid %s not parsed by regular expression function\n", arg);
    ok = 0;
  }

  return ok;
}

/* ************************************************************************** */

#if defined(VDQM)
/* Function to get drive list from VDQM */

int get_drvlist() {
  int rc = 0;
  vdqmnw_t *nw = NULL;
  DrvList_t *drvreq = NULL;
  DrvList_t *tmpdrv = NULL;
  int last_id = 0;
  int found = 0;

  do {
    if ( drvreq == NULL ) 
      drvreq = (DrvList_t *)calloc(1,sizeof(DrvList_t));
    if ( drvreq == NULL ) {
        perror("malloc()");
        return(-1);
    }
    rc = vdqm_NextDrive(&nw,&drvreq->drv);
    if ( rc != -1 && *drvreq->drv.server != '\0' && 
         *drvreq->drv.drive != '\0' &&
         drvreq->drv.DrvReqID != last_id ) {
      found = 0;
      CLIST_ITERATE_BEGIN(drvlist,tmpdrv) {
        if ( strcmp(tmpdrv->drv.dgn,drvreq->drv.dgn) == 0 &&
             strcmp(tmpdrv->drv.server,drvreq->drv.server) == 0 ) found = 1;
      } CLIST_ITERATE_END(drvlist,tmpdrv);
      if ( found == 0 ) { 
        CLIST_INSERT(drvlist,drvreq);
        last_id = drvreq->drv.DrvReqID;
        drvreq = NULL;
      }
    }
  } while ( rc != -1 );
  /* Last drvreq not used */
  if ( drvreq != NULL ) free(drvreq);
  return(0);
}
#endif /* VDQM */

/* Function to create device group list */

int create_devicelist(devgrps)
char devgrps[MAXDGP][CA_MAXDGNLEN+1];
{
  int i = 0;			/* Counter */
  int num_devs = 0;		/* Count of number of devices found */
  char *current_server = NULL;	/* Pointer to token */
  struct servlist *currentserv = NULL; /* Pointer to next server name entry */
#if defined(VDQM)
  DrvList_t *tmpdrv = NULL;
#endif /* VDQM */

/* Clear the devgrpserv list before using it for the first time */

  for (i = 0; i < MAXDGP; i++) {
    devgrpserv[i] = NULL;
  }

/* Travers the list of all drives provided by VDQM. Get all device group     */
/* names and create a linked list of server names which belong to each       */
/* device group.                                                             */

#if defined(VDQM)
  CLIST_ITERATE_BEGIN(drvlist,tmpdrv) {
     for (i=0; (devgrps[i] != NULL && strcmp(devgrps[i],tmpdrv->drv.dgn) &&
          i<num_devs); i++);
     current_server = tmpdrv->drv.server;
     if (devgrpserv[i] == NULL) {
       devgrpserv[i] = (struct servlist *)calloc(1,sizeof(struct servlist));
       currentserv = devgrpserv[i];
     } else {
       if ( i < num_devs ) currentserv = devgrpserv[i];
       while (currentserv->next != NULL ) currentserv = currentserv->next;
       currentserv->next = (struct servlist *)calloc(1,sizeof(struct servlist));
       currentserv = currentserv->next;
     }
     strcpy(currentserv->servname, current_server);
     if ( i == num_devs ) {
       strcpy(devgrps[num_devs], tmpdrv->drv.dgn);
       num_devs++;
     }
  } CLIST_ITERATE_END(drvlist,tmpdrv);
#endif /* VDQM */

  return num_devs;
}

/* ************************************************************************** */


/* Function to create a list of all servers making sure that there are no */
/* duplicates */

int create_serverlist(list, num_devs)
char list[MAXSERVS][9];
int num_devs;
{
  char *current_server = NULL;	/* Pointer to token */
  int j = 0;			/* Counter */
  int server_found = 0;		/* Flag set if server found */
  int numservs = 0;		/* Number of servers */
#if defined(VDQM)
  DrvList_t *tmpdrv = NULL;
#endif /* VDQM */

/* Using drive list created by get_drvlist() and extract the names  of all */
/* servers currently in use.                                               */

#if defined(VDQM)
  CLIST_ITERATE_BEGIN(drvlist,tmpdrv) {
    current_server = tmpdrv->drv.server;
    server_found = 0;
    for (j = 0; j < numservs; j++) {
      if ((strcmp(list[j], current_server)) == 0) {
        server_found = 1;
        break;
      }
    }
    if (!server_found) {
      strcpy(list[numservs], current_server);
      numservs++;
    }
  } CLIST_ITERATE_END(drvlist,tmpdrv);
#endif /* VDQM */

  return numservs;
}

/* ************************************************************************** */


/* Function to create a list of servers for one particular device */

int create_reqserverlist(dev_found, devgroup, list, num_devs)
int dev_found;
char devgroup[CA_MAXDGNLEN+1];
char list[MAXSERVS][9];
int num_devs;
{
  int numservs = 0;		/* Number of servers to be read */

#if defined(VDQM)
  DrvList_t *tmpdrv = NULL;
#endif /* VDQM */

/* If a device has been specified create its list of servers else create a  */
/* list of all the servers in use */

  if ( dev_found ) {
#if defined(VDQM)
    CLIST_ITERATE_BEGIN(drvlist,tmpdrv) {
      if ( strcmp(tmpdrv->drv.dgn,devgroup) == 0 ) {
        strcpy(list[numservs], tmpdrv->drv.server);
        numservs++;
      }
    } CLIST_ITERATE_END(drvlist,tmpdrv);
#endif /* VDQM */
  } else {
    numservs = create_serverlist(list, num_devs);
  }
  return numservs;
}

/* ************************************************************************** */


/* Function to convert date and time into seconds */

time_t
cvt_datime(arg)
char *arg;
{
  time_t current_time = 0;
  static int lastd[12] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int n = 0;
  struct tm t;
  struct tm *tm;

  memset((char *) &t, 0, sizeof(struct tm));
  time (&current_time);	/* Get current time */
  tm = localtime(&current_time);
  n = sscanf(arg, "%2d%2d%2d%2d%2d", &t.tm_mon, &t.tm_mday, &t.tm_hour,
             &t.tm_min, &t.tm_year);

  if (n < 4) {
    return (-1);
  }
  if (n == 4)
    t.tm_year = tm->tm_year;
  else if (t.tm_year >= 0 && t.tm_year <= 37)
    t.tm_year += 100;
  else if (t.tm_year >= 38 && t.tm_year <= 69)
    return (-1);

  if ((t.tm_mon < 1) || (t.tm_mon > 12)) {
    return (-1);
  }
  if ((t.tm_mon == 2) && (t.tm_mday == 29) && (t.tm_year % 4 != 0)) {
    return (-1);
  }
  if ((t.tm_mday < 1) || (t.tm_mday > lastd[t.tm_mon-1])) {
    return (-1);
  }
  t.tm_mon--;

#if defined(sun) && ! defined(SOLARIS)
  return (timelocal(&t));
#else
  t.tm_isdst = -1;
  return (mktime(&t));
#endif
}

/* ************************************************************************** */


/* Function to enter the data on queue lengths */

int enter_data(record, rp, divisions, timestamp, endtime, servername)
struct accttape *rp;
struct numq *record;
time_t divisions[];
time_t timestamp, endtime;
char *servername;
{
  int i = 0;			/* Counter */
  int j = 0;			/* Counter */
  int qlen = 0;			/* Current records queue length */
  int qdifference = 0;		/* Current queue difference */
  int found = 0;		/* Set if a dev group/server element already */
				/* exists in the linked list */
  struct serverqlength *csql = fsql; /* Current server q length element */


/* Get the right dev droup/server element or create a new one */

  while (csql != NULL) {
    if (((strcmp(rp->dgn, csql->devname)) == 0) &&
        ((strcmp(servername, csql->servname)) == 0)) {
      found = 1;
      break;
    }
    csql = csql->next;
  }
  if (found == 0) {
    if (fsql == NULL) {
      fsql = (struct serverqlength *)calloc(1, sizeof(struct serverqlength));
      strcpy (fsql->devname, rp->dgn);
      strcpy (fsql->servname, servername);
      fsql->qlength = 0;
      fsql->next = NULL;
      csql = fsql;
    }
    else {
      csql = (struct serverqlength *)calloc(1, sizeof(struct serverqlength));
      strcpy (csql->devname, rp->dgn);
      strcpy (csql->servname, servername);
      csql->qlength = 0;
      csql->next = fsql;
      fsql = csql;
    }
  }


/* Set queue length from input file */

  qlen = rp->fseq;
  qdifference = qlen - csql->qlength;


/* Check which timeslot current record is related too and enter queue length. */
/* If current queue length is greater than any existing record in that */
/* timeslot store the last queue value for each timeslot. */

  for (i = 0; i < NUMDIVS; i++) {
    if (i == NUMDIVS - 1 && (timestamp / 60) >= divisions[i] &&
                            (timestamp / 60) <= endtime) {
      if (record->lastval[i] == -1) {
        if (i == 0) {
          record->lastval[i] = 0;
        }
        else {
          for (j = (i - 1); j > -1; j--) {
            if (record->lastval[j] > -1) {
              record->lastval[i] = record->lastval[j];
              j = -1;
            }
          }
          if (record->lastval[i] == -1) {
            record->lastval[i] = 0;
          }
        }
      }
      record->lastval[i] += qdifference;
      if (record->timeslot[i] < record->lastval[i]) {
        record->timeslot[i] = record->lastval[i];
      }
      if (record->maxq < record->lastval[i]) {
        record->maxq = record->lastval[i];
      }
      break;
    }
    else if ((timestamp / 60) >= divisions[i] &&
             (timestamp / 60) < divisions[i+1]) {
      if (record->lastval[i] == -1) {
        if (i == 0) {
          record->lastval[i] = 0;
        }
        else {
          for (j = (i - 1); j > -1; j--) {
            if (record->lastval[j] > -1) {
              record->lastval[i] = record->lastval[j];
              j = -1;
            }
          }
          if (record->lastval[i] == -1) {
            record->lastval[i] = 0;
          }
        }
      }
      record->lastval[i] += qdifference;
      if (record->timeslot[i] < record->lastval[i]) {
        record->timeslot[i] = record->lastval[i];
      }
      if (record->maxq < record->lastval[i]) {
        record->maxq = record->lastval[i];
      }
      break;
    }
  }
  csql->qlength = qlen;
  strcpy(record->devname, rp->dgn);
  return(0);
}

/* ************************************************************************** */


/* Function to read in records from the accounting file */

int getacctrec(fd_acct, accthdr, buf, swapped)
int fd_acct;
struct accthdr *accthdr;
char *buf;
int *swapped;
{
  int c = 0;		/* Bytes read by fread or rfio_read */

  if (stdin_flag) {
    c = fread(accthdr, 1, sizeof(struct accthdr), stdin);
  }
  else {
    c = rfio_read(fd_acct, accthdr, sizeof(struct accthdr));
  }

  if (c != sizeof(struct accthdr)) {
    if (c == 0) {
      return(0);
    }
    if (c > 0) {
      fprintf(stderr, "Read returns unexpected: %d\n", c);
    }
    else {
      fprintf(stderr, "Read error: %s\n", sstrerror(errno));
    }
    server_failed = 1;
    return (0);
  }


/* If required swap byte order */

  if (accthdr->package > 255) {
    swap_it(accthdr->package);
    swap_it(accthdr->len);
    swap_it(accthdr->timestamp);
    *swapped = 1;
  }

  if (accthdr->len > max_acctreclen || accthdr->len < 0 ) {
    fprintf (stderr, "corrupted accounting file\n");
    return (0);
  }
  if (stdin_flag) {
    c = fread(buf, 1, accthdr->len, stdin);
  }
  else {
    c = rfio_read(fd_acct, buf, accthdr->len);
  }

  if (c != accthdr->len) {
    if (c >= 0) {
      fprintf(stderr, "Read returns unexpected: %d\n", c);
    }
    else {
      fprintf(stderr, "Read error: %s\n", sstrerror(errno));
    }
    server_failed = 1;
    return (0);
  }

  return(accthdr->len);
}

/* ************************************************************************** */


/* Function to calculate global queue lengths */

int global_queue()
{
  struct numq *nq = NULL;	/* Pointer to numq record */
  int i = 0;			/* Counter */


/* Add final queue lengths for last server to the total_Q field, and */
/* reinitialise the timeslot fields to -1 */

  nq = first;
  while (nq != NULL) {
    for (i = 0; i < NUMDIVS; i++) {
      if (nq->timeslot[i] >= 0) {
        nq->total_Q[i] = nq->total_Q[i] + nq->timeslot[i];
      }
      nq->timeslot[i] = -1;
      nq->lastval[i] = -1;
      if (nq->total_Q[i] > nq->global_maxq) {
        nq->global_maxq = nq->total_Q[i];
      }
    }
    nq = nq->next;
  }
  return(0);
}

/* ************************************************************************** */


/* Function to set graph divisions */

int graph_divs(starttime, endtime, div, divisions)
time_t starttime;
time_t endtime;
time_t divisions[];
int *div;
{
  int i = 0;			/* Counter */
  time_t gph_s_time = 0;	/* Time of first graph division */


/* Calculate the size of each division, by finding time difference between */
/* start and end times (giving answer in seconds). Divide by 60 to turn */
/* answer into minutes, and then by the number of divisions set by NUMDIVS, */
/* to give the size of each division. */

  *div = (endtime - starttime) / 60 / NUMDIVS;


/* Round answer up to nearest 5 minutes */

  if ((*div % 5) > 0) {
    *div = *div + (5 - (*div % 5));
  }


/* Set the start time for the graph in minutes, and then round up to the */
/* nearest 10 minutes */

  gph_s_time = starttime / 60;
  if ((gph_s_time % 10) > 0) {
    gph_s_time = gph_s_time - (gph_s_time % 10);
  }
  divisions[0] = gph_s_time;
  for (i = 1; i < NUMDIVS; i++) {
    divisions[i] = divisions[i-1] + *div;
  }
  return(0);
}

/* ************************************************************************** */

/* Function to print out device usage statistics */

int print_devusage (starttime, endtime, divisions, stdin_flag)
time_t starttime;
time_t endtime;
time_t divisions[];
int stdin_flag;
{
  struct dev_usage *du = NULL;	/* Pointer to dev_usage record */
  struct devstats *dp = NULL;	/* Pointer to devstats record */
  int hour = 0;			/* Hour part of total time unit in use */
  int mins = 0;			/* Minute part of total time unit in use */
  int sec = 0;			/* Second part of total time unit in use */
  long period = 0;		/* Total length of period (in secs) */
  long timedown = 0;		/* Total time unit was down */
  long timeidle = 0;		/* Total time unit was idle */
  int idle_hour = 0;		/* Hour part of idle time */
  int idle_min = 0;		/* Minute part of idle time */
  int idle_sec = 0;		/* Second part of idle time */
  struct tm *tm = NULL;		/* Pointer to tm record */
  time_t pr_time = 0;		/* Division time value */
  int i = 0;			/* Counter */
  int j = 0;			/* Counter */


/* For each device group read through utilisation lists and set any values */
/* for time periods in which no entrys were found */

  if (!stdin_flag) {
    du = dev_use;
    while (du) {
      for (i = 0; i < NUMDIVS; i++) {
        if (du->last_util[i] == -1) {
          du->last_util[i] = 0;
        }
        if (i == 0 && du->max_util[i] == -1) {
          du->max_util[i] = 0;
        }
        else if (du->max_util[i] == -1) {
          du->max_util[i] = du->last_util[i - 1];
        }
      }
      du = du->next;
    }
  }


/* Calculate total period of time in seconds */

  period = endtime - starttime;
 

/* ************************************************** */

/* Print out device usage for each time division */

  du = dev_use;
  while (du) {
    printf("\fDevice utilization for %s", du->dgn);
    print_time_interval(starttime, endtime);
    printf("\n");

    if (!stdin_flag) {
      for (i = 0; i < NUMDIVS; i++) {
        pr_time = divisions[i] * 60;
        if (pr_time <= endtime) {
          tm = localtime(&pr_time);
          printf("%02d/%02d/%04d %02d:%02d", tm->tm_mday, tm->tm_mon+1,
                 tm->tm_year+1900, tm->tm_hour, tm->tm_min);
          printf("\t\t%d\t", du->max_util[i]);
          for (j = 0; j < du->max_util[i]; j++) {
            printf("*");
          }
          printf("\n");
        }
      }
    }


/* Print out statistics for each unit */

    printf("  Device                    assigned            idle               down\n");
    dp = devlist;
    while (dp) {
      if (strcmp (dp->dgn, du->dgn) == 0) {
        hour = dp->timeused / 3600;
        mins = (dp->timeused - (hour * 3600)) / 60;
        sec = dp->timeused - (hour * 3600) - (mins * 60);
        timedown = dp->timedown;
        if (dp->confdown) {
          timedown += (endtime - dp->confdown);
        }
        timeidle = period - (dp->timeused + timedown);
        idle_hour = timeidle / 3600;
        idle_min = (timeidle - (idle_hour * 3600)) / 60;
        idle_sec = timeidle - (idle_hour * 3600) - (idle_min * 60);
        printf("%8s@%-8s  %5d  %3dh%02dm%02ds(%5.1f%%)  ", dp->drive, dp->snm,
	dp->nbmounts, hour, mins, sec, dp->timeused * 100. / period);
        printf("%3dh%02dm%02ds(%5.1f%%)  %5d\n", idle_hour, idle_min,
               idle_sec, timeidle * 100. / period, dp->nbdowns);
      }
      dp = dp->next;
    }
    du = du->next;
  }
  return(0);
}

/* ************************************************************************** */


/* Function to print out mount requests by group and device */

int print_mountdetails(starttime, endtime, num_devs, option, devgroup)
time_t starttime;
time_t endtime;
int num_devs;
char option;
char devgroup[CA_MAXDGNLEN+1];
{
  struct grpstats *gp = NULL;	/* Pointer to grpstats record */
  struct group *gr = NULL;	/* Pointer to group record */
  struct userstats *usr;	/*pointer to userstats record */ 
  int i = 0;			/* Counter */
  int devgroupnum = -1;		/* Index of devgroup in devgrps */


/* Set devgroupnum according to the devgroup name on the command line.  If no */
/* devgroup name was set then leave devgroupnum at -1. */

  if (devgroup[0] != '\0') {
    for (i = 0; i < num_devs; i++) {
      if ((strcmp(devgrps[i], devgroup)) == 0) {
        devgroupnum = i;
        break;
      }
    }
    if (devgroupnum == -1) {
      fprintf(stderr, "\nError selecting device group to display!\n\n");
    }
  }

/* For all groups print out the number of mount requests made for each device */
/* unless devgroup is set.  In which case print out the number of mount */
/* requests for that group only. */

  if (option == 'M') {
    printf("\nNumber of tape mount requests");
  }
  else if (option == 'm') {
    printf("\nNumber of physical tape mounts");
  }
  else if (option == 'u'){
    int device_group_counter;
    usr = users_requests;
    if (usr == NULL) {
      fprintf(stderr, "User list empty\n");
      return(1);
    }
    printf("\nMost active users by device group");
    print_time_interval(starttime, endtime);

    for(device_group_counter=0; device_group_counter<num_devs; device_group_counter++) {
      sort_usrstats(num_devs,device_group_counter);
    }
  }

  if (option != 'u'){
    print_time_interval(starttime, endtime);
    printf("\n\nGroup    ");
    if (devgroupnum == -1) {
      for (i = 0; i < num_devs; i++) {
        printf ("%-8s", devgrps[i]);
      }
    }
    else {
      printf ("%-8s", devgrps[devgroupnum]);
    }
    printf ("\n");

    if (option == 'M') {
      gp = grp_requests;
    }
    else if (option == 'm') {
      gp = grp_mounts;
    }
    
    while (gp) {
      if ( (gr = getgrgid(gp->gid)) == NULL ) {
        printf("\n%-8d",gp->gid);
      } else {
        printf("\n%-8s", gr->gr_name);
      }
      if (devgroupnum == -1) {
        for (i = 0; i < num_devs; i++) {
          printf("%5d   ", gp->nb[i]);
        }
      }
      else {
        printf("%5d   ", gp->nb[devgroupnum]);
      }
      gp = gp->next;
    }
    printf("\nTotal   ");
    if (devgroupnum == -1) {
      for (i = 0; i < num_devs; i++) {
        if (option == 'M') {
          printf("%5d   ", total_requests[i]);
        }
        else if (option == 'm') {
          printf("%5d   ", total_mounts[i]);
        }
      }
    }
    else {
      if (option == 'M') {
        printf("%5d   ", total_requests[devgroupnum]);
      }
      else if (option == 'm') {
        printf("%5d   ", total_mounts[devgroupnum]);
      }
    }
  }
  printf("\n");
  return(0);
}

/* ************************************************************************** */


/* Function to print out queue lengths information */

int print_queue(starttime, endtime, record, divisions, devgroup, serv_name, num_devs)
time_t starttime;
time_t endtime;
struct numq *record;
time_t divisions[];
char devgroup[CA_MAXDGNLEN+1];
char serv_name[CA_MAXHOSTNAMELEN+1];
int num_devs;
{
  int i = 0;			/* Counter */
  int j = 0;			/* Counter */
  struct numq *current = NULL;	/* Pointer to numq record */
  struct tm *tm = NULL;		/* Pointer to tm record */
  time_t pr_time = 0;		/* Division time value */
  int notprinted_dev[MAXDGP];	/* Boolean values used if queue exists */
  struct servlist *currentserv = NULL; /* Pointer to servlist record */


  for (i = 0; i < num_devs; i++) {
    notprinted_dev[i] = 1;
  }


/* For each device group set the values for any time division which has had */
/* no entry and then print out the waiting queue length */

  current = record;
  while (current != NULL) {
    ti_vals(current);
    printf("\f\nWaiting Queue Lengths for Device: %s\n", current->devname);
    printf("During time interval\t");
    print_time_interval(starttime, endtime);
    printf("\n");
    for (i = 0; i < NUMDIVS; i++) {
      pr_time = divisions[i] * 60;
      if (pr_time <= endtime) {
        tm = localtime(&pr_time);
        printf("%02d/%02d/%04d %02d:%02d", tm->tm_mday, tm->tm_mon+1,
               tm->tm_year+1900, tm->tm_hour, tm->tm_min);
        printf("\t\t%d\t  ", current->timeslot[i]);
        for (j = 0; j < current->timeslot[i]; j++) {
          printf("*");
        }
        printf ("\n");
      }
    }
    printf ("\nMaximum queue length for device %s was %d\n",
            current->devname, current->maxq);
    for (i = 0; i < num_devs; i++) {
      if (strcmp (current->devname, devgrps[i]) == 0) {
        notprinted_dev[i] = 0;
        break;
      }
    }
    current = current->next;
  }
  for (i = 0; i < num_devs; i++) {
    if (notprinted_dev[i] == 1) {
      if ((devgroup[0] == '\0') && (serv_name[0] == '\0')) {
        printf("\nNo Queue for Device: %s\n", devgrps[i]);
      }
      else if ((devgroup[0] != '\0') && (serv_name[0] == '\0')) {
        if (strcmp (devgroup, devgrps[i]) == 0) {
          printf("\nNo Queue for Device: %s\n", devgrps[i]);
        }
      }
      else if ((devgroup[0] == '\0') && (serv_name[0] != '\0')) {
        currentserv = devgrpserv[i];
        while (currentserv != NULL) {
          if (strcmp (currentserv->servname, serv_name) == 0) {
            printf("\nNo Queue for Device: %s\n", devgrps[i]);
            break;
          }
          currentserv = currentserv->next;
        }
      }
      else {
        currentserv = devgrpserv[i];
        while (currentserv != NULL) {
          if (strcmp (currentserv->servname, serv_name) == 0) {
            if (strcmp (devgroup, devgrps[i]) == 0) {
              printf("\nNo Queue for Device: %s\n", devgrps[i]);
            }
            break;
          }
          currentserv = currentserv->next;
        }
      }
    }
  }
  return(0);
}

/* ************************************************************************** */


/* Function to print out summary information */

int print_summary(starttime, endtime, num_devs, sumstats, devgroup)
time_t starttime;
time_t endtime;
int num_devs;
struct summary *sumstats;
char devgroup[CA_MAXDGNLEN+1];
{
  int i = 0;			/* Counter */
  int t_requests = 0;
  int t_mounts = 0;
  int devgroupnum = -1;		/* Index of devgroup in devgrps */
  struct userstats *usr = NULL;	/* Pointer to userstats */
  usr= (struct userstats *)calloc(1, sizeof(struct userstats));

  /* Set devgroupnum according to the devgroup name on the command line.  If no */
  /* devgroup name was set then leave devgroupnum at -1. */

  if (devgroup[0] != '\0') {
    for (i = 0; i < num_devs; i++) {
      if ((strcmp(devgrps[i], devgroup)) == 0) {
        devgroupnum = i;
        break;
      }
    }
    if (devgroupnum == -1) {
      fprintf(stderr, "\nError selecting device group to display!\n\n");
    }
  }

  printf("\nNumber of tape mount requests");
  print_time_interval(starttime, endtime);
  printf("\n\nReason\t\t\t");

  if (devgroupnum == -1) {
    for (i = 0; i< num_devs; i++) {
      printf("%-8s", devgrps[i]);
    }
  }
  else {
    printf("%-8s", devgrps[devgroupnum]);
  }

  printf("\n\nNormal\t\t\t");
  if (devgroupnum == -1) {
    for (i = 0; i < num_devs; i++) {
      printf("%-8d", sumstats->norm[i]);
      t_requests = t_requests + sumstats->norm[i];
    }
  }
  else {
    printf("%-8d", sumstats->norm[devgroupnum]);
    t_requests = t_requests + sumstats->norm[devgroupnum];
  }

  printf("\nWrong ring\t\t");
  if (devgroupnum == -1) {
    for (i = 0; i < num_devs; i++) {
      printf("%-8d", sumstats->wrong_ring[i]);
      t_requests = t_requests + sumstats->wrong_ring[i];
    }
  }
  else {
    printf("%-8d", sumstats->wrong_ring[devgroupnum]);
    t_requests = t_requests + sumstats->wrong_ring[devgroupnum];
  }

  printf("\nWrong VSN\t\t");
  if (devgroupnum == -1) {
    for (i = 0; i < num_devs; i++) {
      printf("%-8d", sumstats->wrong_vsn[i]);
      t_requests = t_requests + sumstats->wrong_vsn[i];
    }
  }
  else {
    printf("%-8d", sumstats->wrong_vsn[devgroupnum]);
    t_requests = t_requests + sumstats->wrong_vsn[devgroupnum];
  }

  printf("\nReselect\t\t");
  if (devgroupnum == -1) {
    for (i = 0; i < num_devs; i++) {
      printf("%-8d", sumstats->reselect[i]);
      t_requests = t_requests + sumstats->reselect[i];
    }
  }
  else {
    printf("%-8d", sumstats->reselect[devgroupnum]);
    t_requests = t_requests + sumstats->reselect[devgroupnum];
  }

  printf("\n\nTotal requests/Dev.Grp. ");
  if (devgroupnum == -1) {
    for (i = 0; i < num_devs; i++) {
      printf("%-8d", sumstats->norm[i] + sumstats->wrong_ring[i] +
             sumstats->wrong_vsn[i] + sumstats->reselect[i]);
    }
  }
  else {
    printf("%-8d", sumstats->norm[devgroupnum] +
                   sumstats->wrong_ring[devgroupnum] +
                   sumstats->wrong_vsn[devgroupnum] +
                   sumstats->reselect[devgroupnum]);
  }

  printf("\n\nTotal mounts requested\t%-8d\n", t_requests);

  printf("\n\t\t\t");
  if (devgroupnum == -1) {
    for (i = 0; i < num_devs; i++) {
      printf("%-8s", devgrps[i]);
    }
  }
  else {
    printf("%-8s", devgrps[devgroupnum]);
  }

  printf("\nMount/Device group\t");
  if (devgroupnum == -1) {
    for (i = 0; i < num_devs; i++) {
      printf("%-8d", sumstats->mounts[i]);
      t_mounts = t_mounts + sumstats->mounts[i];
    }
  }
  else {
    printf("%-8d", sumstats->mounts[devgroupnum]);
    t_mounts = t_mounts + sumstats->mounts[devgroupnum];
  }

  printf("\n\nTotal tape mounts\t%-8d\n", t_mounts);
  return(0);
}

/* ************************************************************************** */


/* Function to print out mount time details */

int print_time_details(starttime, endtime)
time_t starttime;
time_t endtime;
{
  struct devstats *dp = NULL;
  int hour = 0;
  int mins = 0;
  int sec = 0;
  int period = 0;
  long timedown = 0;
  long timeidle = 0;

  period = endtime - starttime;
  printf("\n\nDevice utilisation");
  print_time_interval(starttime, endtime);
  printf("\nDevice\t\t\tmnt.req\tmounts\tsec/mnt\tmin s/m\tmax s/m\tfailure rate\n\n");

  dp = devlist;
  while (dp) {
    hour = dp->timeused / 3600;
    mins = (dp->timeused - hour * 3600) / 60;
    sec = dp->timeused - (hour * 3600) - (mins * 60);
    timedown = dp->timedown;
    if (dp->confdown) {
      timedown += (endtime - dp->confdown);
    }
    timeidle = period - (dp->timeused + timedown);

    if (dp->nbmounted > 0) {
      printf("%8s@%-8s\t%5d\t%5d\t%5ld\t%5ld\t%5ld\t%5.1f %%\n", dp->drive, dp->snm
	     , dp->nbreqs, 
	     dp->nbmounted, dp->time2mount/dp->nbmounted, dp->min2mount,
             dp->max2mount, (dp->nbreqs - dp->nbmounted) * 100. / dp->nbreqs);
    }
    else if (dp->nbreqs > 0) {
      printf("%8s%8s\t%5d\t%5d\n", dp->drive, dp->snm, dp->nbreqs, 
	     dp->nbmounted);
    }
    dp = dp->next;
  }
  return(0);
}

/* ************************************************************************** */


/* Function to print time */

int print_time_val(stime)
time_t stime;
{
  struct tm *tm = NULL;		/* Pointer to tm record */

  tm = localtime(&stime);
  printf ("%02d/%02d/%04d %02d:%02d:%02d", tm->tm_mday, tm->tm_mon+1,
          tm->tm_year+1900, tm->tm_hour, tm->tm_min, tm->tm_sec);
  return(0);
}

/* ************************************************************************** */


/* Function to print out time interval */

int print_time_interval(starttime, endtime)
time_t starttime;
time_t endtime;
{
  printf(" (");
  print_time_val(starttime);
  printf("  -  ");
  print_time_val(endtime);
  printf(")");
  return(0);
}

/* ************************************************************************** */


/* Function to print out detailed list of mounted volumes */

int print_tpentry(rp, timestamp, devgroup, Lflag, expstruct,servname)
struct accttape *rp;
time_t timestamp;
char devgroup[CA_MAXDGNLEN+1];
int Lflag;
Cregexp_t *expstruct;
char *servname;
{
  char strtime[27];
  struct passwd *pw = NULL;
  struct group *gr = NULL;
  char user[20];
  char group[20];

  if (!Lflag) {
    if (Cregexp_exec(expstruct, &(rp->vid[0])) == 0) {
      Lflag++;
    }
  }

  if (Lflag) {
    if ((devgroup[0] == '\0') || (strcmp(rp->dgn, devgroup) == 0)) {
      strcpy(strtime, ctime(&timestamp));
      strtime[strlen(strtime) - 1] = '\0';
      if ((pw = (struct passwd *)getpwuid(rp->uid)) == NULL) {
        sprintf(user, "%d", rp->uid);
      }
      else {
        strcpy(user, pw->pw_name);
      }

      if ((gr = getgrgid(rp->gid)) == NULL) {
        sprintf(group, "%d", rp->gid);
      }
      else {
        strcpy(group, gr->gr_name);
      }

      if (rp->fseq == -1) {
        printf("%s n %s %s %s %s %s@%s\n", rp->vid, strtime,
                user, group, rp->dgn, rp->drive, servname);
      }
      else if (rp->fseq == -2) {
        printf("%s u %s %s %s %s %s@%s\n", rp->vid, strtime,
                user, group, rp->dgn, rp->drive, servname);
      }
      else {
        printf("%s %d %s %s %s %s %s@%s\n", rp->vid, rp->fseq, strtime,
                user, group, rp->dgn, rp->drive, servname);
      }
    }
  }
  return(0);
}

/* ************************************************************************** */


/* Function to initialise the queue length timeslots */

int set_q_timeslots(record)
struct numq *record;
{
  int i = 0;			/* Counter */


/* Set each timeslot to -1, so any timeslot with no entry can be identified */

  for (i = 0; i < NUMDIVS; i++)  {
    record->timeslot[i] = -1;
    record->lastval[i] = -1;
    record->total_Q[i] = 0;
  }
  return(0);
}

/* ************************************************************************** */


/* Function to initialise utilisation timeslots */

int set_util_timeslots(du)
struct dev_usage *du;
{
  int i = 0;			/* Counter */


/* Set each timeslot to -1, so any timeslot with no entry can be identified */

  for (i = 0; i < NUMDIVS; i++) {
    du->last_util[i] = -1;
    du->max_util[i] = -1;
    du->unit_util[i] = -1;
  }
  return(0);
}

/* ************************************************************************** */


/* Function to swap byte order of fields */

int swap_fields(rp)
struct accttape *rp;
{
  swap_it(rp->subtype);
  swap_it(rp->uid);
  swap_it(rp->gid);
  swap_it(rp->jid);
  swap_it(rp->fseq);
  swap_it(rp->reason);
  return(0);
}

/* ************************************************************************** */


/* Function to set the value of any timeslot which has had no entry, before */
/* printing */

int ti_vals(record)
struct numq *record;
{
  int i = 0;			/* Counter */

  for (i = 0; i < NUMDIVS; i++) {
    if (i == 0 && record->lastval[i] < 0) {
      record->lastval[i] = 0;
    }
    else if (record->lastval[i] < 0) {
      record->lastval[i] = record->lastval[i - 1];
    }
    if (i == 0 && record->timeslot[i] < 0) {
      record->timeslot[i] = 0;
    }
    else if (record->timeslot[i] < 0) {
      record->timeslot[i] = record->lastval[i - 1];
    }
  }
  return(0);
}

/* ************************************************************************** */


/* Function to give correct usage syntax */

int usage(cmd)
char *cmd;
{
  fprintf(stderr, "Usage: %s ", cmd);
  fprintf(stderr, "%s%s%s", "[--acctdir accounting_dir][-D][-e end_time]\n",
    "\t[-f accounting_file][-g device_group][-L][-M][-m][-Q][-S server name]\n",
    "\t[-s start_time][-T][-u][-V volume_label][-v][-w week_number][-Y]\n");
  return(0);
}
