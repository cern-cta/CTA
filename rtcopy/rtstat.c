/*
* Copyright (C) 1996-2004 by CERN IT
* All rights reserved
*/

/* Modified by Christophe Grosnickel 02/02/2000 */
/* added match_reqerr() */



#ifndef lint
static char sccsid[] = "@(#)rtstat.c,v 1.16 2004/04/28 08:37:43 CERN IT/ADC Claire Redmond";
#endif /* not lint */

#if !defined(_WIN32)
#include <unistd.h>
#endif /* _WIN32 */
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <malloc.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <Cgetopt.h>
#include <sacct.h>
#include <dirent.h>
#include <sys/stat.h>
#include <osdep.h>
#include <net.h>
#if defined(VDQM)
#include <vdqm_api.h>
#endif /* VDQM */
#include <Cregexp.h>
#include <rfio_api.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#if !defined(_WIN32)
#include <sys/times.h>
#else
#include "times.h"
#endif

Cregexp_t *expstruct = NULL;		/* Regular expression structure */

#define SACCT_BUFLEN 4096
#define MAXDGP 20		/* Maximum number of device groups */
#define MAXLEN 7		/* Current max length of device group names */
#define MAXSERVNAME CA_MAXHOSTNAMELEN+1		/* Current maximum length of server name */
#define MAXSERVS 40		/* Maximum number of servers */

/* Macro to swap byte order */

#define swap_it(a) {int __a; \
    swab((char *)&a, (char *)&__a, sizeof(a)); \
a = (unsigned int)__a<<16 | ((unsigned int)__a>>16); }

int allgroups;
struct stats {                  /* Structure to store the statistics info */
    int dev_succ;               /* number of successful developers requests */
    int dev_unsucc;             /* number unsuccessful */
    int usr_succ;               /* number of successful user requests */
    int usr_unsucc;             /* number unsuccessful */
    int usr_errors;		/* number of user errors */
    int tape_errors;		/* numver of tape errors */
    int shift_errors;		/* number of shift errors */
    int robot_errors;		/* number of robot errors */
    int network_errors;		/* number of network errors */
    int other_errors;		/* number of other errors */
    int unknown_errors;		/* number of unidentified errors */
    int no_mess;		/* failed requests with no matching error message */
};
struct stats global;

struct dev_grp {		/* Structure to store device group info */
    char dgn[MAXLEN];		/* device group name */
    char type[9];		/* device group type */
    struct stats stat;		/* statistics record */
    int *err_messages;		/* error messages for device group */
    struct dev_grp *next;	/* pointer to next record in list */
};
struct dev_grp *dev_first = NULL;
struct dev_grp *dev_prev = NULL;

struct failed {			/* Store info on failed requests with no message */
    time_t complete_time;	/* time of completion */
    char dgn[MAXLEN];		/* device group */
    char type[9];		/* device type */
    int jid;			/* job id */
    char server[MAXSERVNAME];	/* server name */
    struct failed *next;	/* pointer to next record */
};
struct failed *fail_first = NULL;
struct failed *fail_prev = NULL;

struct requests {		/* Structure to store request details */
    time_t complete_time;	/* time of completion */
    char dgn[MAXLEN];		/* device group */
    int err_lineno;		/* error line number */
    int exitcode;		/* exit code for command */
    int failed;           /* set if request failed */
    char gid[9];		/* group id */
    char uid[9];          /* user id   */
    int jid;			/* job id */
    int aut;			/* set to number of matches found in err file */
    struct requests *next;	/* next record in list */
    struct requests *prev;	/* pointer to previous record */
    time_t req_time;		/* time of request */
    int retry;			/* retry number for command */
    char server[MAXSERVNAME];	/* server name */
    char type[9];		/* device type */
    char unm[9];		/* unit name */
    char vid[7];		/* volume id */
    char errmsg[CA_MAXLINELEN+1];     /* Error message */  
    char reqtype;     /* Type of request read , write or dump */
    int nb_files; /*number of files for the request */
    int transfer_time;   /* Total time spent for transfers  */
    int transfer_size;   /* Total size (kb) transfered */
};

struct requests *req_first = NULL;
struct requests *req_last = NULL;
struct requests *req_prev = NULL;

struct tape_errs {		/* structure to store tape error info */
    time_t first_time;		/* time of request */
    time_t last_time;		/* last error time */
    char unm[9];		/* unit name */
    char vid[7];		/* voulme id */
    int first_jid;		/* job id of first request with this error */
    int last_jid;		/* job id of last request with this error */
    int num_occs;		/* number of occurances */
    int mess_line;		/* line of error message */
    char server[MAXSERVNAME];	/* server name */
    struct tape_errs *next;	/* next record */
};
struct tape_errs *tape_first = NULL;
struct tape_errs *tape_prev = NULL;

struct tp_sort {
    time_t first_time;            /* time of request */
    time_t last_time;		/*last time of error */
    int first_jid;		/* job id of first request with this error */
    int last_jid;		/* job id of last request with this error */
    int num_occs;		/* number of occurances */
    int mess_line;		/* line of error message */
    char server[MAXSERVNAME];   /* server name */
    char unm[9];                /* unit name */
    char vid[7];                /* volume id */
};
struct tp_sort *tperr_sort;      

struct sorted_ifce_stats {
    
    struct ifce_stats *netstats;
    struct sorted_ifce_stats *next;
    
};

struct server {			/* Structure to store information by server */
    int failed;			/* set if server read failed */
    int nb_dgns;                  /* Number of DGNs server by this server */
    char failure_msg[CA_MAXLINELEN+1];/* failure message */
    char dgn[MAXLEN];		/* first device group on server */
    char type[9];		/* type running on server */
    struct server *next;   	/* pointer to next record in list */
    char server_name[MAXSERVNAME];/* server name */
    struct stats stat;		/* statistics record */
    struct sorted_ifce_stats *sorted_netstats_first; /*Pointer to first sorted network statistics */
    struct ifce_stats *netstats_first; /* Pointer to first network statistics */
    struct ifce_list *ifce_list_first; /* Pointer to first disk server in the list */
    int *err_messages;		/* error messages for server */
};
struct server *serv_first = NULL;
struct server *serv_prev = NULL;

struct store_dgn {		/* Structure to store the dgn for a jid */
    char dgn[MAXLEN];		/* device group name */
    int jid;			/* job id */
    struct store_dgn *next;	/* pointer to next record */
    struct store_dgn *prev;	/* pointer to previous record */
    char unm[9];		/* unit name */
    char vid[7];		/* voulme id */
};
struct store_dgn *store_first = NULL;
struct store_dgn *store_prev = NULL;

struct type {			/* Structure to store info by device type */
    int *err_messages;		/* error messages for device type */
    char type[9];		/* device type */
    struct stats stat;		/* statistics record */
    struct type *next;		/* pointer to next record in list */
};
struct type *type_first = NULL;
struct type *type_prev = NULL;

struct tpconfrec {		/* structure to store data from TPCONFIG file */
    char unm[9];		/* unit name */
    char dgn[9];		/* device group name */
    char dev_name[32];		/* device name */
    char dens[6];		/* density */
    char instat[5];		/* initial status */
    char loader[9];		/* manual or robot */
    char type[9];		/* device type */
};

struct err_mess {		/* structure to store error messages */
    char *message;		/* error message */
    int error_code;		/* type of error */
    int num_occs;		/* number of times the error occurs */
};
struct err_mess *emp;

struct ifce_list
{
    char name[12]; /* Interface name */
    struct ifce_list *next;
    struct ifce_list *prev;
};


struct ifce_stats {  /* Structure to store the network statistics */
    char name[12]; /* Interface name */
    char host_srv[MAXSERVNAME]; /* Host server name */
    char dsk_srv[MAXSERVNAME];  /* Disk server name */
    int tpwfiles;        /* Number of tpwrite files */
    int tprfiles;        /* Number of tpread files  */
    int tpwtime;         /* Time spent for tpwrite commands */
    int tprtime;        /* Time spent for tpread commands */
    int tpwsize;       /* Size of tpwrite files   */
    int tprsize;       /* Size of tpread files    */
    int tpwfiles_concatenated;        /* Number of concatenated tpwrite files */
    int tprfiles_concatenated;        /* Number of concatenated tpread files  */
    int tpwsize_concatenated;       /* Size of concatenated tpwrite files   */
    int tprsize_concatenated;       /* Size of concatenated tpread files    */
    struct ifce_stats *next;
    struct ifce_stats *prev;
};
struct ifce_stats *ifce_first = NULL;
struct ifce_stats *ifce_prev = NULL;

static int totMB = 0;
static int totMBw = 0;
static int totMBr = 0;

static int first_rec = 1;

struct unknown_errs {		/* structure to store unknown messages and details */
    char dgn[MAXLEN];		/* device group */
    char type[9];		/* device type */
    char *message;		/* error message */
    int num_occs;		/* number of occurences */
    char server_name[MAXSERVNAME];/* server */
    struct unknown_errs *next;	/* pointer to next record */
};
struct unknown_errs *unknown_first = NULL;
struct unknown_errs *unknown_prev = NULL;


struct tapeseq   /* structure to store a tape TPPOSIT and RTCOPY completion sequence in order to calculate the actual transfer time*/ 
{
    struct accttape taperec; 
    struct acctrtcp rtcprec;
    time_t tpposit_timestamp;    /* time of tpposit record */
    time_t complete_timestamp;   /*time of rtcopy partial completion record */
    struct tapeseq *next;
    struct tapeseq *prev;
    char server_name[MAXSERVNAME];
    int job_finished;
};

struct tapeseq *tpseq_first = NULL;
struct tapeseq *tpseq_last = NULL;
struct tapeseq *tpseq_prev = NULL;

struct processed_error /* structure to store all the job which error have already been processed */
{
    int jid;
    struct processed_error *next;
};
struct processed_error *processed_error_first = NULL;

struct users /*Structure to store users statistics */
{
    char name[9];
    char groupname[9];	
    int readops; /* Number of tpread commands done by user */
    int writeops; /* Number of tpwrite commands done by user */
    int MBwrite;
    int MBread;
    int totDrvSecs;
    struct users *next;
    struct users *prev;
};
struct users *users_first = NULL;
struct users *sorted_users_first = NULL;

struct groups /* Structure to store user groups statistics */
{
    char name[9];
    int readops; /* Number of tpread commands done by group */
    int writeops; /* Number of tpwrite commands done by group */
    int MBwrite;
    int MBread;
    int totDrvSecs;
    struct groups *next;
    struct groups *prev;
};
struct groups *groups_first = NULL;
struct groups *sorted_groups_first = NULL;

struct volumes /* Structure to store tape volume statistics */
{
    char vid[7];
    char unm[9];
    char server[MAXSERVNAME];
    char reqtype;
    time_t mnt_time;
    time_t rls_time;
    int nbReused;
    int nbFiles;
    int MBytes;
    int last_jid;
    struct volumes *next;
    struct volumes *prev;
};
struct volumes *volList = NULL;
struct volumes *activeVolList = NULL;

int tapes_read = 0;
int tapes_written = 0;

int num_lines = 0;		/* number of lines in error messages file */
int num_tperrs = 0;		/* number of tape errors */
int vflag = 0;                  /* set if verbose requested */
int chk_vol _PROTO((char *, Cregexp_t **));
void clear_store _PROTO(());
void create_devrec _PROTO((char *));
#if defined(VDQM)
void create_dg_s_list _PROTO((int, int, char *, char *));
#endif /* VDQM */
void create_dg_s_list_from_conffile _PROTO((int, int, char *, char *));
void create_failed _PROTO((struct requests *, struct server *));
void update_netstats _PROTO((char *));
void create_netstats _PROTO((struct tapeseq *, char *));
void create_tapeseq _PROTO((struct accttape *, time_t, struct server *));
void match_tapeseq _PROTO((struct acctrtcp *, time_t, struct server *));
void check_tapeseq _PROTO((int, char *));
struct volumes *find_activeVid _PROTO((char *));
void update_vidList _PROTO((char *, int, struct accttape *, struct acctrtcp *, struct requests *));
void print_vidList _PROTO((void));
void clear_netstats _PROTO((void));
void clear_tapeseq _PROTO((void));
void create_reqrec _PROTO((struct acctrtcp *, time_t, struct server *));
void create_servrec _PROTO((char *, char *));
void create_type _PROTO((void));
void create_unknown _PROTO((struct requests *, char * , struct server *));
time_t cvt_datime _PROTO((char *));
void delete_reqrecord _PROTO((struct requests *));
void delete_storerecord _PROTO((struct store_dgn *));
int repair_rec _PROTO((int, struct accthdr *, int, int *));
int getacctrec _PROTO((int, struct accthdr *, char *, int *));
void get_dgn _PROTO((struct requests *, struct server *));
int get_statcode _PROTO((struct requests *));
void insert_stats _PROTO((struct requests *, struct server *, int));
void insert_type _PROTO((char *, struct server *));
int match_reqerr _PROTO((struct acctrtcp *, struct server *));
void match_reqrec _PROTO((
                         struct acctrtcp *,
                         time_t,
                         struct server *,
                         int,
                         char *,
                         struct requests **
                         ));
void print_vidInfo _PROTO((void));
void print_netstats _PROTO((void));
void print_stats _PROTO((time_t, time_t, int, int, int, char *));
void print_tape_errs _PROTO((struct tp_sort *));
void print_time_val _PROTO((time_t));
void print_time_interval _PROTO((time_t, time_t));
void read_errmsg _PROTO((struct requests *, struct server *));
void read_mess2mem _PROTO((void));
void read_tpfile _PROTO((void));
void get_ifce_list _PROTO((struct server *));
void sort_netstats _PROTO((struct server *));
void sort_fill _PROTO((struct server *));
void sort_servlist _PROTO((void));
void sort_tperrs _PROTO((void));
void store_dgn _PROTO((struct accttape *));
void swap_fields _PROTO((struct accttape *));
void swap_fields2 _PROTO((struct acctrtcp *));
int comp _PROTO((struct tp_sort *, struct tp_sort *));
int comp2 _PROTO((struct tp_sort *, struct tp_sort *));
void usage _PROTO((char *));
void store_usrstats _PROTO((void));
void sort_usrstats _PROTO((void));
void print_usrstats _PROTO((void));
void clear_requests _PROTO((void));


time_t starttime = 0;	/* time to start reading records from */
time_t endtime = 0; /* end time */

int main (argc, argv) 
int argc;
char **argv;
{
    char acctdir[80];		/* Accounting directory name and path */
    char acctfile[80];		/* accounting file path and name */
    char acctweek[80];		/* week number.. optional */
    char acctfile2[80];		/* input accounting file */
    struct accthdr accthdr;	/* accthdr record */
    char buf[SACCT_BUFLEN];	/* buffer */
    int c;			/* return from getopt function */
    time_t current_time;	/* current time */
    time_t cvt_datime();	/* date function */
    char dev_group[MAXDGP];	/* device group name */
    int eflag = 0;		/* set if endtime given */
    int errflg = 0;		/* error flag */
    int fd_acct;		/* file descriptor */
    int gflag = 0;		/* set if device group specified */
    int i;			/* counter */
    static struct Coptions longopts[] = {
        {"acctdir", REQUIRED_ARGUMENT, 0, 'A'},
        {"allgroups", NO_ARGUMENT, &allgroups, 1},
        {0, 0, 0, 0}
    };
    int nrec = 0;		/* number of records read */
    int num_serv_errs = 0;	/* number of servers on which open failed */
    struct accttape *rp;	/* pointer to accttape record */
    struct acctrtcp *rtcp;	/* pointer to acctrtcp record */
    struct requests *req;
    char server_errors[MAXSERVS][9];/* list of servers on which open failed */
    int server_failed = 0;	/* set if server open has failed */
    struct server *serv_list;	/* pointer to server structure */
    char server_name[MAXSERVNAME];/* server name if specified */
    int Sflag = 0;		/* set if server specified */
    int sflag = 0;		/* set if starttime specified */
    int sub_time = 0;		/* seconds to subtruct */
    int swapped = 0;		/* set if byte order needs swapping */
    int dflag = 0;                /* set if debug */
    int wflag = 0;		/* set if week number given */
    int Vflag = 0;      /* set if vid info dump is requested */
    int use_conffile = 0;         /* Use configuration file rather than VDQM */
    struct tms timestart, timeend;
    time_t realtime0, realtime1;
    
    long                  clktck = -1;
    long rec_num = 0;
    
    struct tapeseq *tpseq;
    
    /* set acctfile = NULL */
    acctdir[0] = '\0';
    acctfile[0] = '\0';
    acctfile2[0] = '\0';
    acctweek[0] = '\0';
#if !defined(VDQM)
    use_conffile++;
#endif /* VDQM */
    
    /* Read in command line options */
    
    Copterr = 1;
    Coptind = 1;
    while ((c = Cgetopt_long (argc, argv, "cde:f:g:S:s:vw:V:", longopts, NULL)) != EOF) {
        switch (c) {
        case 'A':                 /* Accounting directory */
            strcpy(acctdir, Coptarg);
            break;
        case 'c':
            use_conffile++;
            break; 
        case 'd':
            dflag++;
#ifndef _WIN32
#ifdef _SC_CLK_TCK
            /* Clock ticks per second */
            if ((clktck = sysconf(_SC_CLK_TCK)) < 0) {
                fprintf(stderr,"### sysconf error (%s)\n",strerror(errno));
                exit(EXIT_FAILURE);
            }
#else
            fprintf(stderr,"### No _SC_CLK_TCK defined. Cannot get clock ticks per second.\n");
            exit(EXIT_FAILURE);
#endif
#endif
            break;
        case 'e':		/* endtime */
            if ((endtime = cvt_datime (Coptarg)) < 0) {
                fprintf (stderr, "incorrect time value %s\n", Coptarg);
                errflg = 1;
            }
            eflag++;
            break;
        case 'f':		/* accounting file */
            strcpy (acctfile2, Coptarg);
            break;
        case 'g':		/* device group */
            strcpy (dev_group, Coptarg);
            gflag++;
            break;
        case 'S':		/* server */
            strcpy (server_name, Coptarg);
            Sflag++;
            break;
        case 's':
            if (strncmp (Coptarg, "-", 1) == 0) {
                sub_time = atoi (Coptarg+1);
                time (&current_time);
                if ((starttime = current_time - sub_time) <= 0) {
                    fprintf (stderr, "starttime is a not valid %d\n", starttime);
                    errflg = 1;
                }
            }
            else if ((starttime = cvt_datime (Coptarg)) < 0) {
                fprintf (stderr, "incorrect time value %s\n", Coptarg);
                errflg = 1;
            }
            sflag++;
            break;
        case 'v':
            vflag++;
            break;
        case 'V':
            if ((chk_vol(Coptarg, &expstruct)) == 0)
                errflg++;
            else {
                Vflag++;
                printf("Volumes search pattern=%s\n", Coptarg);
            }
            break;
        case 'w':
            strcpy (acctweek, Coptarg);
            wflag++;
            break;
        case '?':
            errflg = 1;
            break;
        }
    }
    
    /* if invalid options or additional parameters given exit program with message */
    
    if (argc > Coptind) {	/* additional parameters */
        fprintf (stderr, "Additional parameters given\n");
        errflg = 1;
    }
    
    read_mess2mem();/* read error messages into memory */
    
    /* create lists of required servers and device groups */
    
    if ( use_conffile ) 
        (void)create_dg_s_list_from_conffile (gflag, Sflag, dev_group, server_name);
    else
        (void)create_dg_s_list (gflag, Sflag, dev_group, server_name);
    
    if (gflag && dev_first == NULL) {
        fprintf (stderr, "Incorrect device group name given :  %s\n", dev_group);
        errflg = 1;
    }
    else if (Sflag && serv_first == NULL) {
        fprintf (stderr, "Incorrect server given : %s\n", server_name);
        errflg = 1;
    }
    
    if (errflg) {			
        usage (argv[0]);
        exit (1);
    }
    
    /* Determine the type of each device group by reading the TPCONFIG file and */
    /* extracting the type field for each given device group. Create list of types */
    /* sort server list into device type alphanumeric order */
    
    read_tpfile ();
    create_type();
    sort_servlist();
    
    /* For each server in the list open the accounting file and read in the data */
    
    serv_list = serv_first;
    
    while (serv_list) {
        if (! *acctdir) {
            strcpy (acctfile, serv_list->server_name);
            strcat (acctfile, ":");
            if (acctfile2[0] != '\0')
                strcat (acctfile, acctfile2);
            else
                strcat (acctfile, ACCTFILE);
        } else {
            strcpy(acctfile, acctdir);
            strcat(acctfile, "/");
            strcat(acctfile, serv_list->server_name);
            strcat(acctfile, "/sacct");
        }
        if (wflag) {
            strcat (acctfile, ".");
            strcat (acctfile, acctweek);
        }
        
        server_failed = 0;
        
        if (vflag)
            fprintf (stderr, "\nopening file : %s\n", acctfile);
        first_rec = 1;
        swapped = 0;
        if ((fd_acct = rfio_open (acctfile, O_RDONLY)) < 0) 
        {
            strcpy(serv_list->failure_msg, rfio_serror());
            fprintf (stderr, "%s : open error : %s\n", acctfile, serv_list->failure_msg);
            server_failed = 1;
            serv_list->failed = 1; 
            
            strcpy (server_errors[num_serv_errs], serv_list->server_name);
            num_serv_errs++;
        }
        nrec = 0;
        if (! server_failed) 
        {
            while (getacctrec (fd_acct, &accthdr, buf, &swapped)) { 
                
                if (dflag) {
                    times(&timestart);
                    time(&realtime0);
                }
                if (accthdr.package != ACCTRTCOPY && accthdr.package != ACCTTAPE) 
                    continue;
                
                nrec++;
                
                if (!starttime &&nrec == 1) starttime = accthdr.timestamp;
                if (!endtime && nrec == 1) endtime = accthdr.timestamp;
                if (accthdr.timestamp < starttime) {
                    if (!sflag) starttime = accthdr.timestamp;
                    else continue;
                }
                if (endtime && accthdr.timestamp > endtime) {
                    if (!eflag) endtime = accthdr.timestamp;
                    else continue;
                }
                
                if (accthdr.package == ACCTTAPE)
                {
                    rp = (struct accttape *) buf;	
                    if (swapped) 
                    {
                        swap_fields (rp);
                    }	
                    if (rp->subtype == TPASSIGN)       
                    {
                        update_vidList(serv_list->server_name,accthdr.timestamp,
                            rp,NULL,NULL);
                        store_dgn (rp);
                    }
                    
                    if ( rp->subtype == TPMOUNTED )
                    {
                        update_vidList(serv_list->server_name,accthdr.timestamp,
                            rp,NULL,NULL);
                    }
                    
                    if (rp->subtype == TPPOSIT)
                    {
                        create_tapeseq(rp, accthdr.timestamp,serv_list);
                    }
                    
                    if ( rp->subtype == TPUNLOAD )
                    {
                        update_vidList(serv_list->server_name,accthdr.timestamp,
                            rp,NULL,NULL);
                    }
                }
                
                else
                {
                    
                    rtcp = (struct acctrtcp *) buf;
                    if (swapped) {
                        swap_fields2 (rtcp);
                    }  
                    
                    if (rtcp->subtype == RTCPCMDR) 
                    {           	
                        create_reqrec (rtcp, accthdr.timestamp, serv_list);
                    }
                    
                    else if (rtcp->subtype == RTCPCMDC || rtcp->subtype == RTCPPRC)
                    {	      
                        match_reqrec (rtcp, accthdr.timestamp, serv_list, gflag , dev_group,
                            &req);
                        if ( rtcp->subtype == RTCPPRC ) {
                            totMB += rtcp->size/1024;
                            if ( rtcp->reqtype == 'R' || 
                                rtcp->reqtype == 'r' ) totMBr += rtcp->size/1024;
                            if ( rtcp->reqtype == 'W' ||
                                rtcp->reqtype == 'w' ) totMBw +=  rtcp->size/1024;
                            update_vidList(serv_list->server_name,accthdr.timestamp,
                                NULL,rtcp,req);
                        }
                        update_vidList(serv_list->server_name,accthdr.timestamp,
                            NULL,NULL,req);
                    }
                    
                    else if (rtcp->subtype == RTCPEMSG) 
                    {	      
                    /*   fprintf (stdout, "Erreur pour  %d  ,",rtcp->jid);
                        fprintf(stdout, "le message est : %s\n",rtcp->errmsgtxt);*/
                        
                        match_reqerr (rtcp, serv_list);            
                    }
                }
                
                if (dflag) {
                    times(&timeend);
                    time(&realtime1);
                    fprintf(stderr,"%d rec - Real/User/Sys Time : %7.2f %7.2f %7.2f\n",rec_num,(realtime1 - realtime0) / (double) clktck, (timeend.tms_utime - timestart.tms_utime) / (double) clktck,(timeend.tms_stime - timestart.tms_stime) / (double) clktck );
                    rec_num++;
                }        
                
                
        }
        
        if ( Vflag ) print_vidInfo();
        
        update_netstats(serv_list->server_name);
        clear_tapeseq();
        serv_list->netstats_first = ifce_first;
        store_usrstats(); 
        clear_requests();
        ifce_first = NULL;
        clear_store();
        rfio_close (fd_acct);
      }		
      serv_list = serv_list->next;
    }
    
    if ( !Vflag ) {
        print_stats (starttime, endtime, gflag, Sflag, wflag, acctweek); 
        
        if (tape_first != NULL)
            sort_tperrs();
        print_usrstats();
        
        if (num_serv_errs > 0) {
            fprintf (stderr, "\n\n");
            for (i = 0; i < num_serv_errs; i++) 
                fprintf (stderr, "Server %s failed, no information read\n",
                server_errors[i]);
        }
    } else print_vidList();
    
    if (errflg) exit(2);
    else        exit(0);
}

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


/*******************************************************/
/* Function to clear jid and dgn store for each server */
/*******************************************************/

void clear_store()
{
    struct store_dgn *sd;	/* pointer to store_dgn record */
    struct store_dgn *sd2;	/* pointer to store_dgn record */
    
    sd = store_first;
    while (sd) {
        sd2 = sd->next;
        free (sd);
        sd = sd2;
    }
    store_first = NULL;
    store_prev = NULL;
    return;
}

/***************************************/
/* Function to create a dev_grp record */
/***************************************/

void create_devrec(d)
char *d;
{
    int dev_found = 0;          /* flag set if device found */
    struct dev_grp *dt;         /* pointer to device types record */
    
    /* create device records for each device group that is not already in list */
    
    dt = dev_first;
    if (dt == NULL) {
        dt = (struct dev_grp *) calloc (1, sizeof (struct dev_grp));
        strcpy (dt->dgn, d);
        dt->err_messages = (int *) calloc (num_lines, sizeof (int));
        dt->next = NULL;
        dev_first = dt;
        dev_prev = dt;
    }
    else {
        while (dt) {
            if (strcmp (d, dt->dgn) == 0) {
                dev_found = 1;
                break;
            }
            dt = dt->next;
        }
        if (!dev_found) {
            dt = (struct dev_grp *) calloc (1, sizeof (struct dev_grp)) ;
            strcpy (dt->dgn, d);
            dt->err_messages = (int *) calloc (num_lines, sizeof (int));
            dt->next = NULL;
            dev_prev->next = dt;
            dev_prev = dt;
        }
    }
    return;
}

#if defined(VDQM)
/*********************************************************/
/* Function to create lists of device groups and servers */
/* using VDQM.                                           */
/*********************************************************/

void create_dg_s_list (gflag, Sflag, dev_group, server_name)
int gflag;
int Sflag;
char *dev_group;
char *server_name;
{
    char buf[128];              /* buffer */
    char devgroup[MAXDGP];      /* current device group name */
    int rc = 0;
    vdqmnw_t *nw = NULL;
    vdqmDrvReq_t drvreq;
    int last_id = 0;
    int found = 0;
    
    /* open and read configuration file and extract the names of all devices currently */
    /* in use, read the device group names into device_types list , and server names */
    /* into server list . If device group specified on create record for that group */
    /* and for the servers on which they run.  If server specified only create records */
    /* for that server and the devices which run on it */
    
    do {
        memset(&drvreq,'\0',sizeof(drvreq));
        rc = vdqm_NextDrive(&nw,&drvreq);
        if ( rc != -1 && *drvreq.server != '\0' && *drvreq.drive != '\0' &&
            drvreq.DrvReqID != last_id ) {
            
            last_id = drvreq.DrvReqID;
            if (strcmp (drvreq.dgn, "CT1") == 0) continue;
            if (gflag && (strcmp (drvreq.dgn, dev_group)) != 0) continue;
            
            if (!Sflag) {
                create_devrec(drvreq.dgn);
            }
            
            strcpy (devgroup, drvreq.dgn);
            if (Sflag && (strcmp (drvreq.server, server_name)) != 0) continue;
            
            if (Sflag) {
                create_servrec(drvreq.server, devgroup);
                create_devrec(devgroup);
            }
            else {
                create_servrec(drvreq.server, devgroup);
            }
        }
    } while ( rc != -1 );
    return;
}
#endif /* VDQM */

/*********************************************************/
/* Function to create lists of device groups and servers */
/* using a configuration (/etc/shift.conf) file. This    */
/* used to be the way to get tape server names in SHIFT  */
/* whereas in CASTOR we rather use VDQM. However, let's  */
/* keep the option to use a configuration file so that   */
/* information about tape servers that doesn't exist any */
/* more can still be queried.                            */
/*********************************************************/

void create_dg_s_list_from_conffile (gflag, Sflag, dev_group, server_name)
int gflag;
int Sflag;
char *dev_group;
char *server_name;
{
    char buf[128];              /* buffer */
    char *d;                    /* token pointer */
    char devgroup[MAXDGP];      /* current device group name */
    FILE *fp;                   /* file pointer */
    
    /* open and read configuration file and extract the names of all devices currently */
    /* in use, read the device group names into device_types list , and server names */
    /* into server list . If device group specified on create record for that group */
    /* and for the servers on which they run.  If server specified only create records */
    /* for that server and the devices which run on it */
    
    if ((fp = fopen (CONFIGFILE, "r")) != NULL ) {
        fgets (buf, sizeof (buf), fp);
        do {
            d = strtok (buf, " \t\n");
            if (strcmp (d, "TPSERV") == 0 || strcmp (d, "TPSERVR") == 0 ||
                strcmp (d, "TPSERVW") == 0) {
                d = strtok (NULL, " \t");
                if (strcmp (d, "CT1") == 0) continue;
                if (gflag && (strcmp (d, dev_group)) != 0) continue;
                
                if (!Sflag) {
                    create_devrec(d);
                }
                
                strcpy (devgroup, d);
                d = strtok (NULL, " \t\n");
                while (d) {
                    if (Sflag && (strcmp (d, server_name)) != 0) {
                        d = strtok (NULL, " \t\n");
                        continue;
                    }
                    
                    if (Sflag) {
                        create_servrec(d, devgroup);
                        create_devrec(devgroup);
                    }
                    else {
                        create_servrec(d, devgroup);
                    }
                    
                    d = strtok (NULL, " \t\n");
                }
            }
        } while (fgets (buf, sizeof (buf), fp) != NULL);
    }
    fclose (fp);
    return;
}

/*****************************************************************************/
/* Function to create list of failed requests with no matching error message */
/*****************************************************************************/

void create_failed (rq, server)
struct requests *rq;
struct server *server;
{
    struct failed *fd;		/* pointer to failed sturcture */
    
    fd = fail_first;
    if (fd == NULL) {
        fd = (struct failed *) calloc (1, sizeof (struct failed));
        fd->jid = rq->jid;
        strcpy (fd->server, server->server_name);
        strcpy (fd->dgn, rq->dgn);
        strcpy (fd->type, rq->type);
        fd->complete_time = rq->complete_time;
        fd->next = NULL;
        fail_first = fd;
        fail_prev = fd;
    }
    else {
        fd = (struct failed *) calloc (1, sizeof (struct failed));
        fd->jid = rq->jid;
        strcpy (fd->server, server->server_name);
        strcpy (fd->dgn, rq->dgn);
        strcpy (fd->type, rq->type);
        fd->complete_time = rq->complete_time;
        fd->next = NULL;
        fail_prev->next = fd;
        fail_prev = fd;
    }
    return;
}
/***********************************************/
/* Functions to update the  network statistics */
/***********************************************/

void update_netstats (serv_name)
char *serv_name ;
{
    struct ifce_stats *ifce;	/* pointer to stats record */
    struct tapeseq *tpseq;
    int found;
    
    ifce = ifce_first;
    tpseq = tpseq_first; 
    
    
    if (tpseq_first == NULL) 
    {
        return;
    }
    
    tpseq = tpseq_first;
    
    while(tpseq)
    {
        found = 0;
        ifce = ifce_first; 
        
        while ( (strcmp(tpseq->rtcprec.ifce, "???") == 0) || (strcmp(tpseq->rtcprec.ifce, "") == 0) || (strcmp(tpseq->rtcprec.ifce, " ") == 0) || ( tpseq->rtcprec.reqtype == 'd' ) || ( tpseq->rtcprec.reqtype == 'D') || ( tpseq->job_finished == -1) )
        {
            if(tpseq->next != NULL)
            {
                tpseq = tpseq->next;
            }
            else
            { 
                return;
            }
        }
        
        while(ifce)
        {
            
            if ( (strcmp(ifce->name, tpseq->rtcprec.ifce) == 0) && (strcmp(ifce->dsk_srv, tpseq->rtcprec.dsksrvr) == 0) )
            { 
                
                found = 1;
                if ( ( tpseq->rtcprec.reqtype == 'R' ) || ( tpseq->rtcprec.reqtype == 'r') )
                { 
                    ifce->tprfiles++;
                    ifce->tprtime = ifce->tprtime + (tpseq->complete_timestamp - tpseq->tpposit_timestamp);
                    ifce->tprsize = ifce->tprsize + tpseq->rtcprec.size;
                }
                
                else if (tpseq->rtcprec.reqtype == 'W' || tpseq->rtcprec.reqtype == 'w')
                { 
                    ifce->tpwfiles++;
                    ifce->tpwtime = ifce->tpwtime + (tpseq->complete_timestamp - tpseq->tpposit_timestamp) ;
                    ifce->tpwsize = ifce->tpwsize + tpseq->rtcprec.size;
                    
                }
                
                else fprintf(stderr, "Neither tpread nor tpwrite\n");
                
                
            }
            ifce = ifce->next;
        }
        if (found == 0)
        {
            
            ifce = ifce_first;
            while(ifce)
            {    
                ifce = ifce->next;
            }
            create_netstats(tpseq, serv_name);
            
        }
        tpseq = tpseq->next;   
    }
    return;
}


/******************************************/
/* Functions to create network statistics */
/******************************************/

void create_netstats(tpseq, serv_name)
struct tapeseq *tpseq;
char *serv_name;

{
    struct ifce_stats *ifce;	/* pointer to stats record */
    struct ifce_stats *new_ifce ;
    
    ifce = ifce_first;
    if (ifce == NULL) 
    {
        ifce = (struct ifce_stats *) calloc (1, sizeof (struct ifce_stats));
        ifce->prev = NULL;
        ifce_first = ifce;
        ifce_prev = ifce;
        strcpy(ifce->name, tpseq->rtcprec.ifce);
        strcpy(ifce->dsk_srv, tpseq->rtcprec.dsksrvr);
        strcpy(ifce->host_srv, serv_name);
    }
    
    else 
    {      
        while(ifce->next)
        {
            ifce = ifce->next;
        }
        
        new_ifce = (struct ifce_stats *) calloc (1, sizeof (struct ifce_stats));
        new_ifce->prev = ifce;
        new_ifce->next = NULL;
        ifce->next = new_ifce;
        strcpy(new_ifce->name, tpseq->rtcprec.ifce);
        strcpy(new_ifce->dsk_srv, tpseq->rtcprec.dsksrvr);
        strcpy(new_ifce->host_srv, serv_name);
        ifce = new_ifce;
    }
    
    if ( (tpseq->rtcprec.reqtype == 'R') ||  ( tpseq->rtcprec.reqtype == 'r') )
    {
        ifce->tprfiles = 1;
        ifce->tprtime = (tpseq->complete_timestamp - tpseq->tpposit_timestamp);
        ifce->tprsize =  tpseq->rtcprec.size;
        
        ifce->tpwfiles = 0;
        ifce->tpwtime = 0;
        ifce->tpwsize = 0;
    }
    else if ( (tpseq->rtcprec.reqtype == 'W') ||  ( tpseq->rtcprec.reqtype == 'w') )
    {
        ifce->tpwfiles = 1;
        ifce->tpwtime = (tpseq->complete_timestamp - tpseq->tpposit_timestamp);
        ifce->tpwsize =  tpseq->rtcprec.size;
        
        ifce->tprfiles = 0;
        ifce->tprtime = 0;
        ifce->tprsize = 0;  
    }
    else fprintf(stderr, "rtcp type not recognized\n");
    
    return;
}


/********************************************/
/* Functions to create tape sequence record */
/********************************************/

void create_tapeseq(tppositrec, timestamp, serv)
struct accttape *tppositrec;
time_t timestamp;
struct server *serv;
{
    struct tapeseq *tpseq;
    
    
    if (tpseq_first == NULL) 
    {
        tpseq = (struct tapeseq *) calloc (1, sizeof (struct tapeseq));
        tpseq->prev = NULL;
        tpseq_first = tpseq;
        tpseq_prev = tpseq;
        
        memcpy(&(tpseq->taperec), tppositrec, sizeof(struct accttape));
        strcpy(tpseq->server_name, serv->server_name);
        tpseq->rtcprec.jid = 0;
        tpseq->tpposit_timestamp = timestamp;
        tpseq->job_finished = 0;
        tpseq_last = tpseq;
        return;
    }
    
    else /* append the new tape sequence to the last one */
    {
        tpseq = tpseq_first;
        while (tpseq->next)
        {
            tpseq = tpseq->next;
        }
        
        tpseq = (struct tapeseq *) calloc (1, sizeof (struct tapeseq));
        tpseq->prev = tpseq_last;
        tpseq->prev->next = tpseq;
        memcpy(&(tpseq->taperec), tppositrec, sizeof(struct accttape));
        strcpy(tpseq->server_name, serv->server_name);
        tpseq->rtcprec.jid = 0;
        tpseq->job_finished = 0;
        tpseq->tpposit_timestamp = timestamp;
        tpseq_last = tpseq;
        
    }
    return;
}

/*******************************************/
/* Functions to match tape sequence record */
/*******************************************/

void match_tapeseq(partial_completed_rec, timestamp, serv)
struct acctrtcp *partial_completed_rec;
time_t timestamp;
struct server *serv;
{
    struct tapeseq *tpseq;
    struct ifce_stats *ifce;
    int found = 0;
    
    
    if (tpseq_first == NULL)
    {
        /* No TPPOSIT to match for this jid */
        return; 
    }
    tpseq = tpseq_last;
    while (tpseq) 
    {
        if ((tpseq->taperec.jid == partial_completed_rec->jid) &&(tpseq->rtcprec.jid == 0) && (strcmp(tpseq->server_name, serv->server_name) == 0)) 
        {
            found = 1;
            memcpy(&tpseq->rtcprec, partial_completed_rec, sizeof(struct acctrtcp));
            tpseq->complete_timestamp = timestamp;  
            /* Partial completion matched for this jid */     
            return;
        }
        tpseq = tpseq->prev;
    }
    if (found == 0)
    {
        ifce = ifce_first;
        while (ifce)
        {
            if ( (strcmp(partial_completed_rec->ifce,ifce->name) == 0) && (strcmp(partial_completed_rec->dsksrvr, ifce->dsk_srv) == 0)) 
            {
                if ((partial_completed_rec->reqtype == 'R') || (partial_completed_rec->reqtype == 'r')) 
                {
                    ifce->tprfiles_concatenated++;
                    ifce->tprsize_concatenated = ifce->tprsize_concatenated + partial_completed_rec->size; 
                }
                else if ((partial_completed_rec->reqtype == 'W') || (partial_completed_rec->reqtype == 'w'))
                {
                    ifce->tpwfiles_concatenated++;
                    ifce->tpwsize_concatenated = ifce->tpwsize_concatenated + partial_completed_rec->size; 
                }
            }
            ifce = ifce->next;
        }
        
        /*No matching TPPOSIT for jid %d: file maybe concatenated*/
    }
    return;
}
/*******************************************************************/
/* Functions to update a tape sequence when a job is finished      */
/*******************************************************************/

void check_tapeseq(jid, serv_name)
int jid;
char *serv_name ;
{
    struct tapeseq *tpseq;
    int all_jobs_finished = 1;
    
    /* flag all the tapeseq which job has finished */
    if (tpseq_first == NULL) 
    {
        /* No tapeseq to check */
        return;
    }
    
    
    tpseq = tpseq_last;
    
    while (tpseq)
    {
        if(tpseq->taperec.jid == jid)
        {
            tpseq->job_finished = 1;
        }
        tpseq = tpseq->prev;
    }
    
    /* If all jobs have finished in the tapeseq chain, update the stats */
    tpseq = tpseq_first;
    while (tpseq)
    {
        if (tpseq->job_finished == 0)
        {
            /* a Job is still pending*/
            all_jobs_finished = 0;  
        }
        tpseq = tpseq->next;
    }
    if (all_jobs_finished == 0) 
    {
        /*A job is still pending*/
        return;
    }
    else 
    {
        /* updating stats*/
        update_netstats(serv_name);
        clear_tapeseq();
        /*stats updated and seq cleared*/
    }
    return;
}

struct volumes *find_activeVid(char *vid) {
    struct volumes *vl;
    
    if ( vid == NULL || *vid == '\0' ) return(NULL);
    CLIST_ITERATE_BEGIN(activeVolList,vl) {
        if ( strcmp(vl->vid,vid) == 0 ) return(vl);
    } CLIST_ITERATE_END(activeVolList,vl);
    return(NULL);
}

void update_vidList(
                    char *server,
                    int timestamp,
                    struct accttape *rp,	/* pointer to accttape record */
                    struct acctrtcp *rtcp,	/* pointer to acctrtcp record */
                    struct requests *req
                    ) {
    struct volumes *vl;
    char *vid;
    char reqtype = 0;
    
    if ( rp == NULL && rtcp == NULL && req == NULL ) return;
    
    if ( rp != NULL ) {
        if ( rp->subtype == TPASSIGN ) {
            vl = find_activeVid(rp->vid);
            if ( vl == NULL ) return;
            vl->nbReused++;
        }
        if ( rp->subtype == TPMOUNTED ) {
            vl = find_activeVid(rp->vid);
            if ( vl == NULL ) {
                vl = (struct volumes *)calloc(1,sizeof(struct volumes));
                if ( vl == NULL ) {
                    perror("update_vidList() malloc()");
                    exit(1);
                }
                strcpy(vl->vid,rp->vid);
                strcpy(vl->unm,rp->drive);
                if ( server != NULL ) strcpy(vl->server,server);
                vl->last_jid = rp->jid;
                vl->nbReused = 0;
                CLIST_INSERT(activeVolList,vl);
            }
            if ( !isalnum(*vl->unm) ) strcpy(vl->unm,rp->drive);
            vl->mnt_time = timestamp;
        }
        if ( rp->subtype == TPUNLOAD ) {
            vl = find_activeVid(rp->vid);
            if ( vl == NULL ) {
                vl = (struct volumes *)calloc(1,sizeof(struct volumes));
                if ( vl == NULL ) {
                    perror("update_vidList() malloc()");
                    exit(1);
                }
                strcpy(vl->vid,rp->vid);
                strcpy(vl->unm,rp->drive);
                if ( server != NULL ) strcpy(vl->server,server);
                vl->last_jid = rp->jid;
                vl->nbReused = 0;
                vl->mnt_time = starttime;
                CLIST_INSERT(activeVolList,vl);
            }
            if ( !isalnum(*vl->unm) ) strcpy(vl->unm,rp->drive);
            vl->rls_time = timestamp;
            CLIST_DELETE(activeVolList,vl);
            CLIST_INSERT(volList,vl);
        }
    }
    
    if ( rtcp != NULL || req != NULL ) {
        if ( rtcp != NULL ) vid = rtcp->vid;
        else vid = req->vid;
        vl = find_activeVid(vid);
        if ( vl == NULL ) {
            vl = (struct volumes *)calloc(1,sizeof(struct volumes));
            if ( vl == NULL ) {
                perror("update_vidList() malloc()");
                exit(1);
            }
            strcpy(vl->vid,vid);
            if ( req != NULL ) {
                if ( isalnum(*req->unm) )strcpy(vl->unm,req->unm);
                strcpy(vl->server,req->server);
            }
            vl->mnt_time = -1;
            vl->nbReused = 0;
            CLIST_INSERT(activeVolList,vl);
        }
        
        if ( req != NULL ) reqtype = req->reqtype;
        else reqtype = rtcp->reqtype;
        if ( reqtype == 'R' || reqtype == 'r' || 
            reqtype == 'W' || reqtype == 'w' ) vl->reqtype = reqtype;
        
        if ( rtcp != NULL ) {
            if ( rtcp->size > 0 ) vl->MBytes += (rtcp->size/1024);
            vl->nbFiles++;
        }
    }
    
    return;
}

void print_vidList() {
    struct volumes *vl;
    int i;
    char timestr[64];
    if ( volList != NULL ) {
        printf("\n\n****** Volume requests ******\n");
        printf("VID mode drive@server mountTime onDriveTime #reused #files #MBytes\n");
        CLIST_ITERATE_BEGIN(volList,vl) {
            if ( (vl->mnt_time > 0) && (Cregexp_exec(expstruct, &(vl->vid[0])) == 0) ) {
                strcpy(timestr,ctime((time_t *)&(vl->mnt_time)));
                i = strlen(timestr);
                timestr[i-1] = '\0';
                if ( vl->reqtype != 'r' && vl->reqtype != 'R' &&
                    vl->reqtype != 'w' && vl->reqtype != 'W' &&
                    vl->reqtype != 'd' && vl->reqtype != 'D' )
                    vl->reqtype = '?';
                printf("%s %c %s@%s %s %d %d %d %d\n",
                    vl->vid,vl->reqtype,vl->unm,vl->server,timestr,
                    vl->rls_time-vl->mnt_time,vl->nbReused,vl->nbFiles,
                    vl->MBytes);
            }
        } CLIST_ITERATE_END(volList,vl);
    }
    if ( activeVolList != NULL ) {
        CLIST_ITERATE_BEGIN(activeVolList,vl) {
            if ( (vl->mnt_time > 0) && (Cregexp_exec(expstruct, &(vl->vid[0])) == 0) ) {
                strcpy(timestr,ctime((time_t *)&(vl->mnt_time)));
                i = strlen(timestr);
                timestr[i-1] = '\0';
                if ( vl->reqtype != 'r' && vl->reqtype != 'R' &&
                    vl->reqtype != 'w' && vl->reqtype != 'W' &&
                    vl->reqtype != 'd' && vl->reqtype != 'D' )
                    vl->reqtype = '?';
                printf("%s %c %s@%s %s %d %d %d %d\n",
                    vl->vid,vl->reqtype,vl->unm,vl->server,timestr,
                    endtime-vl->mnt_time,vl->nbReused,vl->nbFiles,
                    vl->MBytes);
            }
        } CLIST_ITERATE_END(activeVolList,vl);
    }
    /*
     *  Force print_netstat to only print global statistics
     */
    serv_first = NULL;
    print_netstats();
    return;
}


/*******************************************/
/* Functions to clear the netstats         */
/*******************************************/

void clear_netstats()
{
    struct ifce_stats *ifce;
    
    ifce = ifce_first;
    
    if (ifce->next == NULL)
    {
        free(ifce);
        ifce_first = NULL;
        return;
    }
    
    while (ifce->next)
    {
        ifce = ifce->next;
    }
    
    
    while(ifce->prev)
    {
        ifce = ifce->prev;
        free(ifce->next);
        ifce->next = NULL;        
    }
    free(ifce);
    ifce_first = NULL;
    
    return;
}



/*******************************************/
/* Functions to clear a tape sequence      */
/*******************************************/

void clear_tapeseq()
{
    struct tapeseq *tpseq;
    
    tpseq = tpseq_first;
    
    if(tpseq == NULL) return;
    
    if (tpseq->next == NULL)
    {
        free(tpseq);
        tpseq_first = NULL;
        return;
    }
    
    while (tpseq->next)
    {
        tpseq = tpseq->next;
    }
    
    
    while(tpseq->prev)
    {
        tpseq = tpseq->prev;
        free(tpseq->next);
        tpseq->next = NULL;        
    }
    free(tpseq);
    tpseq_first = NULL;
    tpseq_last = NULL;
    tpseq_prev = NULL;
    return;
}


/*************************************/
/* Functions to create request record */
/*************************************/

void create_reqrec(rtcp, timestamp, serv)
struct acctrtcp *rtcp;
time_t timestamp;
struct server *serv;
{
    struct requests *rq;	/* pointer to requests record */
    struct group *grp;		/* pointer to group record */
    struct passwd *pwd;       /* pointer to user record */
    
    rq = req_first;
    if (rq == NULL) {
        rq = (struct requests *) calloc (1, sizeof (struct requests));
        rq->prev = NULL;
        rq->complete_time = 0;
        req_first = rq;
        req_prev = rq;
    }
    else {
        rq = (struct requests *) calloc (1, sizeof (struct requests));
        rq->prev = req_prev;
        req_prev->next = rq;
        rq->complete_time = 0;
        req_prev = rq;
    }
    req_last = rq;
    
    rq->next = NULL;
    rq->jid = rtcp->jid;
    
    strcpy(rq->vid, rtcp->vid);
    
    rq->err_lineno = -1;
    
    if ( (pwd = getpwuid (rtcp->uid)) != NULL )
    {
        strcpy (rq->uid, pwd->pw_name);
    }
    else 
    {
        sprintf(rq->uid,"%d",rtcp->uid);
        
    }
    if ( (grp = getgrgid (rtcp->gid)) != NULL )
    {
        strcpy (rq->gid, grp->gr_name);
    }
    else 
    {
        sprintf(rq->gid,"%d",rtcp->gid);
        
    }
    
    strcpy (rq->server, serv->server_name);
    rq->reqtype = rtcp->reqtype;
    
    if (rtcp->subtype == RTCPCMDR) rq->req_time = timestamp;
    /*  else rq->complete_time = timestamp;*/
    
    return;
}

/************************************/
/* Function to create server record */
/************************************/

void create_servrec(d, devgroup)
char *d;
char *devgroup;
{
    int serv_found = 0;		/* set if server already in list */
    struct server *svd;		/* pointer to server datat record */
    
    /* create server_data record for each server that is not already in list */
    
    svd = serv_first;
    if (svd == NULL) {
        svd = (struct server *) calloc (1, sizeof (struct server));
        strcpy (svd->server_name, d);
        strcpy (svd->dgn, devgroup);
        svd->nb_dgns = 1;
        svd->netstats_first = NULL;
        svd->sorted_netstats_first = NULL;
        svd->ifce_list_first = NULL;
        svd->next = NULL;
        serv_first = svd;
        serv_prev = svd;
    }
    else {
        while (svd) {
            if (strcmp (svd->server_name,d) == 0) {
                serv_found = 1;
                break;
            }
            svd = svd->next;
        }
        if (!serv_found) {
            svd = (struct server *) calloc (1, sizeof (struct server));
            strcpy (svd->server_name, d);
            strcpy (svd->dgn, devgroup);
            svd->nb_dgns = 1;
            svd->ifce_list_first = NULL; 
            svd->next = NULL;
            serv_prev->next = svd;
            serv_prev = svd;
        } else if ( strcmp(svd->dgn,devgroup) != 0 && *devgroup != '\0' &&
            *svd->dgn != '\0' ) svd->nb_dgns++;
    }
    return;
}

/**********************************/
/* Function to create type record */
/**********************************/

void create_type()
{
    struct dev_grp *dg;		/* pointer to dev_grp record */
    int found = 0;		/* flag set if record exists */
    struct server *sr;		/* pointer to server record */
    
    struct type *ty;		/* pointer to type record */
    
    dg = dev_first;
    ty = type_first;
    
    /* for each type found in device group list, create a record */
    
    while (dg) {
        if (strcmp (dg->type, "") != 0) {
            if (ty == NULL) {
                ty = (struct type *) calloc (1, sizeof (struct type));
                strcpy (ty->type, dg->type);
                ty->err_messages = (int *) calloc (num_lines, sizeof (int));
                ty->stat.dev_succ = ty->stat.dev_unsucc = ty->stat.usr_succ = ty->stat.usr_unsucc = ty->stat.usr_errors = ty->stat.tape_errors = ty->stat.shift_errors = ty->stat.robot_errors = ty->stat.network_errors = ty->stat.other_errors = ty->stat.unknown_errors = ty->stat.no_mess = 0;
                ty->next = NULL;
                type_first = ty;
                type_prev = ty;
            }
            else {
                ty = type_first;
                found = 0;
                while (ty) {
                    if (strcmp (ty->type, dg->type) == 0) {
                        found = 1;
                        break;
                    }
                    ty = ty->next;
                }
                
                if (!found) {
                    ty = (struct type *) calloc (1, sizeof (struct type));
                    strcpy (ty->type, dg->type);
                    ty->err_messages = (int *) calloc (num_lines, sizeof (int));
                    ty->stat.dev_succ = ty->stat.dev_unsucc = ty->stat.usr_succ = ty->stat.usr_unsucc = ty->stat.usr_errors = ty->stat.tape_errors = ty->stat.shift_errors = ty->stat.robot_errors = ty->stat.network_errors = ty->stat.other_errors = ty->stat.unknown_errors = ty->stat.no_mess = 0;
                    ty->next = NULL;
                    type_prev->next = ty;
                    type_prev = ty;
                }
            }
        }
        dg = dg->next;
    }
    
    sr = serv_first;
    while (sr) {
        dg = dev_first;
        while (dg) {
            if (strcmp (dg->dgn, sr->dgn) == 0) {
                strcpy (sr->type, dg->type);
                break;
            }
            dg = dg->next;
        }
        sr = sr->next;
    }
    return;
}

/****************************************************/
/* Function to create unknown error message  record */
/****************************************************/

void create_unknown (rq, message , serv)
struct requests *rq;
char *message;
struct server *serv;

{
    int found = 0;		/* flag set if record exists */ 
    int len = 0;		/* length of message */
    struct unknown_errs *uk;	/* pointer to unknown error message structure */
    char *end;
    
    end = strchr(message,10);  /*Cutting string to one line */
    if (end != NULL) 
    {
        *end = '\0';
    }
    
    uk = unknown_first;
    if (uk == NULL) {
        uk = (struct unknown_errs *) calloc (1, sizeof (struct unknown_errs));	
        len = strlen (message) + 1;
        uk->message = malloc (len);
        strcpy (uk->message, message);
        strcpy (uk->server_name, rq->server);
        strcpy (uk->dgn, rq->dgn);
        strcpy (uk->type, rq->type);
        uk->num_occs++;
        uk->next = NULL;
        unknown_first = uk;
        unknown_prev = uk;
    }
    else {
        while (uk != NULL) {
        /*
        &&
        (strcmp (uk->dgn, rq->dgn) == 0) &&
        (strcmp (uk->type, rq->type) == 0)
            */
            
            if ((strcmp (uk->message, message) == 0) &&
                (strcmp (uk->server_name, rq->server) == 0) ) {
                uk->num_occs++;
                found = 1;
                break;
            }
            uk = uk->next;
        }
        if (!found) {
            uk = (struct unknown_errs *) calloc (1, sizeof (struct unknown_errs));        
            len = strlen (message) + 1;
            uk->message = malloc (len);
            strcpy (uk->message, message);
            strcpy (uk->server_name, rq->server);
            /*if ( (strcmp(rq->type,"") == 0) || (strcmp(rq->dgn,"") == 0) )
            {
            get_dgn(rq,serv);
            
              }
            */
            strcpy (uk->dgn, rq->dgn);
            strcpy (uk->type, rq->type);
            uk->num_occs++;
            uk->next = NULL;
            unknown_prev->next = uk;
            unknown_prev = uk;
        }
    }
    return;
}

/**************************************************/
/* Function to convert date and time into seconds */
/**************************************************/

time_t
cvt_datime(arg)
char *arg;
{
    time_t current_time;
    static int lastd[12] = {31,29,31,30,31,30,31,31,30,31,30,31};
    int n;
    struct tm t;
    struct tm *tm;
    
    memset ((char *) &t, 0, sizeof(struct tm));
    time (&current_time);                /* Get current time */
    tm = localtime (&current_time);
    n = sscanf (arg, "%2d%2d%2d%2d%2d", &t.tm_mon, &t.tm_mday, &t.tm_hour,
        &t.tm_min, &t.tm_year);
    if (n < 4) return (-1);
    if (n == 4)
        t.tm_year = tm->tm_year;
    else if (t.tm_year >= 0 && t.tm_year <= 37)
        t.tm_year += 100;
    else if (t.tm_year >= 38 && t.tm_year <= 69)
        return (-1);
    if ((t.tm_mon < 1) || (t.tm_mon > 12)) return (-1);
    if ((t.tm_mon == 2) && (t.tm_mday == 29) && (t.tm_year % 4 != 0)) return (-1);
    if ((t.tm_mday < 1) || (t.tm_mday > lastd[t.tm_mon-1])) return (-1);
    t.tm_mon--;
    
#if defined(sun) && ! defined(SOLARIS)
    return (timelocal (&t));
#else
    t.tm_isdst = -1;
    return (mktime (&t));
#endif
}

/****************************************/
/* Function to delete a request record  */
/****************************************/

void delete_reqrecord (rq)
struct requests *rq;
{
    if (rq->prev == NULL && rq->next == NULL) {
        req_first = NULL;
        req_prev = NULL;
        req_last = NULL;
    }
    else if (rq->prev == NULL && rq->next != NULL) {
        req_first = rq->next;
        rq->next->prev = NULL;
    }
    else if (rq->prev != NULL && rq->next == NULL) {
        req_prev = rq->prev;
        req_last = rq->prev;
        rq->prev->next = NULL;
    }
    else if (rq->prev != NULL && rq->next != NULL) {
        rq->prev->next = rq->next;
        rq->next->prev = rq->prev;
    }
    free (rq);
    return;
}

/*******************************************/
/* Function to delete a store_dgn  record  */
/*******************************************/

void delete_storerecord (sd)
struct store_dgn *sd;
{
    if (sd->prev == NULL && sd->next == NULL) {
        store_first = NULL;
        store_prev = NULL;
    }
    else if (sd->prev == NULL && sd->next != NULL) {
        store_first = sd->next;
        sd->next->prev = NULL;
    }
    else if (sd->prev != NULL && sd->next == NULL) {
        store_prev = sd->prev;
        sd->prev->next = NULL;
    }
    else if (sd->prev != NULL && sd->next != NULL) {
        sd->prev->next = sd->next;
        sd->next->prev = sd->prev;
    }
    free (sd);
    return;
}

int repair_rec(fd_acct, accthdr, prev_timestamp, swapped)
int fd_acct;
struct accthdr *accthdr;
int prev_timestamp;
int *swapped;
{
    char buf[1024];
    char c;
    int i, j, rc, skipped;
    time_t now;
    
    time(&now);
    fprintf(stderr,"Trying to skip over corrupted accounting data....\n");
    i=0;
    skipped = sizeof(struct accthdr);
    memcpy(&buf[i],accthdr,sizeof(struct accthdr));
    j = 1;
    while ( i == 0 || (rc = rfio_read(fd_acct,&c,1)) == 1 ) {
        if ( i > 0 ) {
            skipped++;
            buf[j] = c;
        }
        memcpy(accthdr,&buf[i],sizeof(struct accthdr));
        if ( *swapped ) {
            swap_it(accthdr->package);
            swap_it(accthdr->len);
            swap_it(accthdr->timestamp);
        }
        if ( accthdr->package > 255 || accthdr->package <= 0 ||
            accthdr->len > SACCT_BUFLEN || accthdr->len < 0 ||
            accthdr->timestamp <= prev_timestamp || accthdr->timestamp > (int)now ) {
            if ( j >= 1023 ) {
                i=0;
                j = sizeof(struct accthdr);
                rc = rfio_read(fd_acct,&buf[i],sizeof(struct accthdr));
                if ( rc != sizeof(struct accthdr) ) break;
            } else {
                i++;
                j++;
            }
        } else {
            rc = 0;
            break;
        }
    }
    if ( rc == 0 ) fprintf(stderr,"skip of corrupted data successful. %d bytes skipped\n",skipped);
    else  fprintf(stderr,"skip of corrupted data failed after %d bytes: rc=%d\n",skipped,rc);
    
    return(rc);
}

/********************************************************/
/* Function to read in records from the accounting file */
/********************************************************/

int getacctrec (fd_acct, accthdr, buf, swapped)
int fd_acct;
struct accthdr *accthdr;
char *buf;
int *swapped;
{
    int c;
    static int prev_timestamp;
    
    c = rfio_read (fd_acct, accthdr, sizeof (struct accthdr));
    
    if (c != sizeof (struct accthdr)) {
        if (c == 0) return (0);
        if (c > 0) fprintf (stderr, "read returns %d\n", c);
        else fprintf (stderr, "read error : %s\n", strerror(errno));
        exit (2);
    }
    
    /* 
    * if required swap byte order. Should only be changed if this is the
    * first record. 
    */
    
    if (first_rec == 1 && (accthdr->package > 255 || accthdr->package < 0)) {
        *swapped = 1;
    }
    if (*swapped) {
        swap_it (accthdr->package);
        swap_it (accthdr->len);
        swap_it (accthdr->timestamp);
    }
    first_rec = 0;
    if (accthdr->len <= 0 || accthdr->len > SACCT_BUFLEN) {
        fprintf (stderr, "corrupted accounting file: package=%d, len=%d, timestamp=%d, swapped=%d\n",
            accthdr->package,accthdr->len,accthdr->timestamp,*swapped);
        if ( repair_rec(fd_acct,accthdr,prev_timestamp,swapped) == -1 ) return(0);
        fprintf (stderr, "accounting record after repair: package=%d, len=%d, timestamp=%d, swapped=%d\n",
            accthdr->package,accthdr->len,accthdr->timestamp,*swapped);
    }
    prev_timestamp = accthdr->timestamp;
    c = rfio_read (fd_acct, buf, accthdr->len);
    
    if ( c != accthdr->len) {
        if (c >= 0) fprintf (stderr, "read returns %d\n", c);
        else fprintf (stderr, "read error : %s\n", strerror(errno));
        if (c == 0) return (0);
        exit (2);
    }
    
    return(accthdr->len);
}

/********************************/
/* Function to get a stored dgn */
/********************************/

void get_dgn (rq, serv)
struct requests *rq;
struct server *serv;
{
    struct store_dgn *sd;
    struct dev_grp *dg;
    int found = 0;
    
    sd = store_first;
    while (sd) {
        if (sd->jid == rq->jid) {
            strcpy (rq->dgn, sd->dgn);
            strcpy (rq->unm, sd->unm);
            delete_storerecord (sd);
            found = 1;
            break;
        }
        sd = sd->next;
    }
    
    if (found == 0)
    {
        if ( serv->nb_dgns == 1 ) strcpy(rq->dgn,serv->dgn);
        else {
            if ( vflag ) fprintf(stderr,"Unknown DGN: server %s job %d vid %s\n",
                rq->server,rq->jid,rq->vid);
            strcpy(rq->dgn,"UNKN");
        }
    }
    
    dg = dev_first;
    while (dg) {
        if (strcmp (rq->dgn, dg->dgn) == 0 ) {
            strcpy (rq->type, dg->type);
            break;
        }
        dg = dg->next;
    }
    return;
}

/******************************************/
/* Function to get statistics return code */
/******************************************/

int get_statcode (rq)
struct requests *rq;
{
    int code;		/* code for exit status */
    
    if (!allgroups && (strcmp (rq->gid, "c3") == 0 || strcmp (rq->gid, "ct") == 0)) 
    {
        if (rq->exitcode == 0 || rq->exitcode == 193 || rq->exitcode == 194
            || rq->exitcode == 195 || rq->exitcode == 197)
        {
            code = 7;
            rq->failed = 0;
        }
        else
        {
            code = 8;
            rq->failed = 1;
        }
    }
    else 
    {
        if (rq->exitcode == 0 || rq->exitcode == 193 || rq->exitcode == 194
            || rq->exitcode == 195 || rq->exitcode == 197)
        {
            code = 9;
            rq->failed = 0; 
        }
        else 
        {
            code = rq->exitcode;	
            rq->failed = 1;
        }	
    }
    
    return (code);
} 

/********************************************/
/* Function to insert statistics into lists */
/********************************************/

void insert_stats (rq, sr, errs)
struct requests *rq;
struct server *sr;
int errs;
{
    int code;			/* code set for statistic evaluation */
    struct dev_grp *dg;		/* pointer to device group record */
    int group_match = 0;	/* set if no group match found */
    struct type *ty;		/* pointer to type record */
    int type_match = 0;		/* set if no type match found */
    
    /* for each list group find then one that matches the current record */
    
    dg = NULL;
    ty = NULL;
    if ((strcmp (rq->dgn, "") == 0) && (errs == 0) ) {
        group_match = 1;
        strcpy (rq->type, sr->type);
    }   
    
    if (!group_match) {
        dg = dev_first;
        while (dg) {
            if (strcmp (dg->dgn, rq->dgn) == 0) 
                break;
            dg = dg->next;
        }
    }
    
    if (dg == NULL)
    {
        group_match = 1;
    }
    
    if (!type_match) {
        ty = type_first;
        while (ty) 
        {
            if (strcmp (ty->type, rq->type) == 0 )
                break;
            ty = ty->next;
        }
    }
    
    if (ty == NULL)
        type_match = 1;
    
    if ( (rq->err_lineno != -1)) 
    {
        
        if( (strcmp(rq->errmsg," ") == 0) ) 
        {
            /*No error message for this jid*/
        }
        else 
            
        {
            sr->err_messages[rq->err_lineno]++;
            if (!group_match)
                dg->err_messages[rq->err_lineno]++;
            if (!type_match)
                ty->err_messages[rq->err_lineno]++;
        }
    }
    
    /* set relevant statistics fields depending on return code */
    
    code = rq->exitcode;
    
    switch (code) {
    case 1:
        global.usr_unsucc++;
        global.usr_errors++;
        sr->stat.usr_unsucc++;
        sr->stat.usr_errors++;
        if (!group_match) {
            dg->stat.usr_unsucc++;
            dg->stat.usr_errors++;
        }
        if (!type_match) {
            ty->stat.usr_unsucc++;
            ty->stat.usr_errors++;
        }
        break;
    case 2:
        global.usr_unsucc++;
        global.tape_errors++;
        sr->stat.usr_unsucc++;
        sr->stat.tape_errors++;
        if (!group_match) {
            dg->stat.usr_unsucc++;
            dg->stat.tape_errors++;
        }
        if (!type_match) {
            ty->stat.usr_unsucc++;
            ty->stat.tape_errors++;
        }
        break;
    case 3:
        global.usr_unsucc++;
        global.shift_errors++;      
        sr->stat.usr_unsucc++;
        sr->stat.shift_errors++;
        if (!group_match) {
            dg->stat.usr_unsucc++;
            dg->stat.shift_errors++;   
        }
        if (!type_match) {
            ty->stat.usr_unsucc++; 
            ty->stat.shift_errors++;   
        }
        break;     
    case 4:
        global.usr_unsucc++;
        global.robot_errors++;      
        sr->stat.usr_unsucc++;
        sr->stat.robot_errors++;
        if (!group_match) { 
            dg->stat.usr_unsucc++;
            dg->stat.robot_errors++;   
        }
        if (!type_match) {
            ty->stat.usr_unsucc++; 
            ty->stat.robot_errors++;   
        }
        break;     
    case 5:
        global.usr_unsucc++;
        global.network_errors++;      
        sr->stat.usr_unsucc++;
        sr->stat.network_errors++;
        if (!group_match) { 
            dg->stat.usr_unsucc++;
            dg->stat.network_errors++;   
        }
        if (!type_match) {
            ty->stat.usr_unsucc++; 
            ty->stat.network_errors++;   
        }
        break;     
    case 6:
        global.usr_unsucc++;
        global.other_errors++;      
        sr->stat.usr_unsucc++;
        sr->stat.other_errors++;
        if (!group_match) { 
            dg->stat.usr_unsucc++;
            dg->stat.other_errors++;   
        }
        if (!type_match){   
            ty->stat.usr_unsucc++;
            ty->stat.other_errors++;   
        }
        break;     
    case 7:
        global.dev_succ++;
        sr->stat.dev_succ++;
        if (!group_match) 
            dg->stat.dev_succ++;
        if (!type_match)
            ty->stat.dev_succ++;
        break;
    case 8:
        global.dev_unsucc++;
        sr->stat.dev_unsucc++;
        if (!group_match) 
            dg->stat.dev_unsucc++;
        if (!type_match )
            ty->stat.dev_unsucc++;
        break;
    case 9:
        global.usr_succ++;
        sr->stat.usr_succ++;
        if (!group_match) 
            dg->stat.usr_succ++;
        if (!type_match)
            ty->stat.usr_succ++;
        break;
    case 10:
        global.usr_unsucc++;
        global.unknown_errors++;
        sr->stat.usr_unsucc++;
        sr->stat.unknown_errors++;
        if (!group_match) {
            dg->stat.usr_unsucc++;
            dg->stat.unknown_errors++;
        }
        if (!type_match) {
            ty->stat.usr_unsucc++;
            ty->stat.unknown_errors++;
        }
        break;
    case 11:
        global.usr_unsucc++;
        global.no_mess++;
        sr->stat.usr_unsucc++;
        sr->stat.no_mess++;
        if (!group_match) {
            dg->stat.usr_unsucc++;
            dg->stat.no_mess++;
        }
        if (!type_match) {
            ty->stat.usr_unsucc++;
            ty->stat.no_mess++;
        }
        break;
  }
  
  
  /*  
  if ( code == 7 || code == 8 || code == 9) 
  {	
  delete_reqrecord (rq);
  }
  */
  
  return;
}

/*********************************************************************/
/* function to insert the device group type into the relevant record */
/*********************************************************************/

void insert_type (p, serv)
char *p;
struct server *serv;
{
    struct dev_grp *dev_list;   /* pointer to device list */
    char *q;
    struct tpconfrec *tprec;    /* tpconfrec record */
    
    /* Read the line into tprec, match dgn and insert type into record */
    
    tprec = (struct tpconfrec *) calloc (1, sizeof (struct tpconfrec));
    sscanf (p, "%s%s%s%s%s%s%s", tprec->unm, tprec->dgn, tprec->dev_name,
        tprec->dens, tprec->instat, tprec->loader, tprec->type);
    if (q = strstr (tprec->type, "/VB")) *q = '\0';
    if (strcmp (tprec->type, "8505") == 0)
        strcpy (tprec->type, "8500");
    
    dev_list = dev_first;
    while (dev_list) {
        if (strcmp (dev_list->dgn, tprec->dgn) == 0 &&
            (strcmp (dev_list->type, "") == 0)) {
            strcpy (dev_list->type, tprec->type);
            break;
        }
        else if (strcmp (dev_list->dgn, tprec->dgn) == 0 &&
            (strcmp (dev_list->type, "") != 0))
            break;
        dev_list = dev_list->next;
    }
    
    strcpy (serv->type, tprec->type);
    
    free (tprec);
    return;
}

/*******************************************/
/* match_reqerr(struct rtcp)               */ 
/* function to match an error to a request */
/*******************************************/

int match_reqerr(rtcp, serv)
struct acctrtcp *rtcp;
struct server *serv;
{
    struct requests *rq = NULL;
    int found = 0;
    
    if(req_last != NULL)  rq = req_last;
    
    while(rq)
    {
        
        if ( (rq->jid == rtcp->jid) && (strcmp(rq->server,serv->server_name) == 0)&& (rq->exitcode != -1))
        {
            strcpy(rq->errmsg,rtcp->errmsgtxt);
            rq->exitcode = -1;
            found = 1;
            break;
        }
        else if ( (rq->jid == rtcp->jid) && (strcmp(rq->server,serv->server_name)) && (rq->exitcode == -1)) 
        {
            found = 1;
            break;
        }
        rq = rq->prev;
    }
    return(found); 
}


/**************************************/
/* Function to match requests records */
/**************************************/

void match_reqrec (rtcp, timestamp, serv, gflag, dev_group, rp)
struct acctrtcp *rtcp;
time_t timestamp;
struct server *serv;
int gflag;
char *dev_group;
struct requests **rp;
{
    int found = 0;		/* flag set if matching record found */
    int skip_insert = 0;	/* set if record deleted */
    struct tapeseq *tpseq;
    struct requests *rq = NULL;

    if ( rp != NULL ) *rp = NULL;
    
    if (rtcp->subtype == RTCPPRC)
    {
        match_tapeseq(rtcp, timestamp, serv);
        /*  return;  */
    }
    
    if (req_first == NULL)
    {
        create_reqrec (rtcp, timestamp, serv);
        rq = req_prev;
        found = 1;
    }
    else
    {
        found = 0; 
        if(req_last != NULL) rq = req_last;
        while (rq) {
            
            if ((rq->jid == rtcp->jid) && (strcmp(serv->server_name,rq->server) == 0)  && (rq->complete_time == 0))
            {
                found = 1;
                break;
            }
            rq = rq->prev;
        }
    }
    
    if (!found) 
    {
        create_reqrec (rtcp, timestamp, serv);
        rq = req_prev;
        return;
    }
    if ( rp != NULL ) *rp = rq;
    
    /*
    if (gflag && strcmp (rq->dgn, dev_group) != 0) 
    {
    delete_reqrecord (rq);
    skip_insert = 1;
    } 
    */
    
    if ( rtcp->subtype == RTCPPRC ) {
        rq->transfer_size += rtcp->size;
    }
    if (rtcp->subtype == RTCPCMDC)
    {         
        
        if (rtcp->exitcode == 205 || rtcp->exitcode == 222) 
        {
            delete_reqrecord (rq);
            return;
        }
        rq->retry = rtcp->retryn;
        rq->complete_time = timestamp;    	 
        if ( rq->req_time > 0 ) 
            rq->transfer_time = rq->complete_time - rq->req_time;
        get_dgn (rq,serv);
        
        if ((rtcp->exitcode != 0) && (rq->exitcode == -1))
        {	
            read_errmsg(rq, serv);
        }
        else if ((rtcp->exitcode == 0) && (rq->exitcode == -1))
        {
            /* A retry occured*/
            rq->exitcode = 0;
        }
        
        rq->exitcode = get_statcode(rq);
        
        if ((rq->exitcode == 7) || (rq->exitcode == 9))
        {
            check_tapeseq(rq->jid, serv->server_name);
            
        }
        else
        {
            tpseq = tpseq_first;
            while (tpseq)
            {
                if ( (tpseq->taperec.jid == rtcp->jid) && (tpseq->taperec.fseq == rtcp->fseq) )
                {
                    if ( tpseq->job_finished != -1) tpseq->job_finished = 1;
                    
                }
                tpseq = tpseq->next;
            }
        }
        
        insert_stats (rq, serv, 0);
        
    }
    
    return;
}

void print_vidInfo() {
    struct requests *rp;
    char strtime[27];
    
    rp = req_first;
    while ( rp != NULL && rp != req_last) {
        if (Cregexp_exec(expstruct, &(rp->vid[0])) == 0) {
            strcpy(strtime, ctime(&rp->req_time));
            strtime[strlen(strtime) - 1] = '\0';
            printf("%s %c %s %s %s %s %s@%s %d %s\n", rp->vid, 
                rp->reqtype, strtime,
                rp->uid, rp->gid, rp->dgn, rp->unm, rp->server,
                rp->complete_time - rp->req_time, 
                (rp->failed ? "failed" : "ok"));
        }
        rp = rp->next;
    }
    return;
}

/********************************************/
/* Function to print out network statistics */
/********************************************/


void print_netstats ()
{
    struct sorted_ifce_stats *ifce;
    struct ifce_list *ifce_list;
    int wspeed, rspeed;
    struct server *serv;	/* pointer to list of all servers to open */
    
    serv = serv_first;
    
    printf("\n\n\n\n\n\n\n\n\n\n");
    printf("\n\t*********************************************\n");
    printf("\t*           Network statistics              *\n");
    printf("\t*********************************************\n\n\n");
    printf("\tTotal nb MBytes transferred: %d (%f TB)\n",totMB,
        (float)totMB/(1024.0*1024.0));
    printf("\tTotal nb MBytes written to tape: %d (%f TB)\n",totMBw,
        (float)totMBw/(1024.0*1024.0));
    printf("\tTotal nb MBytes read from tape: %d (%f TB)\n",totMBr,
        (float)totMBr/(1024.0*1024.0));
    while(serv)
    {
        printf("\n:::::::::::::::::::::::::::::::::::::::::::::\n");
        printf("* Network statistics for server %-8s *\n",serv->server_name);
        printf(":::::::::::::::::::::::::::::::::::::::::::::\n");
        
        if (serv->failed == 1) 
        {
            printf("Server %s failed, no information read\n", serv->server_name);
            printf("  failure message :%s\n", serv->failure_msg);
            serv = serv->next;
            continue;
        }
        
        ifce = serv->sorted_netstats_first;
        
        ifce_list = serv->ifce_list_first;
        
        while(ifce_list)
        {
            printf("\t---------------------------------------------\n");
            printf("\t* interface %-8s *\n",ifce_list->name);
            printf("\t*        (10 most active disk servers)      *\n");
            printf("\t---------------------------------------------\n");
            printf("%-7s %9s %19s %22s %22s\n","Network","DiskSrv","Number_files",
                "Number_Mbytes","Speed(kbyte/s)");
            printf("%-7s %9s (%5s,%5s,%5s) (%6s,%6s,%6s) (%5s,%5s,%5s)\n",
                "Ifce"," ","tpW","tpR","tot","tpW","tpR","tot","tpW","tpR","tot");
            printf("%-7s %9s %19s %22s %22s\n","-----","---------",
                "-------------------","----------------------",
                "----------------------");
            
            while(ifce)
            {   
                if(ifce->netstats != NULL)
                {
                    ifce->netstats->tpwsize = ifce->netstats->tpwsize + ifce->netstats->tpwsize_concatenated;
                    ifce->netstats->tprsize = ifce->netstats->tprsize + ifce->netstats->tprsize_concatenated;
                    printf("%-7s %9s (%5d,%5d,%5d) (%6d,%6d,%6d)",ifce->netstats->name,ifce->netstats->dsk_srv,ifce->netstats->tpwfiles +
                        ifce->netstats->tpwfiles_concatenated, ifce->netstats->tprfiles +
                        ifce->netstats->tprfiles_concatenated,ifce->netstats->tpwfiles +
                        ifce->netstats->tpwfiles_concatenated + ifce->netstats->tprfiles +
                        ifce->netstats->tprfiles_concatenated ,	 (ifce->netstats->tpwsize)/1024,(ifce->netstats->tprsize)/1024,(ifce->netstats->tpwsize + ifce->netstats->tprsize)/1024 );
                    
                    
                    
                    
                    if (ifce->netstats->tprtime == 0 && ifce->netstats->tpwtime != 0)
                        printf(" (%5d,%5d,%5d)\n",(ifce->netstats->tpwsize / (ifce->netstats->tpwtime)),  0,  ((ifce->netstats->tpwsize + ifce->netstats->tprsize) / (ifce->netstats->tpwtime + ifce->netstats->tprtime)))    ;
                    else if (ifce->netstats->tpwtime == 0 && ifce->netstats->tprtime != 0)
                        printf(" (%5d,%5d,%5d)\n",0, (ifce->netstats->tprsize / (ifce->netstats->tprtime)) ,  ((ifce->netstats->tpwsize + ifce->netstats->tprsize) / (ifce->netstats->tpwtime + ifce->netstats->tprtime)))    ;
                    else if (ifce->netstats->tpwtime != 0 && ifce->netstats->tprtime != 0)
                        printf(" (%5d,%5d,%5d)\n", (ifce->netstats->tpwsize / (ifce->netstats->tpwtime)), (ifce->netstats->tprsize / (ifce->netstats->tprtime)) ,  ((ifce->netstats->tpwsize + ifce->netstats->tprsize) / (ifce->netstats->tpwtime + ifce->netstats->tprtime)))    ;
                } 
                ifce = ifce->next;
            }
            printf("---------------------------------------------------------------------------------------\n\n");
            ifce_list = ifce_list->next;
        }
        serv = serv->next;
        
    }
    
}

/************************************/
/* Function to print out statistics */
/************************************/

void print_stats (starttime, endtime, gflag, Sflag, wflag, week)
time_t starttime;
time_t endtime;
int gflag;
int Sflag;
int wflag;
char *week;
{
    
    struct dev_grp *dg;		/* pointer to device group record */
    struct failed *fd;		/* failed record */
    int i = 0;			/* counter */
    struct server *sr;		/* pointer to server record */
    int set = 0;		/* set for new error type */
    struct type *ty;		/* pointer to type record */
    struct unknown_errs *uk;	/* pointer to unknown_err structure */
    int total_reqs = 0;		/* total number of user requests made */
    int ty_totalreqs = 0;	/* total number of requests for type */
    int dg_totalreqs = 0;	/* total number of requests for group */
    int sr_totalreqs = 0;	/* total number of requests for server */
    
    
    total_reqs = global.usr_succ + global.usr_unsucc;
    
    printf ("\nRtcopy Statistics for the period : ");
    print_time_interval (starttime, endtime);
    
    /* print global statistics */
    
    if (!Sflag) {
        if (!gflag) {
            if (!wflag)
                printf ("\n\nSummarising for allservers :");
            else
                printf ("\n\nSummarising week %s for allservers :", week);
            printf ("\n.................................");
            printf ("\n\nUSER COMMANDS SUCCESSFUL : %d", global.usr_succ); 
            printf ("\nDEV COMMANDS SUCCESSFUL : %d", global.dev_succ);
            printf ("\nUSER COMMANDS FAILED : %d", global.usr_unsucc);
            printf ("\nDEV COMMANDS FAILED : %d", global.dev_unsucc);
            if (global.usr_unsucc > 0) {
                printf ("\n\nANALYSED FAILURES : %5.1f %% - %d", 
                    ((float)(global.usr_unsucc) / (float)(total_reqs)) * 100.0, 
                    global.usr_unsucc);
                printf ("\nUSER ERRORS : %5.1f %% - %d", 
                    ((float)(global.usr_errors) /(float)( total_reqs)) * 100.0, 
                    global.usr_errors);
                printf ("\nTAPE ERRORS : %5.1f %% - %d", 
                    ((float)(global.tape_errors) / (float)(total_reqs)) * 100.0, 
                    global.tape_errors);
                printf ("\nSHIFT ERRORS : %5.1f %% - %d",  
                    ((float)(global.shift_errors) / (float)(total_reqs)) * 100.0, 
                    global.shift_errors);
                printf ("\nROBOT ERRORS : %5.1f %% - %d", 
                    ((float)(global.robot_errors) / (float)(total_reqs)) * 100.0, 
                    global.robot_errors);
                printf ("\nNETWORK ERRORS : %5.1f %% - %d", 
                    ((float)(global.network_errors) / (float)(total_reqs)) * 100.0, 
                    global.network_errors);
                printf ("\nOTHER ERRORS : %5.1f %% - %d", 	
                    ((float)(global.other_errors) / (float)(total_reqs)) * 100.0, 
                    global.other_errors);
                printf ("\nUNKNOWN ERRORS : %5.1f %% - %d", 
                    ((float)(global.unknown_errors) / (float)(total_reqs)) * 100.0, 
                    global.unknown_errors);
                printf ("\nNO MESSAGE : %5.1f %% - %d",
                    ((float)(global.no_mess) / (float)(total_reqs)) * 100.0,
                    global.no_mess);
                
                printf ("\n");
                
                /* print errors and num Occs */
                
                for (i = 0; i < num_lines; i++) {
                    if (emp[i].error_code == 1) {
                        if (set == 0 && global.usr_errors > 0) {
                            printf ("\n\nUSER ERRORS (DETAILS)");
                            set = 1;
                        }
                    }
                    else if (emp[i].error_code == 2 && global.tape_errors > 0) {
                        if (set != 2) {
                            printf ("\n\nTAPE ERRORS (DETAIL)");
                            set = 2;
                        }
                    }
                    else if (emp[i].error_code == 3 && global.shift_errors > 0) {
                        if (set != 3) {
                            printf ("\n\nSHIFT ERRORS (DETAIL)");
                            set = 3;
                        }
                    }
                    else if (emp[i].error_code == 4 && global.robot_errors > 0) {
                        if (set != 4) {
                            printf ("\n\nROBOT ERRORS (DETAIL)");
                            set = 4;
                        }
                    }
                    else if (emp[i].error_code == 5 && global.network_errors > 0) {
                        if (set != 5) {
                            printf ("\n\nNETWORK ERRORS (DETAIL)");
                            set = 5;
                        }
                    }
                    else if (emp[i].error_code == 6 && global.other_errors > 0) {
                        if (set != 6) {
                            printf ("\n\nOTHER ERRORS (DETAIL)");
                            set = 6;
                        }
                    }
                    
                    if (emp[i].num_occs > 0)  
                        printf ("\n%s  -> %5.1f %% - %d", emp[i].message, 
                        ((float)(emp[i].num_occs) / (float)(total_reqs)) * 100.0, 
                        emp[i].num_occs);
                }
                printf("\n");
                
                /* print unknown errors */
                
                if (unknown_first != NULL) {
                    printf ("\nUNKNOWN ERRORS FOUND");
                    uk = unknown_first;
                    while (uk != NULL) {
                        uk->message[(strlen(uk->message))] = '\0';
                        printf ("\n%s\t:%s  -> %5.1f %% - %d", uk->server_name, uk->message, 
                            ((float)(uk->num_occs) / (float)(total_reqs)) * 100.0, uk->num_occs);
                        uk = uk->next;
                    }
                    printf ("\n");
                }
                if (global.no_mess > 0) {
                    printf ("\nNumber of failed requests with no matching entry in rtcopy.err file");
                    printf (" = %d", global.no_mess);
                    printf ("\nHost Name\tJob ID\tRequest Complete Time");
                    fd = fail_first;
                    while (fd != NULL) {
                        printf ("\n%s\t\t%d\t", fd->server, fd->jid);
                        print_time_val (fd->complete_time);
                        fd = fd->next;
                    }
                    printf ("\n");
                }
                printf ("\n");
      }
    }
    
    /* print device type statistics */
    
    if (!gflag) {
        ty = type_first;
        while (ty) {
            ty_totalreqs = ty->stat.usr_succ + ty->stat.usr_unsucc;
            if (!wflag)
                printf ("\n\nSummarising for %s servers", ty->type);
            else
                printf ("\n\nSummarising week %s for %s servers", week, ty->type);
            printf ("\n...............................");
            printf ("\n\nUSER COMMANDS SUCCESSFUL : %d", ty->stat.usr_succ);
            printf ("\nDEV COMMANDS SUCCESSFUL : %d", ty->stat.dev_succ);
            printf ("\nUSER COMMANDS FAILED : %d", ty->stat.usr_unsucc);
            printf ("\nDEV COMMANDS FAILED : %d", ty->stat.dev_unsucc);
            if (ty->stat.usr_unsucc > 0) {
                printf ("\n\nANALYSED FAILURES : %5.1f %% - %d",
                    ((float)(ty->stat.usr_unsucc) / (float)(ty_totalreqs)) * 100.0, 
                    ty->stat.usr_unsucc);
                printf ("\nUSER ERRORS : %5.1f %% - %d",
                    ((float)(ty->stat.usr_errors) /(float)(ty_totalreqs)) * 100.0, 
                    ty->stat.usr_errors);
                printf ("\nTAPE ERRORS : %5.1f %% - %d",
                    ((float)(ty->stat.tape_errors) / (float)(ty_totalreqs)) * 100.0, 
                    ty->stat.tape_errors);
                printf ("\nSHIFT ERRORS : %5.1f %% - %d",
                    ((float)(ty->stat.shift_errors) / (float)(ty_totalreqs)) * 100.0, 
                    ty->stat.shift_errors);
                printf ("\nROBOT ERRORS : %5.1f %% - %d",
                    ((float)(ty->stat.robot_errors) / (float)(ty_totalreqs)) * 100.0, 
                    ty->stat.robot_errors);
                printf ("\nNETWORK ERRORS : %5.1f %% - %d",
                    ((float)(ty->stat.network_errors) / (float)(ty_totalreqs)) * 100.0, 
                    ty->stat.network_errors);
                printf ("\nOTHER ERRORS : %5.1f %% - %d",
                    ((float)(ty->stat.other_errors) / (float)(ty_totalreqs)) * 100.0, 
                    ty->stat.other_errors);
                printf ("\nUNKNOWN ERRORS : %5.1f %% - %d",
                    ((float)(ty->stat.unknown_errors) / (float)(ty_totalreqs)) * 100.0, 
                    ty->stat.unknown_errors);
                printf ("\nNO MESSAGE : %5.1f %% - %d",
                    ((float)(ty->stat.no_mess) / (float)(ty_totalreqs)) * 100.0,
                    ty->stat.no_mess);
                printf ("\n");
                set = 0;
                
                /* print errors and num Occs */
                
                for (i = 0; i < num_lines; i++) {
                    if (emp[i].error_code == 1) {
                        if (set == 0 && ty->stat.usr_errors > 0) {
                            printf ("\n\nUSER ERRORS (DETAIL)");
                            set = 1;
                        }
                    }
                    else if (emp[i].error_code == 2 && ty->stat.tape_errors > 0) {
                        if (set != 2) {
                            printf ("\n\nTAPE ERRORS (DETAIL)");
                            set = 2;
                        }
                    }
                    else if (emp[i].error_code == 3 && ty->stat.shift_errors > 0) {
                        if (set != 3) {
                            printf ("\n\nSHIFT ERRORS (DETAIL)");
                            set = 3;
                        }
                    }
                    else if (emp[i].error_code == 4 && ty->stat.robot_errors > 0) {
                        if (set != 4) {
                            printf ("\n\nROBOT ERRORS (DETAIL)");
                            set = 4;
                        }
                    }
                    else if (emp[i].error_code == 5 && ty->stat.network_errors > 0) {
                        if (set != 5) {
                            printf ("\n\nNETWORK ERRORS (DETAIL)");
                            set = 5;
                        }
                    }
                    else if (emp[i].error_code == 6 && ty->stat.other_errors > 0) {
                        if (set != 6) {
                            printf ("\n\nOTHER ERRORS (DETAIL)");
                            set = 6;
                        }
                    }
                    if (ty->err_messages[i] > 0)
                        printf ("\n%s  -> %5.1f %% - %d", emp[i].message,
                        ((float)(ty->err_messages[i]) / (float)(ty_totalreqs)) * 100.0, 
                        ty->err_messages[i]);
                    
                }
                printf ("\n");
                
                /* print unknown errors */
                
                if (unknown_first != NULL && ty->stat.unknown_errors > 0) {
                    printf ("\nUNKNOWN ERRORS FOUND");
                    uk = unknown_first;
                    while (uk != NULL) {
                        if (strstr (uk->type, ty->type) != NULL) 
                        {
                            uk->message[(strlen(uk->message))] = '\0';
                            printf ("\n%s\t:%s  -> %5.1f %% - %d", uk->server_name, uk->message,
                                ((float)(uk->num_occs) / (float)(total_reqs)) * 100.0, uk->num_occs);						
                        }
                        uk = uk->next;
                    }
                    
                }
                if (ty->stat.no_mess > 0) {
                    printf ("\nNumber of failed requests with no matching entry in rtcopy.err file");
                    printf (" = %d", ty->stat.no_mess);
                    printf ("\nHost Name\tJob ID\tRequest Complete Time");
                    fd = fail_first;
                    while (fd != NULL) {
                        if (strcmp (ty->type, fd->type) == 0) {
                            printf ("\n%s\t\t%d\t", fd->server, fd->jid);
                            print_time_val (fd->complete_time);
                        }
                        fd = fd->next;
                    }
                }
                printf ("\n");
        }		     
        ty = ty->next; 
      }
    }
    else {
        
        /* print device group statistics */
        
        dg = dev_first;
        while (dg) {
            dg_totalreqs = dg->stat.usr_succ + dg->stat.usr_unsucc;
            if (!wflag)
                printf ("\n\nSummarising for %s servers", dg->dgn);
            else
                printf ("\n\nSummarising week %s for %s servers", week, dg->dgn);
            printf ("\n...............................");
            printf ("\n\nUSER COMMANDS SUCCESSFUL : %d", dg->stat.usr_succ);
            printf ("\nDEV COMMANDS SUCCESSFUL : %d", dg->stat.dev_succ);
            printf ("\nUSER COMMANDS FAILED : %d", dg->stat.usr_unsucc);
            printf ("\nDEV COMMANDS FAILED : %d", dg->stat.dev_unsucc);
            if (dg->stat.usr_unsucc > 0) {
                printf ("\n\nANALYSED FAILURES : %5.1f %% - %d",
                    ((float)(dg->stat.usr_unsucc) / (float)(dg_totalreqs)) * 100.0, 
                    dg->stat.usr_unsucc);
                printf ("\nUSER ERRORS : %5.1f %% - %d",
                    ((float)(dg->stat.usr_errors) /(float)(dg_totalreqs)) * 100.0, 
                    dg->stat.usr_errors);
                printf ("\nTAPE ERRORS : %5.1f %% - %d",
                    ((float)(dg->stat.tape_errors) / (float)(dg_totalreqs)) * 100.0, 
                    dg->stat.tape_errors);
                printf ("\nSHIFT ERRORS : %5.1f %% - %d",
                    ((float)(dg->stat.shift_errors) / (float)(dg_totalreqs)) * 100.0, 
                    dg->stat.shift_errors);
                printf ("\nROBOT ERRORS : %5.1f %% - %d",
                    ((float)(dg->stat.robot_errors) / (float)(dg_totalreqs)) * 100.0, 
                    dg->stat.robot_errors);
                printf ("\nNETWORK ERRORS : %5.1f %% - %d",
                    ((float)(dg->stat.network_errors) / (float)(dg_totalreqs)) * 100.0, 
                    dg->stat.network_errors);
                printf ("\nOTHER ERRORS : %5.1f %% - %d",
                    ((float)(dg->stat.other_errors) / (float)(dg_totalreqs)) * 100.0, 
                    dg->stat.other_errors);
                printf ("\nUNKNOWN ERRORS : %5.1f %% - %d",
                    ((float)(dg->stat.unknown_errors) / (float)(dg_totalreqs)) * 100.0, 
                    dg->stat.unknown_errors);
                printf ("\nNO MESSAGE : %5.1f %% - %d",
                    ((float)(dg->stat.no_mess) / (float)(dg_totalreqs)) * 100.0,
                    dg->stat.no_mess);
                printf ("\n");
                set = 0;
                
                /* print errors and num Occs */
                
                for (i = 0; i < num_lines; i++) {
                    if (emp[i].error_code == 1) {
                        if (set == 0 && dg->stat.usr_errors > 0) {
                            printf ("\n\nUSER ERRORS (DETAIL)");
                            set = 1;
                        }
                    }
                    else if (emp[i].error_code == 2 && dg->stat.tape_errors > 0) {
                        if (set != 2) {
                            printf ("\n\nTAPE ERRORS (DETAIL)");
                            set = 2;
                        }
                    }
                    else if (emp[i].error_code == 3 && dg->stat.shift_errors > 0) {
                        if (set != 3) {
                            printf ("\n\nSHIFT ERRORS (DETAIL)");
                            set = 3;
                        }
                    }
                    else if (emp[i].error_code == 4 && dg->stat.robot_errors > 0) {
                        if (set != 4) {
                            printf ("\n\nROBOT ERRORS (DETAIL)");
                            set = 4;
                        }
                    }
                    else if (emp[i].error_code == 5 && dg->stat.network_errors > 0) {
                        if (set != 5) {
                            printf ("\n\nNETWORK ERRORS (DETAIL)");
                            set = 5;
                        }
                    }
                    else if (emp[i].error_code == 6 && dg->stat.other_errors > 0) {
                        if (set != 6) {
                            printf ("\n\nOTHER ERRORS (DETAIL)");
                            set = 6;
                        }
                    }
                    if (dg->err_messages[i] > 0)
                        printf ("\n%s  -> %5.1f %% - %d", emp[i].message,
                        ((float)(dg->err_messages[i]) / (float)(dg_totalreqs)) * 100.0, 
                        dg->err_messages[i]);
                }
                printf ("\n");
            }
            dg = dg->next;
        }
    }
  }
  
  /* print server statistics */
  
  sr = serv_first;
  while (sr) {
      if (sr->failed != 1) {
          sr_totalreqs = sr->stat.usr_succ + sr->stat.usr_unsucc;
          if (!wflag)
              printf ("\n\nSummarising for %s server", sr->server_name);
          else
              printf ("\n\nSummarising week %s for %s server", week, sr->server_name);
          printf ("\n...............................");
          printf ("\n\nUSER COMMANDS SUCCESSFUL : %d", sr->stat.usr_succ);
          printf ("\nDEV COMMANDS SUCCESSFUL : %d", sr->stat.dev_succ);
          printf ("\nUSER COMMANDS FAILED : %d", sr->stat.usr_unsucc);
          printf ("\nDEV COMMANDS FAILED : %d", sr->stat.dev_unsucc);
          if (sr->stat.usr_unsucc > 0) {
              printf ("\n\nANALYSED FAILURES : %5.1f %% - %d",
                  ((float)(sr->stat.usr_unsucc) / (float)(sr_totalreqs)) * 100.0, 
                  sr->stat.usr_unsucc);
              printf ("\nUSER ERRORS : %5.1f %% - %d",
                  ((float)(sr->stat.usr_errors) /(float)(sr_totalreqs)) * 100.0, 
                  sr->stat.usr_errors);
              printf ("\nTAPE ERRORS : %5.1f %% - %d",
                  ((float)(sr->stat.tape_errors) / (float)(sr_totalreqs)) * 100.0, 
                  sr->stat.tape_errors);
              printf ("\nSHIFT ERRORS : %5.1f %% - %d",
                  ((float)(sr->stat.shift_errors) / (float)(sr_totalreqs)) * 100.0, 
                  sr->stat.shift_errors);
              printf ("\nROBOT ERRORS : %5.1f %% - %d",
                  ((float)(sr->stat.robot_errors) / (float)(sr_totalreqs)) * 100.0, 
                  sr->stat.robot_errors);
              printf ("\nNETWORK ERRORS : %5.1f %% - %d",
                  ((float)(sr->stat.network_errors) / (float)(sr_totalreqs)) * 100.0, 
                  sr->stat.network_errors);
              printf ("\nOTHER ERRORS : %5.1f %% - %d",
                  ((float)(sr->stat.other_errors) / (float)(sr_totalreqs)) * 100.0, 
                  sr->stat.other_errors);
              printf ("\nUNKNOWN ERRORS : %5.1f %% - %d",
                  ((float)(sr->stat.unknown_errors) / (float)(sr_totalreqs)) * 100.0, 
                  sr->stat.unknown_errors);
              printf ("\nNO MESSAGE : %5.1f %% - %d",
                  ((float)(sr->stat.no_mess) / (float)(sr_totalreqs)) * 100.0,
                  sr->stat.no_mess);
              printf ("\n");
              set = 0;
              
              /* print errors and num Occs */
              
              for (i = 0; i < num_lines; i++) {
                  if (emp[i].error_code == 1) {
                      if (set == 0 && sr->stat.usr_errors > 0) {
                          printf ("\n\nUSER ERRORS (DETAIL)");
                          set = 1;
                      }
                  }
                  else if (emp[i].error_code == 2 && sr->stat.tape_errors > 0) {
                      if (set != 2) {
                          printf ("\n\nTAPE ERRORS (DETAIL)");
                          set = 2;
                      }
                  }
                  else if (emp[i].error_code == 3 && sr->stat.shift_errors > 0) {
                      if (set != 3) {
                          printf ("\n\nSHIFT ERRORS (DETAIL)");
                          set = 3;
                      }
                  }
                  else if (emp[i].error_code == 4 && sr->stat.robot_errors > 0) {
                      if (set != 4) {
                          printf ("\n\nROBOT ERRORS (DETAIL)");
                          set = 4;
                      }
                  }
                  else if (emp[i].error_code == 5 && sr->stat.network_errors > 0) {
                      if (set != 5) {
                          printf ("\n\nNETWORK ERRORS (DETAIL)");
                          set = 5;
                      }
                  }
                  else if (emp[i].error_code == 6 && sr->stat.other_errors > 0) {
                      if (set != 6) {
                          printf ("\n\nOTHER ERRORS (DETAIL)");
                          set = 6;
                      }
                  }
                  
                  if (sr->err_messages[i] > 0)
                      printf ("\n%s  -> %5.1f %% - %d", emp[i].message,
                      ((float)(sr->err_messages[i]) / (float)(sr_totalreqs)) * 100.0, 
                      sr->err_messages[i]);
              }
              printf ("\n");
              /* print unknown errors */
              
              if (unknown_first != NULL && sr->stat.unknown_errors > 0) {
                  printf ("\nUNKNOWN ERRORS FOUND");
                  uk = unknown_first;
                  while (uk != NULL) {
                      uk->message[(strlen(uk->message))] = '\0';
                      if (strcmp (uk->server_name, sr->server_name) == 0)
                          printf ("\n%s\t:%s  -> %5.1f %% - %d", uk->server_name, 
                          uk->message, ((float)(uk->num_occs) / (float)(total_reqs)) 
                          * 100.0, uk->num_occs);
                      uk = uk->next;
                  }
                  printf ("\n");
              }
              
              if (sr->stat.no_mess > 0) {
                  printf ("\nNumber of failed requests with no matching entry ");
                  printf ("in rtcopy.err file");
                  printf (" = %d", sr->stat.no_mess);
                  printf ("\nHost Name\tJob ID\tRequest Complete Time");
                  fd = fail_first;
                  while (fd != NULL) {
                      if (strcmp (sr->server_name, fd->server) == 0) {		 
                          printf ("\n%s\t\t%d\t", fd->server, fd->jid);
                          print_time_val (fd->complete_time);
                      }
                      fd = fd->next;
                  }
                  printf ("\n");
              }
      }
      else
          printf ("\n");
    }
    
    
    
    get_ifce_list(sr);
    sort_netstats(sr);
    sr = sr->next;
  }
  print_netstats();
  
  return;
}

/************************************************/
/* Function to print out details of tape errors */
/************************************************/

void print_tape_errs (tp)
struct tp_sort *tp;
{
    int i = 0;		/* counter */
    
    printf ("\nReq Time           Server    Jid  VID     UNM      #Occ  Error Message\n");
    for (i = 0; i < num_tperrs; i++) {
        print_time_val (tp[i].first_time);
        printf ("  %-7s %5d  %-7s %-9s %3d  %s\n", tp[i].server, tp[i].first_jid,
            tp[i].vid, tp[i].unm, tp[i].num_occs, emp[tp[i].mess_line].message);
        if (tp[i].num_occs > 1) {
            print_time_val (tp[i].last_time);
            printf ("          %5d  %-7s %-9s      %s\n", tp[i].last_jid, tp[i].vid,
                tp[i].unm, emp[tp[i].mess_line].message);
        }
    }
    return;
}

/**************************/
/* Function to print time */
/**************************/

void print_time_val (stime)
time_t stime;
{
    struct tm *tm;       /* pointer to tm record */
    
    tm = localtime (&stime);
    printf ("%02d/%02d/%04d %02d:%02d:%02d",
        tm->tm_mday, tm->tm_mon+1, tm->tm_year+1900, tm->tm_hour, 
        tm->tm_min, tm->tm_sec);
    return;
}

/***************************************/
/* Function to print out time interval */
/***************************************/

void print_time_interval (starttime, endtime)
time_t starttime;
time_t endtime;
{
    printf (" (");
    print_time_val (starttime);
    printf ("  -  ");
    print_time_val (endtime);
    printf (")");
    return;
}

/***************************************************************************/
/* Function to parse the error message and fill the stats                  */
/***************************************************************************/
void read_errmsg(request, serv)
struct requests *request;
struct server *serv;
{
    char *message;
    struct processed_error *pe = NULL;
    struct processed_error *pe_last = NULL;
    
    struct requests *rq;
    struct tape_errs *tp;
    int jid_found = 0;
    int found = 0;
    int found2 = 0;
    int i;
    
    rq = request;
    message = rq->errmsg;
    
    if (!allgroups && (strcmp (rq->gid, "c3") == 0 || strcmp (rq->gid, "ct") == 0)) 
    {
        rq->exitcode = 1; 
        return;
    }
    
    /* first check if an error has already occured for this job */
    /* if so, skip the function */
    
    pe = processed_error_first;
    
    while (pe != NULL) 
    {
        pe_last = pe;
        if (pe->jid == rq->jid) 
        {
            jid_found = 1;
            break;
        }
        pe = pe->next;
    }
    
    if (!jid_found)
    {
        pe = (struct processed_error *) calloc (1, sizeof (struct processed_error));
        pe->jid = rq->jid;
        pe->next = NULL;
        if (processed_error_first == NULL) processed_error_first = pe;
        if  (pe_last != NULL) pe_last->next = pe;
    }
    else return;
    
    
    for (i = 0; i < num_lines; i++) 
    {
        
        if (strstr (message, emp[i].message) != NULL) {
            
            rq->exitcode = emp[i].error_code;    
            emp[i].num_occs++;
            rq->err_lineno = i;
            
            /* Message recognized */
            
            found = 1;
            if (rq->exitcode == 2) {
                tp = tape_first;
                if (tp == NULL) {
                    tp = (struct tape_errs *) calloc (1, sizeof (struct tape_errs));
                    tp->first_time = rq->req_time;
                    strcpy (tp->unm, rq->unm);
                    strcpy (tp->vid, rq->vid);
                    tp->first_jid = rq->jid;
                    strcpy (tp->server, rq->server);
                    tp->mess_line = i;
                    tp->num_occs++;
                    tp->next = NULL;
                    tape_first = tp;
                    tape_prev = tp;
                    num_tperrs++;
                }
                else {
                    while (tp != NULL) {
                        if ((strcmp (tp->unm, rq->unm) == 0) && 
                            (strcmp (tp->vid, rq->vid) == 0) &&
                            (strcmp (tp->server, rq->server) == 0) &&
                            tp->mess_line == i) {
                            tp->last_time = rq->req_time;
                            tp->last_jid = rq->jid;
                            tp->num_occs++;
                            found2 = 1;
                            break;
                        }
                        tp = tp->next;
                    }
                    if (!found2) {
                        tp = (struct tape_errs *) calloc (1, sizeof (struct tape_errs));
                        tp->first_time = rq->req_time;
                        strcpy (tp->unm, rq->unm);
                        strcpy (tp->vid, rq->vid);
                        tp->first_jid = rq->jid;
                        tp->mess_line = i;
                        strcpy (tp->server, rq->server);
                        tp->num_occs++;
                        tp->next = NULL;
                        tape_prev->next = tp;
                        tape_prev = tp;
                        num_tperrs++;
                    }
                }
                found2 = 0;
            }
            break;
        }
    }
    if (!found) {
        /* creating unknown for jid */
        create_unknown (rq, message , serv);
        rq->exitcode = 10;
        rq->err_lineno = -1;
    }
    message = NULL;
    
    return;
}


/***********************************************/
/* Function to read error messages into memory */
/***********************************************/

void read_mess2mem()
{
    char buf[256];		/* buffer */
    struct err_mess *emp1;	/* error messages pointer */
    FILE *fp;			/* file descriptor */
    int len = 0;		/* length of error message */
    char *p1;			/* character pointer */
    
    /* read in the error messages from "err_messages" file into memory */
    
    if ((fp = fopen (RTCOPYERRLIST, "r")) == NULL) {
        printf ("%s :open error\n", RTCOPYERRLIST);
        exit (1);
    }
    else {
        while (fgets (buf, sizeof (buf), fp))
            num_lines++;
    }
    rewind (fp);
    
    emp = (struct err_mess *) malloc (num_lines * sizeof (struct err_mess));
    emp1 = emp;
    while (fgets (buf, sizeof (buf), fp)) {
        p1 = strchr (buf + 1, '"');
        len = p1 - buf;
        emp1->message = malloc (len);
        *p1 = '\0';
        strncpy (emp1->message, buf + 1, len);
        emp1->error_code = atoi (p1 + 2);
        emp1->num_occs = 0;
        emp1++;
    }
    
    return;
}

/*********************************************************************/
/* Function to read the TPCONFIG files to extract device group types */
/*********************************************************************/

void read_tpfile()
{
    char buf[256];		/* buffer */
    int c;			/* number of items read */
    int fp;			/* file identifier */
    char *p;			/* pointer to buffer */
    char *q;			/* pointer for strchr function */
    char savebuf[256];		/* save partial line */
    int saveflag = 0;		/* flag set if save required */
    int server_failed = 0; 	/* set if server open failed */
    struct server *serv_list;	/* pointer to list of all servers to open */
    char tpconfile[80];		/* TPCONFIG file path and name */
    
    tpconfile[0] = '\0';
    
    /* for each server in the server list open the TPCONFIG file and read in data */
    
    serv_list = serv_first;
    while (serv_list) {
        
        /* copy the name and path of the TPCCONFIG file into tpconfile */
        
        strcpy (tpconfile, serv_list->server_name);
        strcat (tpconfile, ":");
        strcat (tpconfile, TPCONFIG);
        
        if ((fp = rfio_open (tpconfile, O_RDONLY)) < 0) {
            fprintf (stderr, "%s : open error : %s\n", tpconfile, rfio_serror());
            server_failed = 1;
            serv_list->failed = 1;
        }
        
        /* read in data, for each full line read in that is not a comment line  or a blank */
        /* line insert_type. if an incomplete line is read in save it in savebuf and continue */
        /* If at end of file saveflag is still set insert type from savebuf */
        
        if (!server_failed) {
            while ((c = rfio_read (fp, buf, sizeof (buf) - 1)) > 0) {
                buf[c] = 0;
                p = buf;
                if (saveflag) {
                    q = strchr (buf, '\n');	
                    if (!q) {	
                        strcat (savebuf, p);	
                        continue;
                    } 
                    *q = '\0';			
                    strcat (savebuf, p);	
                    if (*savebuf != '#' && *savebuf != '\0') 
                        insert_type(savebuf, serv_list);
                    saveflag = 0;		
                    p = q + 1;			
                }
                while (q = strchr (p, '\n')) {
                    *q = '\0';			
                    if (*p == '#' || *p == '\0') {
                        p = q + 1;		
                        continue;
                    }
                    insert_type(p, serv_list);	
                    p = q + 1;
                }
                if (strlen (p)) {		
                    strcpy (savebuf, p);	
                    saveflag = 1;	
                }
            }
            
            if (saveflag && *savebuf != '#' && *savebuf != '\0')
                insert_type(savebuf, serv_list);
            
            rfio_close(fp);
        }
        server_failed = 0;
        serv_list = serv_list->next;
    }
    return;
}
/**********************************************************/
/* Function to get the names of the different interfaces  */
/**********************************************************/
void get_ifce_list(serv)
struct server *serv;
{
    struct ifce_stats *ifce;
    struct ifce_list *ifce_list;
    int found = 0;
    
    ifce = serv->netstats_first; 
    
    while(ifce)
    {
        ifce_list = serv->ifce_list_first;
        
        if (ifce_list == NULL )
        {
            serv->ifce_list_first =  (struct ifce_list*) calloc (1, sizeof (struct ifce_list));
            strcpy(serv->ifce_list_first->name, ifce->name);
            ifce_list = serv->ifce_list_first;
        }
        while(ifce_list)
        {
            if ( strcmp(ifce_list->name, ifce->name) == 0) 
                found =1;
            ifce_list = ifce_list->next;
        }
        
        if(!found)
        {
            ifce_list = serv->ifce_list_first;
            while (ifce_list->next) ifce_list = ifce_list->next;
            ifce_list->next = (struct ifce_list*) calloc (1, sizeof (struct ifce_list));
            ifce_list = ifce_list->next;
            strcpy(ifce_list->name,ifce->name);
        }
        ifce = ifce->next; 
    }
    return;
}


/************************************************/
/* Function to sort the network statistics by */
/* interface and  take the 10 most active servers  */
/************************************************/
void sort_netstats(serv)
struct server *serv;
{
    int i =0;
    long int tot_max = 0;
    long int tot_prev = 999999*1024;
    long int tot = 0;
    struct ifce_stats *ifce = NULL;
    struct sorted_ifce_stats *sorted_netstats;
    struct ifce_list *ifce_list = NULL;
    
    serv->sorted_netstats_first = (struct sorted_ifce_stats*) calloc (1, sizeof (struct sorted_ifce_stats));
    
    sorted_netstats = serv->sorted_netstats_first;
    ifce_list = serv->ifce_list_first;
    
    ifce = serv->netstats_first;
    
    ifce_list = serv->ifce_list_first;  
    
    while (ifce_list)
    {
        i = 0;
        
        while ( i < 10)
        {
            ifce = serv->netstats_first;
            
            while (ifce)
            {
                tot = ifce->tpwsize + ifce->tprsize;
                
                if (( tot > tot_max) && (tot < tot_prev) && (strcmp(ifce->name,ifce_list->name) == 0))            
                {     
                    sorted_netstats->netstats = ifce;
                    tot_max = tot;
                }         
                ifce = ifce->next;
            }
            
            if((i < 9) && (tot_max != 0))
            {
                sorted_netstats->next = (struct sorted_ifce_stats*) calloc (1, sizeof (struct sorted_ifce_stats));
                sorted_netstats = sorted_netstats->next;
            }
            
            tot_prev = tot_max;
            
            tot_max = 0;
            i++; 
        }
        ifce_list = ifce_list->next;
    }
    
    sorted_netstats = serv->sorted_netstats_first;
    
    return;
}

/************************************************/
/* Function to copy info into new server record */
/************************************************/
void sort_fill(serv_rec)
struct server *serv_rec;
{
    strcpy (serv_rec->server_name, serv_first->server_name);
    strcpy (serv_rec->type, serv_first->type);
    serv_rec->nb_dgns = serv_first->nb_dgns;
    serv_rec->err_messages = (int *) calloc (num_lines, sizeof (int));
    serv_rec->next = NULL;
    return;
}

/****************************************************/
/* Function to sort the server list into type order */
/****************************************************/
void sort_servlist()
{
    int c = 0;			/* return value from strcmp */
    int d = 0;			/* return value from strcmp */
    struct server *new_first; 	/* pointer to new server list */
    struct server *prev;
    struct server *serv_new;   	/* pointer to server list */
    struct server *st;		/* pointer to server structure */
    struct server *temp;
    
    new_first = (struct server*) calloc (1, sizeof (struct server));
    sort_fill (new_first);
    
    while (serv_first) {
        serv_new = new_first;
        while (serv_new) {
            c = strcmp (serv_first->type, serv_new->type);
            d = strcmp (serv_first->server_name, serv_new->server_name);
            
            if (c == 0 && d == 0) break; 
            
            st = (struct server*) calloc (1, sizeof (struct server));
            sort_fill (st);
            
            if (c > 0) {
                if (serv_new->next == NULL) {
                    serv_new->next = st;
                    break;
                }
                else 
                    prev = serv_new;		     
            }
            if (c < 0) {
                if (serv_new == new_first) {
                    st->next = serv_new;
                    new_first = st;
                    break;
                }
                else {
                    prev->next = st;
                    st->next = serv_new;
                    break;
                }
            }
            if (c == 0) {
                if (d < 0) {
                    if (serv_new == new_first) {
                        st->next = serv_new;
                        new_first = st;
                        break;
                    }
                    else {
                        prev->next = st;
                        st->next = serv_new;
                        break;
                    }
                }
                else if (d > 0) {
                    if (serv_new->next == NULL) {
                        serv_new->next = st;
                        break;
                    }
                    else 
                        prev = serv_new; 
                }
            }
            serv_new = serv_new->next;	
        }
        temp = serv_first;
        serv_first = serv_first->next;
        free (temp);
    }
    serv_first = new_first;
    return;
}

/***********************************************/
/* Function to sort tape errors by unm and vid */
/***********************************************/

void sort_tperrs()
{
    struct tape_errs *tp;
    struct tape_errs *tp2;
    struct tp_sort *tps;
    int comp();
    int comp2();
    
    tperr_sort = (struct tp_sort *) malloc (num_tperrs * sizeof (struct tp_sort));
    tps = tperr_sort;
    
    tp = tape_first;
    while (tp != NULL) {
        tps->first_time = tp->first_time;	
        tps->last_time = tp->last_time;
        strcpy (tps->unm, tp->unm);
        strcpy (tps->vid, tp->vid);
        tps->first_jid = tp->first_jid;
        tps->last_jid = tp->last_jid;
        tps->mess_line = tp->mess_line;
        tps->num_occs = tp->num_occs;
        strcpy (tps->server, tp->server);
        tps++;
        tp2 = tp;
        tp = tp->next;
        free (tp2);
    }
    
    qsort (tperr_sort, num_tperrs, sizeof (struct tp_sort), comp);
    printf ("\nDETAILS OF TAPE ERRORS - SORTED BY VID");
    print_tape_errs (tperr_sort);
    qsort (tperr_sort, num_tperrs, sizeof (struct tp_sort), comp2);
    printf ("\nDETAILS OF TAPE ERRORS - SORTED BY UNM");
    print_tape_errs (tperr_sort);
    return;
}
/********************************************************/
/* Function to extract the device group for the request */
/********************************************************/
void store_dgn (rp)
struct accttape *rp;
{
    struct store_dgn *sd;       /* pointer to stored dgn record */
    int found = 0;              /* flag set if record found */
    
    /* store the jid and dgn for ACCTTAPE records */
    /* if not already stored create a record */
    
    sd = store_first;
    if (sd == NULL) {
        sd = (struct store_dgn *) calloc (1, sizeof (struct store_dgn));
        sd->next = NULL;
        sd->prev = NULL;
        strcpy (sd->dgn, rp->dgn);
        strcpy (sd->unm, rp->drive);
        sd->jid = rp->jid;
        store_first = sd;
        store_prev = sd;
    }
    else {
        while (sd) {
            if (sd->jid == rp->jid) {
                if (strcmp (sd->unm, "") == 0)
                    strcpy (sd->unm, rp->drive);
                found = 1;
                break;
            }
            sd = sd->next;
        }
        if (! found) {
            sd = (struct store_dgn *) calloc (1, sizeof (struct store_dgn));
            sd->next = NULL;
            strcpy (sd->dgn, rp->dgn);
            strcpy (sd->unm, rp->drive);
            sd->jid = rp->jid;
            sd->prev = store_prev;
            store_prev->next = sd;
            store_prev = sd;
        }
    }
    return;
}

/*****************************************/
/* Function to swap byte order of fields */
/*****************************************/
void swap_fields (rp)
struct accttape *rp;
{
    swap_it (rp->subtype);
    swap_it (rp->uid);
    swap_it (rp->gid);
    swap_it (rp->jid);
    swap_it (rp->fseq);
    swap_it (rp->reason);
}

void swap_fields2 (rtcp)
struct acctrtcp *rtcp;
{
    swap_it (rtcp->subtype);
    swap_it (rtcp->uid);
    swap_it (rtcp->gid);
    swap_it (rtcp->jid);
    swap_it (rtcp->stgreqid);
    swap_it (rtcp->size);
    swap_it (rtcp->retryn);
    swap_it (rtcp->exitcode);
    return;
}

/*********************************/
/* Functions to sort tape errors */
/*********************************/
int comp (a, b)
struct tp_sort *a;
struct tp_sort *b;
{
    if (strcmp (a->vid, b->vid) == 0) return (0);
    else if (strcmp (a->vid, b->vid) < 0) return (-1);
    else return (1);
}

int comp2 (a, b)
struct tp_sort *a;
struct tp_sort *b;
{
    if (strcmp (a->unm, b->unm) == 0) return (0);
    else if (strcmp (a->unm, b->unm) < 0) return (-1);
    else return (1);
}

/*****************************************/
/* Function to give correct usage syntax */
/*****************************************/

void usage (cmd)
char *cmd;
{
    fprintf (stderr, "usage: %s ", cmd);
    fprintf (stderr, "%s%s%s", "[--acctdir accounting_dir][--allgroups][-e end_time]\n",
        "\t[-f accounting_file][-g device_group][-S server_name][-s start_time]\n",
        "\t[-v][-w week_number]\n");
    return;
}

/***************************************************/
/* Function to store user and group statistics     */
/***************************************************/

void store_usrstats()
{
    struct requests *rq;
    struct users *usr, *tmp_usr, *crap;
    struct groups *grp, *tmp_grp;
    int found_usr,found_grp;
    
    
    rq = req_first;
    
    while(rq)
    {
        if(rq->failed == 1) 
        {
            rq = rq->next;
            continue;
        }
        
        found_usr = 0;
        found_grp = 0;
        
        usr = users_first;
        grp = groups_first;
        
        if(users_first == NULL)
        {
            users_first = (struct users *) calloc (1, sizeof (struct users));
            strcpy(users_first->name,rq->uid);
            strcpy(users_first->groupname, rq->gid);
            users_first->readops = 0;
            users_first->writeops = 0;
            users_first->MBwrite = 0;
            users_first->MBread = 0;
            if(rq->reqtype == 'r') {
                users_first->readops++;
                users_first->MBread+=rq->transfer_size/1024;
            }
            if(rq->reqtype == 'w') {
                users_first->writeops++;
                users_first->MBwrite+=rq->transfer_size/1024;
            }
            users_first->totDrvSecs += rq->transfer_time;
            users_first->next = NULL;
            users_first->prev = NULL;
            found_usr = 1;
        }
        
        while(usr)
        {
            if(strcmp(usr->name,rq->uid) == 0)
            {
                found_usr = 1;
                if(rq->reqtype == 'r') {
                    usr->readops++;
                    usr->MBread += rq->transfer_size/1024;
                }
                if(rq->reqtype == 'w') {
                    usr->writeops++;
                    usr->MBwrite += rq->transfer_size/1024;
                }
                usr->totDrvSecs += rq->transfer_time; 
                break;
            }
            tmp_usr = usr;
            usr = usr->next;
        }
        
        if(found_usr == 0)
        {
            usr = (struct users *) calloc (1, sizeof (struct users));
            strcpy(usr->name,rq->uid);
            strcpy(usr->groupname,rq->gid);
            usr->readops = 0;
            usr->writeops = 0;
            usr->MBread = usr->MBwrite = usr->totDrvSecs = 0;
            if(rq->reqtype == 'r') {
                usr->readops++;
                usr->MBread += rq->transfer_size/1024;
            }
            if(rq->reqtype == 'w') {
                usr->writeops++;
                usr->MBwrite += rq->transfer_size/1024;
            }
            usr->totDrvSecs += rq->transfer_time;
            usr->prev = tmp_usr;
            usr->prev->next = usr;
            usr->next = NULL;	
        }	
        
        if(groups_first == NULL)
        {
            groups_first = (struct groups *) calloc (1, sizeof (struct groups));
            strcpy(groups_first->name,rq->gid);
            groups_first->readops = 0;
            groups_first->writeops = 0;
            groups_first->MBread = groups_first->MBwrite = 0;
            groups_first->totDrvSecs =0;
            if(rq->reqtype == 'r') {
                groups_first->readops++;
                groups_first->MBread += rq->transfer_size/1024;
            }
            if(rq->reqtype == 'w') {
                groups_first->writeops++;
                groups_first->MBwrite += rq->transfer_size/1024;
            }
            groups_first->totDrvSecs += rq->transfer_time;
            
            groups_first->next = NULL;
            groups_first->prev = NULL;
            found_grp = 1;
        }
        
        while(grp)
        {
            if(strcmp(grp->name,rq->gid) == 0)
            {
                found_grp = 1;
                if(rq->reqtype == 'r') {
                    grp->readops++;
                    grp->MBread += rq->transfer_size/1024;
                }
                if(rq->reqtype == 'w') {
                    grp->writeops++;
                    grp->MBwrite += rq->transfer_size/1024;
                };
                grp->totDrvSecs += rq->transfer_time;
                break;
            }
            tmp_grp = grp;
            grp = grp->next;
        }
        
        if(found_grp == 0)
        {
            grp = (struct groups *) calloc (1, sizeof (struct groups));
            strcpy(grp->name,rq->gid);
            if(rq->reqtype == 'r') {
                grp->readops++;
                grp->MBread += rq->transfer_size/1024;
            }
            if(rq->reqtype == 'w') {
                grp->writeops++;
                grp->MBwrite += rq->transfer_size/1024;
            }
            grp->totDrvSecs += rq->transfer_time;
            grp->prev = tmp_grp;
            grp->prev->next = grp;
            grp->next = NULL;
        }
        if(rq->reqtype == 'r') tapes_read++;
        if(rq->reqtype == 'w') tapes_written++;
        rq = rq->next;
        
}

return;
}

/***************************************************/
/* Function to sort user and group statistics     */
/***************************************************/

void sort_usrstats()
{
    struct users *usr = NULL;
    struct groups *grp = NULL;
    struct users *sorted_usr = NULL;
    struct groups *sorted_grp = NULL;
    int i =0;
    long int tot_max = 0;
    long int tot_prev = 999999*1024;
    long int tot = 0;
    
    sorted_users_first = (struct users*) calloc (1, sizeof (struct users));
    sorted_groups_first = (struct groups*) calloc (1, sizeof (struct groups));
    
    
    sorted_usr = sorted_users_first;
    sorted_grp = sorted_groups_first;
    
    i = 0;
    
    while ( i < 20)
    {
        usr = users_first;
        
        while (usr)
        {
            tot = usr->readops + usr->writeops;
            
            if (( tot > tot_max) && (tot < tot_prev) )            
            {     
                strcpy(sorted_usr->name,usr->name);
                strcpy(sorted_usr->groupname, usr->groupname);
                sorted_usr->readops = usr->readops;
                sorted_usr->writeops = usr->writeops;
                sorted_usr->MBread = usr->MBread;
                sorted_usr->MBwrite = usr->MBwrite;
                sorted_usr->totDrvSecs = usr->totDrvSecs;
                sorted_usr->next = NULL;
                sorted_usr->prev = NULL;
                tot_max = tot;
            }         
            usr = usr->next;
        }
        
        if((i < 19) && (tot_max != 0))
        {
            sorted_usr->next = (struct users*) calloc (1, sizeof (struct users));
            sorted_usr->next->prev = sorted_usr;
            sorted_usr = sorted_usr->next;
        }
        
        tot_prev = tot_max;
        
        tot_max = 0;
        i++; 
    }
    
    i = 0;
    tot_max = 0;
    tot_prev = 999999*1024;
    tot = 0;
    
    while ( i < 20)
    {
        grp = groups_first;
        
        while (grp)
        {
            tot = grp->readops + grp->writeops;
            
            if (( tot > tot_max) && (tot < tot_prev) )            
            {     
                strcpy(sorted_grp->name,grp->name);
                sorted_grp->readops = grp->readops;
                sorted_grp->writeops = grp->writeops;
                sorted_grp->MBread = grp->MBread;
                sorted_grp->MBwrite = grp->MBwrite;
                sorted_grp->totDrvSecs = grp->totDrvSecs;
                sorted_grp->next = NULL;
                sorted_grp->prev = NULL;
                tot_max = tot;
            }         
            grp = grp->next;
        }
        
        if((i < 19) && (tot_max != 0))
        {
            sorted_grp->next = (struct groups*) calloc (1, sizeof (struct groups));
            sorted_grp->next->prev = sorted_grp;
            sorted_grp = sorted_grp->next;
        }
        
        tot_prev = tot_max;
        
        tot_max = 0;
        i++; 
    }
    
    
    return;   
}
/***************************************************/
/* Function to print user and group statistics     */
/***************************************************/

void print_usrstats()
{
    struct users *usr;
    struct groups *grp;
    
    sort_usrstats();
    printf("\n\n\n\n\n\n\n\n\n\n");
    printf("\t******************************************\n");
    printf("\t*           User Statistics              *\n");
    printf("\t******************************************\n");
    
    printf("\n\tTapes read : %d\n\tTapes written : %d\n", tapes_read, tapes_written);
    printf("\n");
    printf("-- 20 most active users and groups --\n\n");
    usr = sorted_users_first;
    printf("User /Group     Tapes read   Tapes written  MB read  MB write  Drive hrs\n");
    printf("-----------    ------------  -------------  -------  --------  ---------\n");
    while(usr)
    {
        printf("%-8s/%-2s    %6d       %6d          %6d    %6d     %6d\n", usr->name, usr->groupname, usr->readops, usr->writeops,usr->MBread,usr->MBwrite,usr->totDrvSecs/3600);
        usr = usr->next;
    }
    printf("\n");
    printf("Group        Tapes read    Tapes written  MB read  MB write  Drive hrs\n");
    printf("----------   ------------  -------------  -------  --------  ---------\n");
    grp = sorted_groups_first;
    while(grp)
    {
        printf("%-12s %6d        %6d          %6d    %6d     %6d\n", grp->name, grp->readops, grp->writeops,grp->MBread,grp->MBwrite,grp->totDrvSecs/3600);
        grp = grp->next;
    }
    printf("\n\n");
    return;
}
/*******************************************************/
/* Function to clear the requests list */
/*******************************************************/

void clear_requests()
{
    struct requests *rq;	/* pointer to request record */
    struct requests *tmp_rq;	/* pointer to request record */
    
    rq = req_first;
    while (rq) {
        tmp_rq = rq->next;
        free (rq);
        rq = tmp_rq;
    }
    req_first = NULL;
    req_prev = NULL;
    req_last = NULL;
    return;
}
