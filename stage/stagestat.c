/*
 * Copyright (C) 1996-1998 by CERN/CN/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)stagestat.c	1.2 05/06/98 CERN CN-PDP/DM Claire Redmond";
#endif /* not lint */

#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <math.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "stage.h"
#include "../h/sacct.h"
#include <time.h>

/* Macro to swap byte order */

#define swap_it(a) swab((char *)&a,(char *)&a,sizeof(a));\
                   a=((unsigned int)a<<16)|((unsigned int)a>>16);


extern char *optarg;			/* command line optional argument */
#if !defined(linux)
extern char *sys_errlist[];
#endif

struct file_inf{
   int uid;				/* user id number */	
   int gid;				/* group id number */
   char poolname[MAXPOOLNAMELEN];	/* pool name */
   time_t last_stgin;			/* time file was last staged in */
   time_t total_stgin_gc;	/* total stagein time if clrd by garbage collector */
   time_t total_stgin_uc;	/* total stagein time if clrd by use */	
   union{
        struct{				/* tape files */
		char vid[7];		/* volume id */
		char fseq[MAXFSEQ];	/* file sequence number */
	}t;
	struct{				/* disk files */
		char xfile[MAXHOSTNAMELEN+MAXPATH];	/* filename and path */
	}d;
   }u1;
   int stage_status;			/* current stage status */
   int num_periods_gc;		/* number periods terminated by garbage collector */
   int num_periods_uc;		/* number periods terminated by user */
   int num_accesses;		/* number times file accessed in current time period */
   char t_or_d;				/* identifies if file is from tape or disk */
   struct file_inf *next;
};
struct file_inf *file_last, *file_list = NULL;

struct stg_inf{
   int reqid;				/* request id of stagein command */
   int num_files;			/* total number of files requested */
   struct stg_inf *next;
};
struct stg_inf *stg_last, *srec_match, *stage_list = NULL;

struct frec_nbaccs{
   struct file_inf *rec;		/* pointer to a file_inf record */
   int nbaccesses;			/* number of accesses */
   char poolname[MAXPOOLNAMELEN];	/* pool name */
};

struct frec_avg{
   struct file_inf *rec;		/* pointer to a file_inf record */
   float avg_life;			/* total average life for the file */
   char poolname[MAXPOOLNAMELEN];	/* pool name */
};

struct pool_inf{
   char poolname[MAXPOOLNAMELEN];	/* pool name */
   float num_files;			/* total number of files requested by pool */
   int num_frecs;			/* total num tape files requested */
   int num_xrecs;			/* total num disk files requested */
   int acc;		/* num files accessed, not stgd in or clrd during time_period */
   int acc_clrd;	/* num files accesses and clrd, not staged in */
   int stg_acc;		/* num files staged in and accessed, not clrd */
   int stg_acc_clrd;	/* num files staged in, accessed and clrd */
   int total_stged_files;  /* num files staged in, accd, clrd by garbage collector */ 
   float total_accesses;   /* total num of file accesses for pool */
   float total_avg_life;   /* total avg life of files clrd by garbage collector */
   struct pool_inf *next;
};
struct pool_inf *pool_last, *pool_list = NULL;

 
int nb_req[12];			/* array to store num requests for each command */	
int nb_restart = 0;		/* number of times stager restarted */
int rc[12][10];		/* array to store num of errors and warnings for each command */
int num_remounts = 0;		/* number of files remounted during time period */
unsigned int num_frecs = 0;		/* total number of tape files */
unsigned int num_xrecs = 0;		/* total number of disk files */

main(argc, argv)
int argc;
char **argv;
{
   char acctfile[80];			/* accounting file name */
   char buf[256];
   char poolname[MAXPOOLNAMELEN];	/* pool name */
 
   struct accthdr accthdr;		/* accthdr record */
   struct acctstage *rp;		/* pointer to accstage record */
   struct stg_inf *srecord;		/* pointer to stg_inf record */

   time_t cvt_datime();			/* date function */
   time_t endtime = 0;			/* time up to which to obtain information */
   time_t starttime = 0;		/* time from which to obtain information */
	
   int c;				
   int errflg = 0;			/* error flag */
   int fd_acct;				/* file identifier */
   int nrec = 0;			/* number of records read */
   int num_mounts = 0;			/* number of mounts made */
   int num_multireqs = 0;		/* number of multifile requests */
   int swapped = 0;			/* flag set if byte order swapped */
   int pflag = 0;			/* pool group specified */
   int tflag = 0;			/* sort on lifetime */
   int aflag = 0;			/* sort on num accesses */	

   acctfile[0] = '\0';
   while ((c = getopt (argc, argv, "e:f:s:p:S:")) != EOF) {
	switch (c) {
	case 'e':
		if ((endtime = cvt_datime (optarg)) < 0) {
			fprintf (stderr, "incorrect time value %s\n", optarg);
			errflg++;
		}
		break;
	case 'f':
		strcpy (acctfile, optarg);
		break;
	case 's':
		if ((starttime = cvt_datime (optarg)) < 0) {
			fprintf (stderr, "incorrect time value %s\n", optarg);
			errflg++;
		}
		break;
	case 'p':
		pflag++;
		strcpy (poolname, optarg);
		break;
	case 'S':
		if (strcmp (optarg, "t") == 0) tflag++;
		else if (strcmp (optarg, "a") == 0) aflag++;
		else {
		      	fprintf (stderr,"Incorrect or no sort criteria given %s\n",
				optarg);
		      	errflg++;
		}
		break;
	case '?':
		errflg++;
		break;
	}
   }
   if (errflg) {
	usage (argv[0]);
	exit (USERR);
   }
   if (acctfile[0] == '\0') strcpy (acctfile, ACCTFILE);
   if ((fd_acct = open (acctfile, O_RDONLY)) < 0) {
	fprintf (stderr, "%s : open error : %s\n", acctfile, sys_errlist[errno]);
	exit (USERR);
   }
   while (getacctrec (fd_acct, &accthdr, buf, &swapped)) {
	nrec++;
	if (!starttime && nrec == 1) starttime = accthdr.timestamp;
	if (accthdr.package != ACCTSTAGE) continue;
	if (accthdr.timestamp < starttime) continue;
	if (endtime && accthdr.timestamp > endtime) break;
	rp = (struct acctstage*)buf;
	if (swapped) {
		swap_fields (rp);
		swapped = 0;
	}
	if (rp->subtype == STGSTART)		/* stgdaemon started */
		nb_restart++;
	else if (rp->subtype == STGCMDR) {	/* command recieved */
		nb_req[rp->req_type]++;
		if (rp->req_type == 1) create_stglist (rp);
	}
	else if (rp->subtype == STGCMDS && rp->u2.s.t_or_d == 't') /* stager started */
		num_mounts ++; 	
	else if (rp->subtype == STGFILS && rp->req_type == 1) {	   /* file staged */
		if (pflag && strcmp (poolname, rp->u2.s.poolname) != 0) continue;
		enter_fils_details (rp, accthdr);
		enter_pool_details (rp);
	}
	else if (rp->subtype == STGFILC) {	/* file cleared */
		if (pflag && strcmp (poolname, rp->u2.s.poolname) != 0) continue;
		enter_filc_details (rp, accthdr);	
	}
	else if (rp->subtype == STGCMDC && rp->req_type < 12) {  /* command completed */
                if (rp->exitcode < 5) rc[rp->req_type][rp->exitcode]++;
                else if (rp->exitcode == ENOSPC) rc[rp->req_type][5]++;
                else if (rp->exitcode == CLEARED) rc[rp->req_type][6]++;
                else if (rp->exitcode == REQKILD) rc[rp->req_type][8]++;
                else if (rp->exitcode == BLKSKPD || rp->exitcode == TPE_LSZ
                        || rp->exitcode == MNYPARI || rp->exitcode == LIMBYSZ)
                        rc[rp->req_type][7]++;
                else if (rp->exitcode == ESTNACT) rc[rp->req_type][9]++;
        }
   }
   if (!endtime && nrec) endtime = accthdr.timestamp;
   close (fd_acct);
	
   if (pflag && pool_list == NULL){
	printf ("\nNo records found for Pool : %s\n", poolname);
   	pflag = 0;
   }

/* read through stg_inf list and determine number of multifile requests */

   srecord = stage_list;
   while (srecord != NULL) {
	if (srecord->num_files > 1) num_multireqs++;
	srecord = srecord->next;
   }

/* print out statistics */	

   print_globstat (starttime, endtime, num_mounts, num_multireqs);
   print_poolstat (tflag, aflag, pflag, poolname);

   if (errflg)
	exit (SYERR);
   else
	exit (0);
}



/* Function to create a file record for each new file found */

create_filelist (rp)
struct acctstage *rp;
{
   struct file_inf *frec;			/* pointer to file_inf record */ 

   frec = (struct file_inf*) calloc (1, sizeof (struct file_inf));
   frec->next = NULL;
   frec->uid = rp->uid;
   frec->gid = rp->gid;	
   strcpy (frec->poolname, rp->u2.s.poolname);
   if (rp->u2.s.t_or_d == 't') {
	strcpy (frec->u1.t.vid, rp->u2.s.u1.t.vid);
	strcpy (frec->u1.t.fseq, rp->u2.s.u1.t.fseq);
	frec->t_or_d = 't';
	num_frecs++;
   }
   else {
      	strcpy (frec->u1.d.xfile, rp->u2.s.u1.d.xfile);
	frec->t_or_d = 'd';
	num_xrecs++;
   }
          
   if (file_list == NULL) {
        file_list = frec;
       	file_last = frec;
   }
   else {
        file_last->next = frec;
       	file_last = frec;
   }
}

/* Function to create record for each pool group found */

create_pool_list (rp)
struct acctstage *rp;
{
   struct pool_inf *pf;				/* pointer to pool_inf record */

   pf = (struct pool_inf*) calloc (1, sizeof (struct pool_inf));
   pf->next = NULL;
   strcpy (pf->poolname, rp->u2.s.poolname);
   if (pool_list == NULL) {
	pool_list = pf;
	pool_last = pf;
   }
   else {
	pool_last->next = pf;
	pool_last = pf;
   }
}



/* Function to create record for each stagein request */

create_stglist (rp)
struct acctstage *rp;
{
   struct stg_inf *sp;				/* pointer to stg_inf record */
	
   sp = (struct stg_inf*) calloc (1, sizeof (struct stg_inf));
   sp->next = NULL;
   sp->reqid = rp->reqid;
   if (stage_list == NULL) {
	stage_list = sp;
  	stg_last = sp;
   }
   else {
	stg_last->next = sp;
	stg_last = sp;
   }
}


/* Function to covert date and time into seconds */

time_t
cvt_datime (arg)
char *arg;
{
   time_t current_time;
   static int lastd[12] = {31,29,31,30,31,30,31,31,30,31,30,31};
   int n;
   struct tm t;
   struct tm *tm;

   memset ((char *) &t, 0, sizeof(struct tm));
   time (&current_time);		/* Get current time */
   tm = localtime (&current_time);
   n = sscanf (arg, "%2d%2d%2d%2d%2d", &t.tm_mon, &t.tm_mday, &t.tm_hour,
	&t.tm_min, &t.tm_year);
   if (n < 4) return (-1);
   if (n == 4) t.tm_year = tm->tm_year;
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

/* Function to enter file record details for each FILS stagein command */

enter_filc_details (rp, accthdr)
struct acctstage *rp;
struct accthdr accthdr;
{
   int found = 0;			/* record found flag */
   struct file_inf *frecord;		/* pointer to file_inf record */

/* search through file list for record relating to same file */ 

   frecord = file_list;
   while (frecord != NULL) {
	if (frecord->t_or_d == 't') {
		if ((strcmp (rp->u2.s.u1.t.vid, frecord->u1.t.vid) == 0) && 
		   (strcmp (rp->u2.s.u1.t.fseq, frecord->u1.t.fseq) == 0)&&
		   (strcmp (rp->u2.s.poolname, frecord->poolname) == 0)) {
			found = 1;
			break;
		}
	}
	else if (frecord->t_or_d == 'd') {
		 if ((strcmp (rp->u2.s.u1.d.xfile, frecord->u1.d.xfile) == 0) &&
		     (strcmp (rp->u2.s.poolname, frecord->poolname) == 0)) {
			found = 1;
			break;
		}
	}
	frecord = frecord->next;
   }

/* if record found reset stage_status to 0 and calculate staged in time if staged */
/* in during the relevant time period */

   if (found) {
	if (frecord->last_stgin > 0 && frecord->stage_status == 1) {
		if (rp->uid == 0) {		/* garbage collected */
			frecord->total_stgin_gc = frecord->total_stgin_gc + 
			(accthdr.timestamp - frecord->last_stgin);
			frecord->num_periods_gc++;
		}	
		else {				/* user cleared */
			frecord->total_stgin_uc = frecord->total_stgin_uc +
			(accthdr.timestamp - frecord->last_stgin);
			frecord->num_periods_uc++;
		}
	}
	frecord->stage_status = 0;
   }
}


/* Function to enter details of each file clear command */

enter_fils_details (rp, accthdr)
struct acctstage *rp;
struct accthdr accthdr;
{
   int found = 0;			/* record found flag */
   int match = 0;			/* stg_inf record match flag */
   struct file_inf *frecord;		/* pointer to file_inf record */

/* search file list for matching file and pool */

   frecord = file_list;
   while (frecord != NULL) {
	if (rp->u2.s.t_or_d == 't' && frecord->t_or_d == 't') {
		if ((strcmp (rp->u2.s.u1.t.vid, frecord->u1.t.vid) == 0) && 
		   (strcmp (rp->u2.s.u1.t.fseq, frecord->u1.t.fseq) == 0) &&
		   (strcmp (rp->u2.s.poolname, frecord->poolname) == 0)) {
			found = 1;
			break;
		}
	}
	else if (rp->u2.s.t_or_d == 'd' && frecord->t_or_d == 'd') {
		if ((strcmp (rp->u2.s.u1.d.xfile, frecord->u1.d.xfile) == 0) &&
		  (strcmp (rp->u2.s.poolname, frecord->poolname) == 0)){
			found = 1;
			break;
		}
	}
	frecord = frecord->next;
   }

/* if a matching record is not found then create one, else increment num_remounts */

   if (!found){
	create_filelist (rp);
	frecord = file_last;
   }	
   else if (rp->u2.s.nbaccesses == 1) num_remounts++; 

/* if number of accesses is 1 and no error on exit of command, set last stgin time */
/* to timestamp of the current record */	

   if (rp->u2.s.nbaccesses == 1 && (rp->exitcode == 0 || rp->exitcode == 193 
      || rp->exitcode == 194 || rp->exitcode == 195 || rp->exitcode == 197)) 
	frecord->last_stgin = accthdr.timestamp;

   if (rp->retryn == 0) {
 	match = match_2_stgin (rp);
	frecord->num_accesses++;
	frecord->stage_status = 1;
	if (match) srec_match->num_files++;
   }
}
			

/* Function to enter details for each pool */

enter_pool_details (rp)
struct acctstage *rp;
{
   struct pool_inf *pf;			/* pointer to pool_inf record */
   int found = 0;

   pf = pool_list;
   while (pf != NULL) {
        if (strcmp (pf->poolname, rp->u2.s.poolname) == 0) {
               	found = 1;
                break;
        }
        else pf = pf->next;
   }

   if (strcmp (rp->u2.s.poolname, "") != 0 && !found)
        create_pool_list (rp);
}

/* Function to read each record from the accounting file */

getacctrec (fd_acct, accthdr, buf,swapped)
int fd_acct;
struct accthdr *accthdr;
char *buf;
int *swapped;
{
   int c;

   if ((c = read (fd_acct,accthdr,sizeof(struct accthdr))) != sizeof(struct accthdr)) {
	if (c == 0) return (0);
	if (c > 0)
		fprintf (stderr, "read returns %d\n", c);
	else
		fprintf (stderr, "read error : %s\n", sys_errlist[errno]);
	exit (SYERR);
   }


/* If package is > 255 then byte order needs swopping */

   if (accthdr->package > 255) {
        swap_it (accthdr->package);
        swap_it (accthdr->len);
        swap_it (accthdr->timestamp);
        *swapped = 1;
   }

   if ((c = read (fd_acct, buf, accthdr->len)) != accthdr->len) {
	if (c >= 0)
		fprintf (stderr, "read returns %d\n",c);
        else
                fprintf (stderr, "read error : %s\n", sys_errlist[errno]);
        exit (SYERR);
   }
   return (accthdr->len);
}

/* Function to match a FILS request with a stagein request */

match_2_stgin (rp)
struct acctstage *rp;
{
   int matched =0;				/* record matched flag */
   struct stg_inf *srec;			/* pointer to stg_inf record */
	
   srec = stage_list;
   while (srec != NULL) {
	if (rp->reqid == srec->reqid) {
		matched = 1;
		srec_match = srec;
		break;
	}
	else srec = srec->next;
   }
   return matched;
}

	
/* Function to print out a record */

print_fdetails (frecord)
struct file_inf *frecord;
{
   struct passwd *pwd;				/* password structure */
   struct group *grp;				/* group structure */
   float avg = 0.0;				/* average life */

/* get the password and group name for each record */

   pwd = getpwuid (frecord->uid);  
   grp = getgrgid (frecord->gid);

/* Calculate the average life time of the file. If the file has been garbage */
/* collected then average life is calculated using the garbage collected info */
/* only.  If the file has not been garbage collected, then the average life */
/* is calculated using the user cleared information */

   if (frecord->num_periods_gc > 0) 
  	avg = (frecord->total_stgin_gc / 3600.0) / frecord->num_periods_gc;
   else if (frecord->num_periods_uc > 0)
	avg = (frecord->total_stgin_uc / 3600.0) / frecord->num_periods_uc;
   else avg = 0.0;

   if ((frecord->num_periods_gc > 0 && frecord->total_stgin_gc > 0) ||
	(frecord->total_stgin_uc > 0 && frecord->num_periods_uc > 0))
       	printf ("\n%8s %8s    %8.2f  %8d %12s %8s", frecord->u1.t.vid,
                frecord->u1.t.fseq, avg, frecord->num_accesses, pwd->pw_name, 
                grp->gr_name);
   else if (frecord->stage_status == 1)
        printf ("\n%8s %8s    %8s  %8d %12s %8s", frecord->u1.t.vid,
                frecord->u1.t.fseq,"      nc", frecord->num_accesses, 
		pwd->pw_name, grp->gr_name);
   else if (frecord->last_stgin == 0)
        printf ("\n%8s %8s    %8s  %8d %12s %8s", frecord->u1.t.vid,
                frecord->u1.t.fseq,"      ac", frecord->num_accesses, 
		pwd->pw_name, grp->gr_name);
   else printf ("\n%8s %8s    %8.2f  %8d %12s %8s", frecord->u1.t.vid,
                frecord->u1.t.fseq, 0, frecord->num_accesses, pwd->pw_name, 
                grp->gr_name);
}

/* Function to print out global statistics */

print_globstat (starttime, endtime, num_mounts, num_multireqs)
time_t starttime;
time_t endtime;
int num_mounts;
int num_multireqs;
{
   char hostname[MAXHOSTNAMELEN];
   int num_mounts_avoided = 0;

   gethostname (hostname, MAXHOSTNAMELEN);
   printf ("\t%s Stager statistics", hostname);
   print_time_interval (starttime, endtime);
   printf ("\nCommand    No Reqs Success Warning Userr Syserr Unerr Conferr ");
   printf ("ENOSPC Cleared Kill Other\n\n");
   printf ("stagein    %7d %7d %7d %5d %6d %5d %7d %6d %7d %4d %5d\n",
           nb_req[1], rc[1][0], rc[1][7], rc[1][1], rc[1][2], rc[1][3],
           rc[1][4], rc[1][5], rc[1][6], rc[1][8], rc[1][9]);
   printf ("stageout   %7d %7d %7d %5d %6d %5d %7d %6d %7d %4d %5d\n",
           nb_req[2], rc[2][0], rc[2][7], rc[2][1], rc[2][2], rc[2][3],
           rc[2][4], rc[2][5], rc[2][6], rc[2][8], rc[2][9]);
   printf ("stagewrt   %7d %7d %7d %5d %6d %5d %7d %6d %7d %4d %5d\n",
           nb_req[3], rc[3][0], rc[3][7], rc[3][1], rc[3][2], rc[3][3],
           rc[3][4], rc[3][5], rc[3][6], rc[3][8], rc[3][9]);
   printf ("stageput   %7d %7d %7d %5d %6d %5d %7d %6d %7d %4d %5d\n",
           nb_req[4], rc[4][0], rc[4][7], rc[4][1], rc[4][2], rc[4][3],
           rc[4][4], rc[4][5], rc[4][6], rc[4][8], rc[4][9]);
   printf ("stagealloc %7d %7d %7d %5d %6d %5d %7d %6d %7d %4d %5d\n",
           nb_req[11], rc[11][0], rc[11][7], rc[11][1], rc[11][2], rc[11][3],
           rc[11][4], rc[11][5], rc[11][6], rc[11][8], rc[11][9]);
   printf ("stageqry   %7d\n", nb_req[5]);
   printf ("stageclr   %7d\n", nb_req[6]);
   printf ("stageupdc  %7d\n", nb_req[8]);
   printf ("stageinit  %7d\n", nb_req[9]);
   printf ("stagecat   %7d\t\n", nb_req[10]);
   printf ("restart(s) %7d\t\n", nb_restart);
   printf ("\nStagein Statistics :\n");
   printf ("\tNumber of mounts\t\t\t%d\n", num_mounts);
   printf ("\tNumber of remounts\t\t\t%d\n", num_remounts);
   num_mounts_avoided = rc[1][0] - num_mounts;
   if (num_mounts_avoided < 0 ) num_mounts_avoided = 0;
   printf ("\tNumber of mounts avoided by stager\t%d\n", num_mounts_avoided);
   printf ("\tNumber of multifile requests\t\t%d\n", num_multireqs);
}

/* Function to print out the time interval */

print_time_interval (starttime, endtime)
time_t starttime;
time_t endtime;
{
   struct tm *tm;

   tm = localtime (&starttime);
   printf (" (%02d/%02d/%02d %02d:%02d:%02d",
           tm->tm_mday, tm->tm_mon+1, tm->tm_year, tm->tm_hour, tm->tm_min, 
	   tm->tm_sec);
   tm = localtime (&endtime);
   printf ("  -  %02d/%02d/%02d %02d:%02d:%02d)\n",
           tm->tm_mday, tm->tm_mon+1, tm->tm_year, tm->tm_hour, tm->tm_min, 
           tm->tm_sec);
}


/*Function to print out a Xrecord*/

print_xdetails (frecord)
struct file_inf * frecord;
{
   struct passwd *pwd;				/* password structure */
   struct group *grp;				/* group structure */
   float avg = 0.0;				/* average life */

/* get the password and group name for each record */
	
   pwd = getpwuid (frecord->uid);
   grp = getgrgid (frecord->gid);

/* Calculate the average life time of the file. If the file has been garbage */
/* collected then average life is calculated using the garbage collected info */
/* only.  If the file has not been garbage collected, then the average life */
/* is calculated using the user cleared information */

   if (frecord->num_periods_gc > 0) 
	avg = (frecord->total_stgin_gc / 3600.0) / frecord->num_periods_gc;
   else if (frecord->num_periods_uc > 0)
	avg = (frecord->total_stgin_uc / 3600.0) / frecord->num_periods_uc;
   else avg = 0.0;

   if ((frecord->num_periods_gc > 0 && frecord->total_stgin_gc > 0) ||
	(frecord->num_periods_uc > 0 && frecord->total_stgin_uc > 0))
	printf ("\n%-35s %8.2f  %8d   %15s %15s", frecord->u1.d.xfile,
        	avg, frecord->num_accesses, pwd->pw_name, grp->gr_name);
   else if (frecord->stage_status == 1)
        printf ("\n%-35s %8s  %8d   %15s %15s", frecord->u1.d.xfile,
		"      nc", frecord->num_accesses, pwd->pw_name, grp->gr_name);
   else if (frecord->last_stgin == 0)
        printf ("\n%-35 %8s  %8d   %15s %15s", frecord->u1.d.xfile,
		"      ac", frecord->num_accesses, pwd->pw_name, grp->gr_name);
   else printf ("\n%-35s %8.2f  %8d   %15s %15s", frecord->u1.d.xfile,
		0, frecord->num_accesses, pwd->pw_name, grp->gr_name);
}


/* Function to create  pool information */

sort_poolinf ()
{
   struct pool_inf *pf;				/* pointer to pool_inf record */
   struct file_inf *frecord;			/* pointer to file_inf record */

/* for each record, match to its pool and set the relevant variables in the pool */
/* structure  */

   frecord = file_list;
   while (frecord != NULL) {
	pf = pool_list;
	while (pf != NULL) {
	   if (strcmp (frecord->poolname, pf->poolname) == 0){
		if (frecord->stage_status == 1 && frecord->last_stgin == 0) pf->acc++;
		else if (frecord->stage_status == 1 && frecord->last_stgin > 0) 
			pf->stg_acc++;
		else if (frecord->stage_status == 0 && frecord->last_stgin > 0) 
			pf->stg_acc_clrd++; 
		else if (frecord->stage_status == 0 && frecord->last_stgin == 0) 				pf->acc_clrd++;
		if (frecord->num_periods_gc>0 && frecord->total_stgin_gc > 0) {
			pf->total_stged_files++;
			pf->total_avg_life = pf->total_avg_life + 
				((frecord->total_stgin_gc / 3600.0) /
			 	frecord->num_periods_gc);
		}
		if (frecord->t_or_d == 't') pf->num_frecs++;
		else pf->num_xrecs++;
		pf->num_files++;
		pf->total_accesses = pf->total_accesses + frecord->num_accesses;
		break;
           }
  	   else pf = pf->next;
	}
	frecord = frecord->next;
   }
}


/* Function to create pool records and print results */

print_poolstat (tflag, aflag, pflag, poolname)
int tflag;
int aflag;
int pflag;
char poolname[MAXPOOLNAMELEN];
{
int i; 						/* counter */ 
int fi = 0; 					/* counter for tape files */
int xi = 0;					/* counter for disk files */
struct frec_nbaccs *faccs;		/* arrays to store address of each record */
struct frec_nbaccs *xaccs;		/* along with either the average life or the */ struct frec_avg *favg;			/* number of accesses and poolname.  These */
struct frec_avg *xavg;			/* arrays are used to sort the data */
char current_pool[MAXPOOLNAMELEN];		/* current pool name */
struct pool_inf *pf;				/* pointer to pool_inf record */
struct file_inf *frecord;			/* pointer to file_inf record */
int comp ();
int comp2 ();

/* If a sort criteria has been given then create the relevant array to store the */
/* data */

   if (aflag && num_frecs > 0)
	faccs = (struct frec_nbaccs*) calloc (num_frecs, sizeof (struct frec_nbaccs));
   else if (tflag && num_frecs > 0)
        favg = (struct frec_avg*) calloc (num_frecs, sizeof (struct frec_avg));
   if (aflag && num_xrecs > 0)
        xaccs = (struct frec_nbaccs*) calloc (num_xrecs, sizeof (struct frec_nbaccs));
   else if (tflag && num_xrecs > 0)
        xavg = (struct frec_avg*) calloc (num_xrecs, sizeof (struct frec_avg));

/* once arrays have been created cycle through the file records and copy the */
/* relevant data into the sort array */
		
   if (aflag || tflag) {
        frecord = file_list;
        while (frecord != NULL){
	   if (num_frecs > 0 && frecord->t_or_d == 't') {
               	if (aflag) {
                       	faccs[fi].rec = frecord;
                        strcpy (faccs[fi].poolname, frecord->poolname);
                        faccs[fi].nbaccesses = frecord->num_accesses;
                }
                else if (tflag) {
                        favg[fi].rec = frecord;
                        strcpy (favg[fi].poolname, frecord->poolname);
                        if (frecord->num_periods_gc > 0 && frecord->total_stgin_gc > 0)
                   		favg[fi].avg_life = (frecord->total_stgin_gc / 3600.0)
						/ frecord->num_periods_gc;
                        else if (frecord->num_periods_uc > 0 && frecord->total_stgin_uc
				> 0)
                                favg[fi].avg_life = (frecord->total_stgin_uc / 3600.0)
						/ frecord->num_periods_uc;
                        else favg[fi].avg_life = 0.0;
                }
		fi++;
           }
           if (num_xrecs > 0 && frecord->t_or_d == 'd') {
                if (aflag) {
                        xaccs[xi].rec = frecord;
                        strcpy (xaccs[xi].poolname, frecord->poolname);
                        xaccs[xi].nbaccesses = frecord->num_accesses;
                }
                else if (tflag) {
                        xavg[xi].rec = frecord;
                        strcpy (xavg[xi].poolname, frecord->poolname);
                        if (frecord->num_periods_gc > 0 && frecord->total_stgin_gc > 0)
           	             xavg[xi].avg_life = (frecord->total_stgin_gc / 3600.0)
						/ frecord->num_periods_gc;
                        else if (frecord->num_periods_uc > 0 && frecord->total_stgin_uc
				> 0)
                             xavg[xi].avg_life = (frecord->total_stgin_uc / 3600.0)
						/ frecord->num_periods_uc;
                        else xavg[xi].avg_life = 0.0;
               }
	       xi++;
           }
           frecord = frecord->next;
	}
   }

/* if sort criteria specified sort the arrays into ascending order */

   if  (aflag && num_frecs > 0) 
	qsort (faccs, num_frecs, sizeof (struct frec_nbaccs), comp);
   else if (tflag && num_frecs > 0) 
        qsort (favg, num_frecs, sizeof (struct frec_avg), comp2);
   if (aflag && num_xrecs > 0) 
        qsort (xaccs, num_xrecs, sizeof (struct frec_nbaccs), comp);
   else if (tflag && num_xrecs > 0) 
        qsort (xavg, num_xrecs, sizeof (struct frec_avg), comp2);

/* enter the pool information into the pool)inf records */

   sort_poolinf();	

/* print out pool statistics */

   pf = pool_list;
   while (pf != NULL) {
	strcpy (current_pool, pf->poolname);
	if (pflag && strcmp (current_pool,poolname) != 0) continue;
        printf ("\f\nFile Request Details for Pool : %10s\n\n", current_pool);
        printf ("Number of requests started before time period began\t\t\t%d\n",
                 pf->acc_clrd + pf->acc);
        printf ("Out of these:\tNumber accessed before being cleared\t\t\t%d\n",
                 pf->acc_clrd);
        printf ("\t\tNumber accessed but not cleared during time period\t%d\n",
                 pf->acc);
        printf ("\nNumber of requests started after beginning of time period\t\t%d\n",
	         pf->stg_acc+pf->stg_acc_clrd);
        printf ("Out of these:\tNumber accessed but not cleared\t\t\t\t%d\n",
                 pf->stg_acc);
        printf ("\t\tNumber accessed and then cleared\t\t\t%d\n", pf->stg_acc_clrd);

        if (pf->num_files > 0)
                 printf ("\nAverage number of file accesses \t:\t %8.2f",
                           pf->total_accesses/pf->num_files);
        if (pf->total_avg_life > 0.0 && pf->total_stged_files > 0)
                 printf ("\n Average lifetime of a staged file \t:\t %8.2f hours\n",
                          (pf->total_avg_life/pf->total_stged_files)/3600.0);

/* if sort criteria given print out file details */

	if (aflag && pf->num_frecs > 0) {
                 printf ("\n\nLifetime and accesses per file sorted by number of ");
                 printf ("accesses\n");
                 printf ("\nTape VID     Fseq    Avg Life   Number    Login Name");
                 printf ("     Group");
                 printf ("\n                        (hrs)  Accesses");
                 for (i = 0; i < num_frecs; i++){
                    	frecord = faccs[i].rec;
                        if (strcmp (frecord->poolname, current_pool) == 0)
        			print_fdetails (frecord);
                 }
	}
	else if (tflag && pf->num_frecs > 0) {
                 printf ("\n\nLifetime and accesses per file sorted by Average ");
                 printf ("Lifetime\n");
                 printf ("\nTape VID     Fseq    Avg Life   Number    Login Name ");
                 printf ("    Group");
                 printf ("\n                        (hrs)  Accesses");
                 for (i = 0; i<num_frecs ; i++) {
                        frecord = favg[i].rec;
                        if ((strcmp (frecord->poolname, current_pool) == 0) &&
			   (frecord->total_stgin_gc > 0 || frecord->total_stgin_uc > 0))
                        	print_fdetails (frecord);
                 }
        }

	if (aflag && pf->num_xrecs > 0) {
                 printf ("\n\nLifetime and accesses per Xfile sorted by number of ");
                 printf ("accesses");
                 printf ("\nXFile Name                          Avg Life   Number");  
                 printf ("        Login Name      Group ID ");
                 printf ("\n                                       (hrs)  Accesses");
                 for(i=0; i < num_xrecs; i++) {
                        frecord = xaccs[i].rec;
                        if (strcmp (frecord->poolname, current_pool) == 0)
                                 print_xdetails (frecord);
                 }
	}
	else if (tflag && pf->num_xrecs > 0) {
                 printf ("\n\nLifetime and accesses per Xfile sorted by average");
                 printf ("lifetime");
                 printf ("\nXFile Name                          Avg Life   Number");        
                 printf ("        Login Name      Group ID ");
		 printf ("\n                                       (hrs)  Accesses");
		 for(i=0; i< num_xrecs; i++) {
                        frecord = xavg[i].rec;
                        if ((strcmp (frecord->poolname, current_pool) == 0) &&
		       	   (frecord->total_stgin_gc > 0 || frecord->total_stgin_uc > 0))
                                 print_xdetails (frecord);
                 }
        }
  	printf("\n"); 
	pf = pf->next;
   }
}

/* Functions to sort records into ascending order */

int comp (a, b)
struct frec_nbaccs *a;
struct frec_nbaccs *b;
{
   int c = 0;

   if (c = strcmp(a->poolname, b->poolname)) return (c);
   else if (a->nbaccesses <  b->nbaccesses) return (1);
   else if (a->nbaccesses ==  b->nbaccesses) return (0);
   else return (-1); 
}

int comp2 (a, b)
struct frec_avg  *a;
struct frec_avg  *b;
{
   int c = 0;

   if (c = strcmp(a->poolname, b->poolname)) return (c);
   else if (a->avg_life < b->avg_life) return (1);
   else if (a->avg_life == b->avg_life) return (0);
   else return (-1);
}

swap_fields (rp)
struct acctstage *rp;
{
   swap_it (rp->subtype);
   swap_it (rp->uid);
   swap_it (rp->gid);
   swap_it (rp->reqid);
   swap_it (rp->req_type);
   swap_it (rp->retryn);
   swap_it (rp->exitcode);
   swap_it (rp->u2.s.nbaccesses);
}

usage (cmd)
char *cmd;
{
   fprintf (stderr, "usage: %s ", cmd);
   fprintf (stderr, "%s",
   "[-e end_time][-f accounting_file][-s start_time][-p pool_name][-S a or t]\n");
}


