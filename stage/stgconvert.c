/*
 * $Id: stgconvert.c,v 1.1 1999/11/29 15:27:43 jdurand Exp $
 */

/*
 * Conversion Program between SHIFT stager catalog and CASTOR/Cdb stager database
 */

/* ============== */
/* System headers */
/* ============== */
#ifndef _WIN32
#include <unistd.h>
#endif
#include <sys/types.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

/* =============================================================== */
/* Local headers for threads : to be included before ANYTHING else */
/* =============================================================== */
#include "Cdb_api.h"                /* CASTOR Cdb Interface */
#include "Cstage_db.h"              /* Generated STAGE/Cdb header */
#include "stage.h"                  /* SHIFT's headerx */
#include "serrno.h"                 /* CASTOR's serrno */

/* =============== */
/* Local variables */
/* =============== */
static char *sccsid = "@(#)$RCSfile: stgconvert.c,v $ $Revision: 1.1 $ $Date: 1999/11/29 15:27:43 $ CERN IT-PDP/DM Jean-Damien Durand / Jean-Philippe Baud";

/* ====== */
/* Macros */
/* ====== */
#ifdef _WIN32
#define FILE_OFLAG ( O_RDWR | O_CREAT | O_BINARY )
#else
#define FILE_OFLAG ( O_RDWR | O_CREAT )
#endif
#ifdef _WIN32
#define FILE_MODE ( _S_IREAD | _S_IWRITE )
#else
#define FILE_MODE ( S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH )
#endif

#define SHIFT_TO_CASTOR -1
#define CASTOR_TO_SHIFT 1

#define CDB_USERNAME "Cdb_Stage_User"
#define CDB_PASSWORD "Cdb_Stage_Password"

int main(argc,argv)
     int argc;
     char **argv;
{
  /* For options */
  extern char *optarg;
  extern int optind, opterr, optopt;
  int errflg;
  int c;
  int convert_direction = 0;         /* 1 : SHIFT -> CASTOR, -1 : CASTOR -> SHIFT */
  int help = 0;                      /* 1 : help wanted */
  char *stgcat = NULL;               /* SHIFT's stager catalog path */
  int stgcat_fd = -1;
  char *stgpath = NULL;              /* SHIFT's stager link catalog path */
  int stgpath_fd = -1;
  struct stat stgcat_statbuff, stgpath_statbuff;
  char *Cdb_username = NULL;
  char *Cdb_password = NULL;
  int rc;
  Cdb_sess_t Cdb_session;
  int Cdb_session_opened = 0;
  Cdb_db_t Cdb_db;
  int Cdb_db_opened = 0;
  char *error = NULL;
  struct stgcat_entry *stcp = NULL;  /* stage catalog pointer */
  struct stgcat_entry *stcs = NULL;  /* start of stage catalog pointer */
  struct stgcat_entry *stce = NULL;  /* end of stage catalog pointer */
  struct stgpath_entry *stpp = NULL; /* stage path catalog pointer */
  struct stgpath_entry *stps = NULL; /* start of stage path catalog pointer */
  struct stgpath_entry *stpe = NULL; /* end of stage path catalog pointer */
  struct Cstgcat_common common;
  struct Cstgcat_tape tape;
  struct Cstgcat_disk disk;
  struct Cstgcat_hsm hsm;
  struct Cstgcat_link link;
  Cdb_off_t Cdb_offset;

  while ((c = getopt(argc,argv,"c:hu:p:v")) != EOF) {
    switch (c) {
    case 'h':
      help = 1;
      break;
    case 'c':
      convert_direction = atoi(optarg);
      break;
    case 'u':
      Cdb_username = optarg;
      break;
    case 'p':
      Cdb_password = optarg;
      break;
    case 'v':
      printf("%s\n",sccsid);
      return(EXIT_SUCCESS);
    case '?':
      ++errflg;
      printf("Unknown option\n");
      break;
    default:
      ++errflg;
      printf("?? getopt returned character code 0%o (octal) 0x%lx (hex) %d (int) '%c' (char) ?\n"
          ,c,(unsigned long) c,c,(char) c);
      break;
    }
  }

  if (errflg != 0) {
      printf("### getopt error\n");
      stgconvert_usage();
      return(EXIT_FAILURE);
  }

  if (help != 0) {
    stgconvert_usage();
    return(EXIT_SUCCESS);
  }

  if (convert_direction != SHIFT_TO_CASTOR && 
      convert_direction != CASTOR_TO_SHIFT) {
    printf("?? -c option is REQUIRED and HAS to be either %d (SHIFT->CASTOR) or %d (CASTOR->SHIFT) , nothing else.\n",
            SHIFT_TO_CASTOR,CASTOR_TO_SHIFT);
    stgconvert_usage();
    return(EXIT_FAILURE);
  }

  if (optind >= argc || optind > (argc - 2)) {
    printf("?? Exactly two parameters are requested\n");
    stgconvert_usage();
    return(EXIT_FAILURE);
  }

  stgcat = argv[optind];
  stgpath = argv[optind+1];

  /* Open the catalogs first in read-mode first for safety */
  if ((stgcat_fd = open(stgcat, FILE_OFLAG, FILE_MODE)) < 0) {
    printf("### open of %s error, %s\n",stgcat,strerror(errno));
    return(EXIT_FAILURE);
  }
  if ((stgpath_fd = open(stgpath, FILE_OFLAG, FILE_MODE)) < 0) {
    printf("### open of %s error, %s\n",stgpath,strerror(errno));
    close(stgcat_fd);
    return(EXIT_FAILURE);
  }

  /* == From now on we will goto stgconvert_return for any exit == */

  if (fstat(stgcat_fd, &stgcat_statbuff) < 0) {
    printf("### fstat on %s error, %s\n"
            ,stgcat
            ,strerror(errno));
    rc = EXIT_FAILURE;
    goto stgconvert_return;
  }
  if (fstat(stgpath_fd, &stgpath_statbuff) < 0) {
    printf("### fstat on %s error, %s\n"
            ,stgpath
            ,strerror(errno));
    rc = EXIT_FAILURE;
    goto stgconvert_return;
  }

  /* If the user said CASTOR -> SHIFT conversion and one of those files is of length > 0   */
  /* he will overwrite existing non-zero length files. We then check if that's really what */
  /* he wants to do.                                                                       */
  if (convert_direction == CASTOR_TO_SHIFT) {
    if (stgcat_statbuff.st_size > 0 || stgpath_statbuff.st_size > 0) {
      int answer;

      printf("### WARNING : You are going to truncate a SHIFT catalog that is of non-zero length\n");
      printf("### Current stgcat length  : 0x%lx\n",(unsigned long) stgcat_statbuff.st_size);
      printf("### Current stgpath length : 0x%lx\n",(unsigned long) stgpath_statbuff.st_size);
      printf("### Do you really want to proceed (y/n) ? ");
      
      answer = getchar();
      if (answer != 'y' && answer != 'Y') {
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }
      /* User said yes. This means that we are going to truncate the files if they already exist */
      /* and have non-zero length.                                                               */
    }
  }

  /* We anyway truncate the files */
  if (ftruncate(stgcat_fd, (size_t) 0) != 0) {
    printf("### stat on %s error, %s\n"
            ,stgcat
            ,strerror(errno));
    rc = EXIT_FAILURE;
    goto stgconvert_return;
  }
  if (ftruncate(stgpath_fd, (size_t) 0) != 0) {
    printf("### stat on %s error, %s\n"
            ,stgpath
            ,strerror(errno));
    rc = EXIT_FAILURE;
    goto stgconvert_return;
  }

  /* We open a connection to Cdb */
  if (Cdb_username == NULL) {
    Cdb_username = CDB_USERNAME;
  }
  if (Cdb_password == NULL) {
    Cdb_password = CDB_PASSWORD;
  }
  if (Cdb_login(Cdb_username,Cdb_password,&Cdb_session) != 0) {
    printf("### Cdb_login(\"%s\",\"%s\",&session) error, %s\n"
            ,Cdb_username
            ,Cdb_password
            ,sstrerror(serrno));
    if (Cdb_error(&Cdb_session,&error) == 0) {
      printf("--> more info:\n%s",error);
    }
    rc = EXIT_FAILURE;
    goto stgconvert_return;
    
  }

  Cdb_session_opened = -1;

  /* We open the database */
  if (Cdb_open(&Cdb_session,"Cstage",&Cdb_Cstage_interface,&Cdb_db) != 0) {
    printf("### Cdb_open(&Cdb_session,\"Cstage\",&Cdb_Cstage_interface,&Cdb_db) error, %s\n"
            ,sstrerror(serrno));
    if (Cdb_error(&Cdb_session,&error) == 0) {
      printf("--> more info:\n%s",error);
    }
    printf("### PLEASE SUBMIT THE STAGER DATABASE WITH THE TOOL Cdb_create IF NOT ALREADY DONE.\n");
    rc = EXIT_FAILURE;
    goto stgconvert_return;
  }

  Cdb_db_opened = -1;

  if (convert_direction == SHIFT_TO_CASTOR) {
    /* SHIFT -> CASTOR conversion */

    if (stgcat_statbuff.st_size > 0) {
      if ((stcs = malloc(stgcat_statbuff.st_size)) == NULL) {
        printf("### malloc error, %s\n",strerror(errno));
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }
      if (read(stgcat_fd,stcs,stgcat_statbuff.st_size) != stgcat_statbuff.st_size) {
        printf("### read error, %s\n",strerror(errno));
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }
    }
    if (stgpath_statbuff.st_size > 0) {
      if ((stps = malloc(stgpath_statbuff.st_size)) == NULL) {
        printf("### malloc error, %s\n",strerror(errno));
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }
      if (read(stgpath_fd,stps,stgpath_statbuff.st_size) != stgpath_statbuff.st_size) {
        printf("### read error, %s\n",strerror(errno));
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }
    }

    stce = stcs;
    stce += stgcat_statbuff.st_size;
    stpe = stps;
    stpe += stgpath_statbuff.st_size;

    /* We loop on the SHIFT's stgcat catalog */
    for (stcp = stcs; stcp < stce; stcp++) {
      if (stcp2Cdb(stcp,&common,&tape,&disk,&hsm) != 0) {
        printf("### stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d\n",stcp->reqid);
      }

      /* We delete the eventual previous entry with the same reqid in the master table */
      if (Cdb_keyfind(&Cdb_db,"Cstage_common","Cstage_common_per_reqid","w",&common,&Cdb_offset) != 0) {
        if (Cdb_delete(&Cdb_db,"Cstage_common",&Cdb_offset) != 0) {
          printf("### Cannot delete entry with reqid = %d in Cdb's table \"Cstage_common\" (%s)\n"
                  ,stcp->reqid
                  ,sstrerror(serrno));
        }
      }

      switch (stcp->t_or_d) {
      case 't':
        /* We delete the eventual previous entry with the same reqid in the tape table */
        if (Cdb_keyfind(&Cdb_db,"Cstage_tape","Cstage_tape_per_reqid","w",&tape,&Cdb_offset) != 0) {
          if (Cdb_delete(&Cdb_db,"Cstage_tape",&Cdb_offset) != 0) {
            printf("### Cannot delete entry with reqid = %d in Cdb's table \"Cstage_tape\" (%s)\n"
                   ,stcp->reqid
                   ,sstrerror(serrno));
          }
        }
        break;
      case 'd':
        /* We delete the eventual previous entry with the same reqid in the disk table */
        if (Cdb_keyfind(&Cdb_db,"Cstage_disk","Cstage_disk_per_reqid","w",&disk,&Cdb_offset) != 0) {
          if (Cdb_delete(&Cdb_db,"Cstage_disk",&Cdb_offset) != 0) {
            printf("### Cannot delete entry with reqid = %d in Cdb's table \"Cstage_disk\" (%s)\n"
                   ,stcp->reqid
                   ,sstrerror(serrno));
          }
        }
        break;
      case 'm':
        /* We delete the eventual previous entry with the same reqid in the hsm table */
        if (Cdb_keyfind(&Cdb_db,"Cstage_hsm","Cstage_hsm_per_reqid","w",&hsm,&Cdb_offset) != 0) {
          if (Cdb_delete(&Cdb_db,"Cstage_hsm",&Cdb_offset) != 0) {
            printf("### Cannot delete entry with reqid = %d in Cdb's table \"Cstage_hsm\" (%s)\n"
                   ,stcp->reqid
                   ,sstrerror(serrno));
          }
        }
        break;
      default:
        printf("### stcp->t_or_d == '%c' unrecognized. Please update this program.\n",stcp->t_or_d);
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }

      /* We insert this record */
      if (Cdb_insert(&Cdb_db,"Cstage_common",NULL,&common,&Cdb_offset) != 0) {
        printf("### Cannot insert entry with reqid = %d in Cdb's table \"Cstage_common\" (%s)\n"
                ,stcp->reqid
                ,sstrerror(serrno));
      } else {
        printf("--> reqid = %d inserted in \"Cstage_common\" at offset 0x%lx\n",stcp->reqid,(unsigned long) Cdb_offset);
      }

      switch (stcp->t_or_d) {
      case 't':
        if (Cdb_insert(&Cdb_db,"Cstage_tape",NULL,&tape,&Cdb_offset) != 0) {
          printf("### Cannot insert entry with reqid = %d in Cdb's table \"Cstage_tape\" (%s)\n"
                 ,stcp->reqid
                 ,sstrerror(serrno));
        } else {
          printf("--> reqid = %d inserted in \"Cstage_tape\" at offset 0x%lx\n",stcp->reqid,(unsigned long) Cdb_offset);
        }
        break;
      case 'd':
        if (Cdb_insert(&Cdb_db,"Cstage_disk",NULL,&disk,&Cdb_offset) != 0) {
          printf("### Cannot insert entry with reqid = %d in Cdb's table \"Cstage_disk\" (%s)\n"
                 ,stcp->reqid
                 ,sstrerror(serrno));
        } else {
          printf("--> reqid = %d inserted in \"Cstage_disk\" at offset 0x%lx\n",stcp->reqid,(unsigned long) Cdb_offset);
        }
        break;
      case 'm':
        if (Cdb_insert(&Cdb_db,"Cstage_hsm",NULL,&hsm,&Cdb_offset) != 0) {
          printf("### Cannot insert entry with reqid = %d in Cdb's table \"Cstage_hsm\" (%s)\n"
                 ,stcp->reqid
                 ,sstrerror(serrno));
        } else {
          printf("--> reqid = %d inserted in \"Cstage_hsm\" at offset 0x%lx\n",stcp->reqid,(unsigned long) Cdb_offset);
        }
        break;
      default:
        printf("### stcp->t_or_d == '%c' unrecognized. Please update this program.\n",stcp->t_or_d);
        rc = EXIT_FAILURE;
        goto stgconvert_return;
      }

    }

    /* We loop on the SHIFT's stgpath catalog */
    for (stpp = stps; stpp < stpe; stpp++) {
      if (stpp2Cdb(stpp,&link) != 0) {
        printf("### stpp2Cdb (eg. stgpath -> Cdb on-the-fly) conversion error for reqid = %d\n",stcp->reqid);
      }

      /* We delete the eventual previous entry with the same reqid in the master table */
      if (Cdb_keyfind(&Cdb_db,"Cstage_link","Cstage_link_per_reqid","w",&link,&Cdb_offset) != 0) {
        if (Cdb_delete(&Cdb_db,"Cstage_link",&Cdb_offset) != 0) {
          printf("### Cannot delete entry with reqid = %d in Cdb's table \"Cstage_link\" (%s)\n"
                 ,stcp->reqid
                 ,sstrerror(serrno));
        }
      }

      /* We insert this record */
      if (Cdb_insert(&Cdb_db,"Cstage_link",NULL,&link,&Cdb_offset) != 0) {
        printf("### Cannot insert entry with reqid = %d in Cdb's table \"Cstage_link\" (%s)\n"
                ,stcp->reqid
                ,sstrerror(serrno));
      } else {
        printf("--> reqid = %d inserted in \"Cstage_link\" at offset 0x%lx\n",stcp->reqid,(unsigned long) Cdb_offset);
      }
    }
    
  } else {
    
  }

 stgconvert_return:
  if (stcs != NULL) {
    free(stcs);
  }
  if (stps != NULL) {
    free(stps);
  }
  if (Cdb_db_opened != 0) {
    Cdb_close(&Cdb_db);
  }
  if (Cdb_session_opened != 0) {
    Cdb_logout(&Cdb_session);
  }
  close(stgcat_fd);
  close(stgpath_fd);
  return(rc);
}

int stgconvert_usage() {
  printf(
         "\n"
         "Usage : stgconvert -c <number> [options] stgcat_path stgpath_path\n"
         "\n"
         "  This program will convert from and to the SHIFT stager catalog\n"
         "and the CASTOR interfaced to Cdb database.\n"
         "\n"
         "Options are:\n"
         "  -h                Print this help and exit\n"
         "  -c <number>       Convert direction, where\n"
         "                    -c %2d means: SHIFT -> CASTOR\n"
         "                    -c %2d means: CASTOR -> SHIFT\n"
         "  -u                Cdb username. Defaults to \"%s\"\n"
         "  -p                Cdb password. Defaults to \"%s\"\n"
         "  -v                Print version and exit\n"
         "\n"
         "Note : It can very well be that Cdb is running with a zero length or non-existing\n"
         "       password file. In this case you can ignore -u and -p options.\n"
         "       If this is not the case and if you don't know the Cdb username and/or password\n"
         "       either contact somebody who knows, either delete the Cdb password file, which\n"
         "       should be located at /usr/spool/db/Cdb.pwd\n"
         "\n"
         " Example of SHIFT->CASTOR convertion:\n"
         "  stgconvert -c 1 /usr/spool/stage/stgcat /usr/spool/stage/stgpath\n"
         "\n"
         " Example of CASTOR->SHIFT convertion:\n"
         "  stgconvert -c -1 /usr/spool/stage/stgcat /usr/spool/stage/stgpath\n"
         "\n"
         "Comments to castor-support@listbox.cern.ch\n"
         "\n"
         ,SHIFT_TO_CASTOR,CASTOR_TO_SHIFT,CDB_USERNAME,CDB_PASSWORD
         );
}








