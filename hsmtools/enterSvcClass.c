/******************************************************************************
 *                      enterSvcClass.c
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: enterSvcClass.c,v $ $Revision: 1.20 $ $Release$ $Date: 2009/07/13 06:22:09 $ $Author: waldron $
 *
 * @author Olof Barring
 *****************************************************************************/

#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <errno.h>
#include <common.h>
#include <Castor_limits.h>
#include <osdep.h>
#include <serrno.h>
#include <Cgetopt.h>
#include <castor/stager/SvcClass.h>
#include <castor/stager/FileClass.h>
#include <castor/stager/TapeCopy.h>
#include <castor/stager/IStagerSvc.h>
#include <castor/Services.h>
#include <castor/BaseAddress.h>
#include <castor/stager/TapePool.h>
#include <castor/stager/DiskPool.h>
#include <castor/IAddress.h>
#include <castor/IObject.h>
#include <castor/IClient.h>
#include <castor/Constants.h>
#include <vmgr_api.h>

static char *itemDelimiter = ":";

enum SvcClassAttributes {
  Name = 1,
  NbDrives,
  DefaultFileSize,
  MaxReplicaNb,
  GcPolicy,
  MigratorPolicy,
  RecallerPolicy,
  StreamPolicy,
  Disk1Behavior,
  FailJobsWhenNoSpace,
  ForcedFileClass,
  ReplicateOnClose,
  TapePools,
  DiskPools
} svcClassAttributes;

static struct Coptions longopts[] = {
  {"help"               , NO_ARGUMENT      ,          0, 'h'                },
  {"Name"               , REQUIRED_ARGUMENT,          0, Name               },
  {"DefaultFileSize"    , REQUIRED_ARGUMENT,          0, DefaultFileSize    },
  {"MaxReplicaNb"       , REQUIRED_ARGUMENT,          0, MaxReplicaNb       },
  {"NbDrives"           , REQUIRED_ARGUMENT,          0, NbDrives           },
  {"GcPolicy"           , REQUIRED_ARGUMENT,          0, GcPolicy           },
  {"MigratorPolicy"     , REQUIRED_ARGUMENT,          0, MigratorPolicy     },
  {"RecallerPolicy"     , REQUIRED_ARGUMENT,          0, RecallerPolicy     },
  {"StreamPolicy"       , REQUIRED_ARGUMENT,          0, StreamPolicy       },
  {"Disk1Behavior"      , REQUIRED_ARGUMENT,          0, Disk1Behavior      },
  {"FailJobsWhenNoSpace", REQUIRED_ARGUMENT,          0, FailJobsWhenNoSpace},
  {"ForcedFileClass"    , REQUIRED_ARGUMENT,          0, ForcedFileClass    },
  {"ReplicateOnClose"   , REQUIRED_ARGUMENT,          0, ReplicateOnClose   },
  {"TapePools"          , REQUIRED_ARGUMENT,          0, TapePools          },
  {"DiskPools"          , REQUIRED_ARGUMENT,          0, DiskPools          },
  {NULL                 ,                 0,       NULL,                   0}
};

void usage(char *cmd)
{
  int i;
  fprintf(stdout,"Usage: %s \n",cmd);
  for (i=0; longopts[i].name != NULL; i++) {
    fprintf(stdout,"\t--%s %s\n",longopts[i].name,
            (longopts[i].has_arg == NO_ARGUMENT ? "" : longopts[i].name));
  }
  return;
}

int countItems(char *itemStr)
{
  char *p;
  int nbItems = 0;
  if ( itemStr == NULL ) return(0);

  p = itemStr;
  while ( p != NULL ) {
    nbItems++;
    p = strstr(p+1,itemDelimiter);
  }
  return(nbItems);
}

int splitItemStr(char *itemStr,
                 char ***itemArray,
                 int *nbItems)
{
  char *p, *tmpStr;
  int tmpNbItems, i;

  if ( itemStr == NULL || itemArray == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  tmpNbItems = countItems(itemStr);
  if ( tmpNbItems == 0 ) {
    if ( nbItems != NULL ) *nbItems = tmpNbItems;
    *itemArray = NULL;
    return(0);
  }
  tmpStr = strdup(itemStr);
  if ( itemArray == NULL ) {
    serrno = errno;
    return(-1);
  }
  *itemArray = (char **)calloc(tmpNbItems,sizeof(char *));
  if ( itemArray == NULL ) {
    serrno = errno;
    return(-1);
  }
  *nbItems = tmpNbItems;
  p = strtok(tmpStr,itemDelimiter);
  for (i=0; i<tmpNbItems; i++) {
    if ( p == NULL ) break;
    (*itemArray)[i] = strdup(p);
    p = strtok(NULL,itemDelimiter);
  }
  free(tmpStr);
  return(0);
}


/* NoArgOpt represents a parsed command-line option that has no argument */
typedef struct {
  int set; /* Boolean, non-zero == this option is set */
} NoArgOpt;


/* IntOpt represents a parsed command-line option with an integer argument */
typedef struct {
  int set; /* Boolean, non-zero == this option is set */
  int value;
} IntOpt;


/* Unsigned64Opt represents a parsed command-line option with an unsigned */
/* 64-bit integer argument                                                */
typedef struct {
  int set; /* Boolean, non-zero == this option is set */
  u_signed64 value;
} Unsigned64Opt;


/* StrOpt represents a parsed command-line option with a string argument */
typedef struct {
  int set; /* Boolean, non-zero == this option is set */
  char *value;
} StrOpt;

/* StrsOpt represents a parsed command-line option with one or more string */
/* arguments.                                                              */
typedef struct {
  int set; /* Boolean, non-zero == this option is set */
  char **values;
  int nbValues;
} StrsOpt;


/* ParsedCmdLine represents the parsed command-line arguments */
typedef struct {
  NoArgOpt      help;
  StrOpt        name;
  Unsigned64Opt defaultFileSize;
  IntOpt        maxReplicaNb;
  IntOpt        nbDrives;
  StrOpt        gcPolicy;
  StrOpt        migratorPolicy;
  StrOpt        recallerPolicy;
  StrsOpt       tapePools;
  StrsOpt       diskPools;
  IntOpt        disk1Behavior;
  IntOpt        failJobsWhenNoSpace;
  StrOpt        forcedFileClass;
  StrOpt        streamPolicy;
  IntOpt        replicateOnClose;
} ParsedCmdLine;


/**
 * Parses the specified command-line argument into the specified data structure.
 *
 * This function prints any parse errors it encounters to stderr.
 *
 * @param argc          The total number of command-line arguments including
 *                      the name of the command (argv[0]).
 * @param argv          The command-line arguments.
 * @param parsedCmdLine Out parameter: The data structure into which the parsed
 *                      command-line arguments should be stored.
 * @return              0 on success, else non-zero.
 */
static int parseCmdLine(int argc, char *argv[], ParsedCmdLine *parsedCmdLine) {
  int c;

  /* Clear the parsed command-line arguments data structure */
  memset(parsedCmdLine, '\0', sizeof(*parsedCmdLine));

  /* Parse the command-line arguments */
  Coptind = 1;
  Copterr = 1;
  while ((c = Cgetopt_long(argc,argv,"h",longopts,NULL)) != EOF) {
    switch (c) {
    case 'h':
      parsedCmdLine->help.set = 1;
      break;
    case Name:
      parsedCmdLine->name.value = strdup(Coptarg);
      parsedCmdLine->name.set   = 1;
      break;
    case DefaultFileSize:
      parsedCmdLine->defaultFileSize.value = strtou64(Coptarg);
      parsedCmdLine->defaultFileSize.set   = 1;
      break;
    case MaxReplicaNb:
      parsedCmdLine->maxReplicaNb.value = atoi(Coptarg);
      if(parsedCmdLine->maxReplicaNb.value < 0) {
        fprintf(stderr,
                "Error parsing maxReplicaNb"
                ": Value must be 0 or positive"
                ": value= %d\n", parsedCmdLine->maxReplicaNb.value);
        return(1); /* Failure */
      }
      parsedCmdLine->maxReplicaNb.set   = 1;
      break;
    case NbDrives:
      parsedCmdLine->nbDrives.value = atoi(Coptarg);
      if(parsedCmdLine->nbDrives.value < 0) {
        fprintf(stderr,
                "Error parsing nbDrives"
                ": Value must be 0 or positive"
                ": value= %d\n", parsedCmdLine->nbDrives.value);
        return(1); /* Failure */
      }
      parsedCmdLine->nbDrives.set   = 1;
      break;
    case GcPolicy:
      parsedCmdLine->gcPolicy.value = strdup(Coptarg);
      parsedCmdLine->gcPolicy.set   = 1;
      break;
    case MigratorPolicy:
      parsedCmdLine->migratorPolicy.value = strdup(Coptarg);
      parsedCmdLine->migratorPolicy.set   = 1;
      break;
    case RecallerPolicy:
      parsedCmdLine->recallerPolicy.value = strdup(Coptarg);
      parsedCmdLine->recallerPolicy.set   = 1;
      break;
    case TapePools:
      if(splitItemStr(Coptarg, &parsedCmdLine->tapePools.values,
                      &parsedCmdLine->tapePools.nbValues) == -1) {
        fprintf(stderr, "Error parsing tape pools string: %s\n",
                sstrerror(serrno));
        return(1); /* Failure */
      }
      parsedCmdLine->tapePools.set   = 1;
      break;
    case DiskPools:
      if(splitItemStr(Coptarg, &parsedCmdLine->diskPools.values,
                      &parsedCmdLine->diskPools.nbValues) == -1) {
        fprintf(stderr, "Error parsing disk pools string: %s\n",
                sstrerror(serrno));
        return(1); /* Failure */
      }
      parsedCmdLine->diskPools.set   = 1;
      break;
    case Disk1Behavior:
      if (!strcasecmp(Coptarg, "yes")) {
        parsedCmdLine->disk1Behavior.value = 1;
        parsedCmdLine->disk1Behavior.set   = 1;
      } else if (!strcasecmp(Coptarg, "no")) {
        parsedCmdLine->disk1Behavior.value = 0;
        parsedCmdLine->disk1Behavior.set   = 1;
      } else {
        fprintf(stderr,
                "Invalid option for Disk1Behavior, value must be 'yes' or 'no'\n");
        return(1); /* Failure */
      }
      break;
    case FailJobsWhenNoSpace:
      if (!strcasecmp(Coptarg, "yes")) {
        parsedCmdLine->failJobsWhenNoSpace.value = 1;
        parsedCmdLine->failJobsWhenNoSpace.set   = 1;
      } else if (!strcasecmp(Coptarg, "no")) {
        parsedCmdLine->failJobsWhenNoSpace.value = 0;
        parsedCmdLine->failJobsWhenNoSpace.set   = 0;
      } else {
        fprintf(stderr,
                "Invalid option for FailJobsWhenNoSpace,"
                " value must be 'yes' or 'no'\n");
        return(1); /* Failure */
      }
      break;
    case ForcedFileClass:
      parsedCmdLine->forcedFileClass.value = strdup(Coptarg);
      parsedCmdLine->forcedFileClass.set   = 1;
      break;
    case StreamPolicy:
      parsedCmdLine->streamPolicy.value = strdup(Coptarg);
      parsedCmdLine->streamPolicy.set   = 1;
      break;
    case ReplicateOnClose:
      if (!strcasecmp(Coptarg, "yes")) {
        parsedCmdLine->replicateOnClose.value = 1;
        parsedCmdLine->replicateOnClose.set   = 1;
      } else if (!strcasecmp(Coptarg, "no")) {
        parsedCmdLine->replicateOnClose.value = 0;
        parsedCmdLine->replicateOnClose.set   = 0;
      } else {
        fprintf(stderr,
                "Invalid option for ReplicateOnClose, value must be 'yes' or 'no'\n");
        return(1); /* Failure */
      }
      break;
    case '?':
      fprintf(stderr, "Unknown command-line option: %c\n", Coptopt);
      return(1); /* Failure */
      break;
    default:
      fprintf(stderr, "Error calling getopt(): Return value is 0x%x\n", c);
      return(1); /* Failure */
    }
  }

  return(0); /* Successfully parsed the command-line arguments */
}


/**
 * Prints an error message to stderr and returns a non-zero value if one or
 * more of the specified tape pools are not defined in the VMGR.
 *
 * @param  pools   The tape pools.
 * @param  nbPools The number of tape pools.
 * @return         0 if all of the tape pools exist else non-zero.
 */
static int checkTapePoolsInVmgr(char **pools, int nbPools) {
  int  rc = 0; /* Start with a function return code of success */
  int  i  = 0;

  /* For each tape pool */
  for(i=0; i<nbPools; i++ ) {

    /* Check the tape pool is defined in the VMGR */
    uid_t      uid            = 0;
    gid_t      gid            = 0;
    u_signed64 capacity       = 0;
    u_signed64 tot_free_space = 0;;

    /* If there is an error querying the VMGR about the tape pool */
    if(vmgr_querypool(pools[i], &uid, &gid, &capacity, &tot_free_space) < 0) {

      /* Display the error and remember that it occured */
      fprintf(stderr, "Error checking tape pool %s: ", pools[i]);
      if(serrno == ENOENT) {
        fprintf(stderr, "Tape pool is not defined in the VMGR\n");
      } else {
        fprintf(stderr, "Error querying VMGR: %s\n", sstrerror(serrno));
      }
      rc = 1; /* Failure */
    }
  }

  return rc;
}


int main(int argc, char *argv[]) {
  char                        *cmd             = argv[0];
  const int                   defaultReplicaNb = 1;
  struct C_BaseAddress_t      *baseAddr        = NULL;
  struct C_IAddress_t         *iAddr           = NULL;
  struct C_IObject_t          *iObj            = NULL;
  struct Cstager_IStagerSvc_t *fsSvc           = NULL;
  struct C_Services_t         *svcs            = NULL;
  struct C_IService_t         *iSvc            = NULL;
  struct Cstager_FileClass_t  *fileClass       = NULL;
  struct Cstager_SvcClass_t   *svcClass        = NULL;
  struct Cstager_SvcClass_t   *svcClassOld     = NULL;
  ParsedCmdLine               parsedCmdLine;

  /* Parse the command-line                                            */
  /* Display an error message and return if there are any parse errors */
  if(parseCmdLine(argc, argv, &parsedCmdLine)) {
    usage(cmd);
    return(1); /* Failure */
  }

  /* Display usage message and return if the help option has been specifed */
  if(parsedCmdLine.help.set) {
    usage(cmd);
    return(0); /* Success */
  }

  /* Display an error message if the name option has not be specified */
  if(!parsedCmdLine.name.set) {
    fprintf(stderr, "SvcClass 'name' is required\n");
    usage(cmd);
    return(1); /* Failure */
  }

  /* Display an error mesaage and return if one or more tape pools have been */
  /* specified that are not defined in the VMGR                              */
  if(parsedCmdLine.tapePools.set &&
     checkTapePoolsInVmgr(parsedCmdLine.tapePools.values,
                          parsedCmdLine.tapePools.nbValues)) {
    usage(cmd);
    return(1); /* Failure */
  }

  /* Connect to the stager database */
  if(C_Services_create(&svcs) == -1) {
    fprintf(stderr,"Cannot create catalogue DB services: %s\n",
            sstrerror(serrno));
    return(1); /* Failure */
  }
  if(C_Services_service(svcs,"DbStagerSvc",SVC_DBSTAGERSVC,&iSvc) == -1) {
    fprintf(stderr,"Cannot create fs svc: %s, %s\n",
            sstrerror(serrno),
            C_Services_errorMsg(svcs));
    return(1); /* Failure */
  }
  fsSvc = Cstager_IStagerSvc_fromIService(iSvc);

  /* Create a memory resident version of the service class */
  Cstager_SvcClass_create(&svcClass);

  Cstager_SvcClass_setMaxReplicaNb(svcClass, defaultReplicaNb);

  /* Set the attributes of the service class */
  Cstager_SvcClass_setFailJobsWhenNoSpace(svcClass, 1);  /* by default */
  Cstager_SvcClass_setName(svcClass, parsedCmdLine.name.value);
  if(parsedCmdLine.defaultFileSize.set) {
    Cstager_SvcClass_setDefaultFileSize(svcClass,
                                        parsedCmdLine.defaultFileSize.value);
  } else {
    Cstager_SvcClass_setDefaultFileSize(svcClass,
                                        2147483648u);
  }
  if(parsedCmdLine.maxReplicaNb.set) {
    Cstager_SvcClass_setMaxReplicaNb(svcClass,
                                     parsedCmdLine.maxReplicaNb.value);
  }
  if(parsedCmdLine.nbDrives.set) {
    Cstager_SvcClass_setNbDrives(svcClass, parsedCmdLine.nbDrives.value);
  }
  if(parsedCmdLine.gcPolicy.set) {
    Cstager_SvcClass_setGcPolicy(svcClass, parsedCmdLine.gcPolicy.value);
  }
  if(parsedCmdLine.migratorPolicy.set) {
    Cstager_SvcClass_setMigratorPolicy(svcClass,
                                       parsedCmdLine.migratorPolicy.value);
  }
  if(parsedCmdLine.recallerPolicy.set) {
    Cstager_SvcClass_setRecallerPolicy(svcClass,
                                       parsedCmdLine.recallerPolicy.value);
  }
  if(parsedCmdLine.disk1Behavior.set) {
    if(parsedCmdLine.disk1Behavior.value) {
      Cstager_SvcClass_setDisk1Behavior(svcClass, 1);
      Cstager_SvcClass_setFailJobsWhenNoSpace(svcClass, 1);
    } else {
      Cstager_SvcClass_setDisk1Behavior(svcClass, 0);
    }
  }
  if(parsedCmdLine.failJobsWhenNoSpace.set) {
    if(parsedCmdLine.failJobsWhenNoSpace.value) {
      Cstager_SvcClass_setFailJobsWhenNoSpace(svcClass, 1);
    } else {
      Cstager_SvcClass_setFailJobsWhenNoSpace(svcClass, 0);
    }
  }
  if(parsedCmdLine.forcedFileClass.set) {
    if(Cstager_IStagerSvc_selectFileClass(fsSvc, &fileClass,
                                          parsedCmdLine.forcedFileClass.value) == -1) {
      fprintf(stderr,"Cstager_IStagerSvc_selectFileClass(%s): %s, %s\n",
              parsedCmdLine.name.value, sstrerror(serrno),
              Cstager_IStagerSvc_errorMsg(fsSvc));
      return(1); /* Failure */
    }

    if(fileClass == NULL) {
      fprintf(stderr, "FileClass %s does not exist\n",
              parsedCmdLine.forcedFileClass.value);
      return(1); /* Failure */
    }

    Cstager_SvcClass_setForcedFileClass(svcClass, fileClass);
  }
  if(parsedCmdLine.streamPolicy.set) {
    Cstager_SvcClass_setStreamPolicy(svcClass,
                                     parsedCmdLine.streamPolicy.value);
  }
  if(parsedCmdLine.replicateOnClose.set) {
    if(parsedCmdLine.replicateOnClose.value) {
      Cstager_SvcClass_setReplicateOnClose(svcClass, 1);
    } else {
      Cstager_SvcClass_setReplicateOnClose(svcClass, 0);
    }
  }

  /* Display an error message and return if the service class already exists */
  int rc = Cstager_IStagerSvc_selectSvcClass(fsSvc, &svcClassOld,
                                             parsedCmdLine.name.value);
  if ( (rc == 0) && (svcClassOld != NULL) ) {
    fprintf(stderr,
            "SvcClass %s already exists, please use 'modifySvcClass' command\n"
            "to change any attribute of an existing SvcClass\n",
            parsedCmdLine.name.value);
    Cstager_SvcClass_print(svcClassOld);
    return(1);
  }

  /* Display the direct attributes of the service class */
  fprintf(stdout,"Adding SvcClass: %s\n", parsedCmdLine.name.value);

  if(C_BaseAddress_create(&baseAddr) == -1) {
    fprintf(stderr,"C_BaseAddress_create(): Unknown error\n");
    return(1);  /* Failure */
  }

  C_BaseAddress_setCnvSvcName(baseAddr,"DbCnvSvc");
  C_BaseAddress_setCnvSvcType(baseAddr,SVC_DBCNV);
  iAddr = C_BaseAddress_getIAddress(baseAddr);
  iObj = Cstager_SvcClass_getIObject(svcClass);
  /* Create representation with auto commit on */
  if(C_Services_createRep(svcs, iAddr, iObj, 1) == -1) {
    fprintf(stderr,"C_Services_createRep(): %s, %s\n",
            sstrerror(serrno),
            C_Services_errorMsg(svcs));
    return(1); /* Failure */
  }

  /* Add the tape pools, creating them in the stager database if necessary */
  if(parsedCmdLine.tapePools.set) {

    int                       i         = 0;
    char                      *poolName = NULL;
    struct Cstager_TapePool_t *pool     = NULL;

    /* For each tape pool name on the command-line */
    for(i=0; i<parsedCmdLine.tapePools.nbValues; i++) {

      poolName  = parsedCmdLine.tapePools.values[i];

      fprintf(stdout, "Adding tape pool %s to SvcClass %s\n", poolName,
              parsedCmdLine.name.value);

      pool = NULL;
      if(Cstager_IStagerSvc_selectTapePool(fsSvc, &pool, poolName) == -1) {
        fprintf(stderr,"Cstager_IStagerSvc_selectTapePool(%s): %s, %s\n",
                poolName, sstrerror(serrno), Cstager_IStagerSvc_errorMsg(fsSvc));
        return(1);  /* Failure */
      }
      if(pool == NULL ) {
        Cstager_TapePool_create(&pool);
        if(pool == NULL ) {
          fprintf(stderr,"Cstager_TapePool_create(): %s\n", sstrerror(serrno));
          return(1); /* Failure */
        }
        Cstager_TapePool_setName(pool, poolName);
      }
      Cstager_TapePool_addSvcClasses(pool, svcClass);
      Cstager_SvcClass_addTapePools(svcClass, pool);
    }
    if(C_Services_fillRep(svcs, iAddr, iObj, OBJ_TapePool, 0) == -1) {
      fprintf(stderr,"C_Services_fillRep(svcClass,OBJ_TapePool): %s, %s\n",
              sstrerror(serrno), C_Services_errorMsg(svcs));
      return(1); /* Failure */
    }
  }

  /* Add the disk pools, creating them in the stager database if necessary */
  if(parsedCmdLine.diskPools.set) {

    int                       i         = 0;
    char                      *poolName = NULL;
    struct Cstager_DiskPool_t *pool     = NULL;

    /* For each disk pool name on the command-line */
    for(i=0; i<parsedCmdLine.diskPools.nbValues; i++ ) {

      poolName = parsedCmdLine.diskPools.values[i];

      fprintf(stdout,"Adding disk pool %s to SvcClass %s\n", poolName,
              parsedCmdLine.name.value);

      pool = NULL;
      if(Cstager_IStagerSvc_selectDiskPool(fsSvc, &pool, poolName) == -1) {
        fprintf(stderr,"Cstager_IStagerSvc_selectDiskPool(%s): %s, %s\n",
                poolName, sstrerror(serrno), Cstager_IStagerSvc_errorMsg(fsSvc));
        return(1); /* Failure */
      }
      if(pool == NULL ) {
        Cstager_DiskPool_create(&pool);
        if(pool == NULL ) {
          fprintf(stderr,"Cstager_DiskPool_create(): %s\n", sstrerror(serrno));
          return(1); /* Failure */
        }
        Cstager_DiskPool_setName(pool, poolName);
      }
      Cstager_DiskPool_addSvcClasses(pool, svcClass);
      Cstager_SvcClass_addDiskPools(svcClass, pool);
    }
    if(C_Services_fillRep(svcs, iAddr, iObj, OBJ_DiskPool, 0) == -1) {
      fprintf(stderr,"C_Services_fillRep(svcClass,OBJ_DiskPool): %s, %s\n",
              sstrerror(serrno), C_Services_errorMsg(svcs));
      return(1); /* Failure */
    }
  }

  if(fileClass != NULL  &&
     C_Services_fillRep(svcs, iAddr, iObj, OBJ_FileClass, 0) == -1) {
    fprintf(stderr,"C_Services_fillRep(svcClass,OBJ_FileClass): %s, %s\n",
            sstrerror(serrno), C_Services_errorMsg(svcs));
    return(1); /* Failure */
  }

  if(C_Services_commit(svcs,iAddr) == -1) {
    fprintf(stderr,"C_Services_fillRep(svcClass,OBJ_DiskPool): %s, %s\n",
            sstrerror(serrno), C_Services_errorMsg(svcs));
    return(1); /* Failure */
  }

  /* Print the svcclass sttributes */
  rc = Cstager_IStagerSvc_selectSvcClass(fsSvc, &svcClassOld,
                                         parsedCmdLine.name.value);
  if ( (rc == 0) && (svcClassOld != NULL) ) {
    Cstager_SvcClass_print(svcClassOld);
  }

  return(0); /* Success */
}
