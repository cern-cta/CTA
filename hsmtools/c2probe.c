/******************************************************************************
 *                      c2probe.c
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
 * @(#)$RCSfile: c2probe.c,v $ $Revision: 1.4 $ $Release$ $Date: 2007/05/07 06:58:05 $ $Author: waldron $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <errno.h>
#include <serrno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <rfio.h>
#include <serrno.h>
#include <log.h>
#include <stage_api.h>
#include <Cpwd.h>
#include <Cinit.h>
#include <Cuuid.h>
#include <Cgetopt.h>
#include <stager_api.h>
#include <Cthread_api.h>
#include <stager_client_commandline.h>
#include <Cpool_api.h>
#include <u64subr.h>
#define ERRBUFSIZE 255
/*
 * Various circular list operations.
 */
#define CLIST_ITERATE_BEGIN(X,Y) {if ( (X) != NULL ) {(Y)=(X); do {
#define CLIST_ITERATE_END(X,Y) Y=(Y)->next; } while ((X) != (Y));}}
#define CLIST_INSERT(X,Y) {if ((X) == NULL) {X=(Y); (X)->next = (X)->prev = (X);} \
else {(Y)->next=(X); (Y)->prev=(X)->prev; (Y)->next->prev=(Y); (Y)->prev->next=(Y);}}
#define CLIST_DELETE(X,Y) {if ((Y) != NULL) {if ( (Y) == (X) ) (X)=(X)->next; \
 if ((Y)->prev != (Y) && (Y)->next != (Y)) {\
 (Y)->prev->next = (Y)->next; (Y)->next->prev = (Y)->prev;} else {(X)=NULL;}}}

#define UPDATE_TIMING(SVCC,WHAT) { \
    TimingItem_t *myItem; \
    myItem = getTimingItem(SVCC); \
    myItem->lastUpdate = myItem->WHAT = time(NULL); \
    (void)unlockTimingList();}
#define UPDATE_ERROR(SVCC,WHAT) { \
    TimingItem_t *myItem; \
    myItem = getTimingItem(SVCC); \
    addErrorItem(&(myItem->WHAT)); \
    (void)unlockTimingList();}
#define _DELTA_TIME(WHAT,A,B) (WHAT ## A - WHAT ## B >= 0 ? WHAT ## A - WHAT ## B : time(NULL)-WHAT ## B)
#define DELTA_TIME(WHAT) _DELTA_TIME(WHAT,_end,_start)

extern int Cinitdaemon _PROTO((char *, void (*)(int)));
extern int Cinitservice _PROTO((char *, void (*)(int)));

uid_t runUid;
gid_t runGid;
char *directoryName = NULL;
char *statFileName = "/tmp/c2probe.out";
char *logFileName = "/tmp/c2probe.log";
char **svcClasses = NULL;
char *stageHost = NULL;
int nbSvcClasses = 0, bufferSize = 1024*1024;
u_signed64 nbBytesToWrite = 1;
int nbLoops = -1;
int sleepTime = 900; /* Probe every 15min by default */
int keepErrorTime = 7200; /* Error counter sliding window == 2 hours by default */
int help_flag = 0;
int appendStat_flag = 0;
static char *itemDelimiter = ":";

typedef struct ErrorItem
{
  time_t timestamp;
  struct ErrorItem *next;
  struct ErrorItem *prev;
} ErrorItem_t;

typedef struct TimingItem 
{
  char *svcClass;
  time_t lastUpdate;
  time_t rfio_open_write_start;
  time_t rfio_open_write_end;
  time_t rfio_open_read_start;
  time_t rfio_open_read_end;
  time_t rfio_write_start;
  time_t rfio_write_end;
  time_t rfio_read_start;
  time_t rfio_read_end;
  time_t rfio_close_write_start;
  time_t rfio_close_write_end;
  time_t rfio_close_read_start;
  time_t rfio_close_read_end;
  time_t stager_qry_start;
  time_t stager_qry_end;
  time_t stager_rm_start;
  time_t stager_rm_end;
  ErrorItem_t *rfio_open_write_error;
  ErrorItem_t *rfio_open_read_error;
  ErrorItem_t *rfio_write_error;
  ErrorItem_t *rfio_read_error;
  ErrorItem_t *rfio_close_write_error;
  ErrorItem_t *rfio_close_read_error;
  ErrorItem_t *stager_qry_error;
  ErrorItem_t *stager_rm_error;
  ErrorItem_t *corruption_error;
  struct TimingItem *next;
  struct TimingItem *prev;
} TimingItem_t;

TimingItem_t *timingList = NULL;

enum RunOptions
{
  Noop,
  DirectoryName,
  StatFile,
  AppendStat,
  LogFile,
  StageHost,
  SvcClasses,
  BufferSize,
  NbBytesToWrite,
  NbLoops,
  SleepTime,
  KeepErrorTime,
  RunAsUser
} runOptions;

static struct Coptions longopts[] = 
{
  {"help",NO_ARGUMENT,&help_flag,'h'},
  {"DirectoryName",REQUIRED_ARGUMENT,NULL,DirectoryName},
  {"StatFile",REQUIRED_ARGUMENT,NULL,StatFile},
  {"AppendStat",NO_ARGUMENT,&appendStat_flag,1},
  {"LogFile",REQUIRED_ARGUMENT,NULL,LogFile},
  {"StageHost",REQUIRED_ARGUMENT,NULL,StageHost},
  {"SvcClasses",REQUIRED_ARGUMENT,NULL,SvcClasses},
  {"NbLoops",REQUIRED_ARGUMENT,NULL,NbLoops},
  {"SleepTime",REQUIRED_ARGUMENT,NULL,SleepTime},
  {"KeepErrorTime",REQUIRED_ARGUMENT,NULL,KeepErrorTime},
  {"BufferSize",REQUIRED_ARGUMENT,NULL,BufferSize},
  {"NbBytesToWrite",REQUIRED_ARGUMENT,NULL,NbBytesToWrite},
  {"RunAsUser",REQUIRED_ARGUMENT,NULL,RunAsUser},
  {NULL, 0, NULL, 0}
};

void usage(
           cmd
           )
     char *cmd;
{
  int i;
  fprintf(stdout,"Usage: %s \n",cmd);
  for (i=0; longopts[i].name != NULL; i++) {
    fprintf(stdout,"\t--%s %s\n",longopts[i].name,
            (longopts[i].has_arg == NO_ARGUMENT ? "" : longopts[i].name));
  }
  return;
}

int lockTimingList() 
{
  return(Cthread_mutex_lock(&timingList));
}

int unlockTimingList()
{
  return(Cthread_mutex_unlock(&timingList));
}

TimingItem_t *getTimingItem(
                            char *svcClass
                            ) 
{
  TimingItem_t *item;
  
  if ( svcClass == NULL ) return(NULL);
  if ( lockTimingList() == -1 ) return(NULL);
  
  CLIST_ITERATE_BEGIN(timingList,item) 
    {
      if ( strcmp(svcClass,item->svcClass) == 0 ) return(item);
    }
  CLIST_ITERATE_END(timingList,item);
  unlockTimingList();
  return(NULL);
}

int addErrorItem(
                 ErrorItem_t **list
                 ) 
{
  ErrorItem_t *item;
  item = (ErrorItem_t *)calloc(1,sizeof(ErrorItem_t));
  if ( item == NULL ) return(-1);
  item->timestamp = time(NULL);
  CLIST_INSERT(*list,item);
  return(0);
}

int countErrors(
                ErrorItem_t **list
                ) 
{
  ErrorItem_t *item;
  time_t now;
  int errorCount, errorDeleted;
  
  now = time(NULL);
  for (;;) {
    errorCount = errorDeleted = 0;
    CLIST_ITERATE_BEGIN(*list,item) 
      {
        if ( now-(item->timestamp) > keepErrorTime ) {
          CLIST_DELETE(*list,item);
          free(item);
          errorDeleted++;
          break;
        }
        errorCount++;
      }
    CLIST_ITERATE_END(*list,item);
    if (errorDeleted == 0) break;
  }
  return(errorCount);
}

int countItems(
               itemStr
               )
     char *itemStr;
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

int splitItemStr(
                 itemStr,
                 itemArray,
                 nbItems
                 )
     char *itemStr;
     char ***itemArray;
     int *nbItems;
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

int qryFile(char *svcClass, char *path) 
{
	struct stage_query_req *qrequests;
	struct  stage_filequery_resp *qresponses;
	int nbqresps = 0, rc;
  char errbuf[ERRBUFSIZE+1];
  struct stage_options opts;

  opts.stage_host = stageHost;
  opts.service_class = svcClass;
  opts.stage_version=2;
  opts.stage_port=0;
  rc=getDefaultForGlobal(
                         &opts.stage_host,
                         &opts.stage_port,
                         &opts.service_class,
                         &opts.stage_version
                         );
  memset(&errbuf,  '\0', sizeof(errbuf));

  /* Setting the error buffer */
  stage_seterrbuf(errbuf, sizeof(errbuf));

  create_query_req(&qrequests,1);
  qrequests[0].type = BY_FILENAME;
  qrequests[0].param = strdup(path);

  UPDATE_TIMING(svcClass,stager_qry_start);
  rc = stage_filequery(qrequests,1,&qresponses,&nbqresps,&opts);
  UPDATE_TIMING(svcClass,stager_qry_end);
  if ( rc != 0 ) {
    log(LOG_ERR,"stage_filequery(%s), svcClass=%s: %s\n",
        path,svcClass,sstrerror(serrno));
    UPDATE_ERROR(svcClass,stager_qry_error);
  }
  free_filequery_resp(qresponses,nbqresps);
  free_query_req(qrequests,1);
  return(0);
}

int rmFile(char *svcClass, char *path) 
{
  struct stage_filereq *reqs;
  struct stage_fileresp *response;
  int nbresps, nbreqs;
  char *reqid;
  char errbuf[ERRBUFSIZE+1];
  int rc;
  struct stage_options opts;

  opts.stage_host = stageHost;
  opts.service_class = svcClass;
  opts.stage_version=2;
  opts.stage_port=0;
  rc=getDefaultForGlobal(
                         &opts.stage_host,
                         &opts.stage_port,
                         &opts.service_class,
                         &opts.stage_version
                         );
  memset(&errbuf,  '\0', sizeof(errbuf));

  /* Setting the error buffer */
  stage_seterrbuf(errbuf, sizeof(errbuf));

  nbreqs = 1;
  create_filereq(&reqs,nbreqs);
  reqs[0].filename = strdup(path);

  UPDATE_TIMING(svcClass,stager_rm_start);
  rc = stage_rm(reqs,
                nbreqs,
                &response,
                &nbresps,
                &reqid,
                &opts);

  UPDATE_TIMING(svcClass,stager_rm_end);
  if (rc < 0) {
    log(LOG_ERR, "stage_rm(%s,%s): %s, errbuf=%s\n",
        svcClass,path,sstrerror(serrno),errbuf);
    UPDATE_ERROR(svcClass,stager_rm_error);
    return(-1);
  }

  if (response == NULL) {
    log(LOG_ERR, "stage_rm(%s,%s): Response object is NULL\n",
        svcClass,path);
    return(-1);
  }
  free_filereq(reqs,nbreqs);
  free_fileresp(response,nbresps);

  return(0);
}

void dumpStat() 
{
  int i, fd, rc, oflags;
  TimingItem_t *myItem;
  time_t now;
  char printBuffer[1024];

  if ( appendStat_flag == 0 )   oflags = O_CREAT|O_TRUNC|O_WRONLY;
  else   oflags = O_CREAT|O_APPEND|O_WRONLY;

  fd = open(statFileName,oflags,0766);
  if ( fd == -1 ) {
    log(LOG_ERR,"dumpStat() open(%s): %s\n",statFileName,sstrerror(errno));
    return;
  }
  now = time(NULL);
  for (i=0; i<nbSvcClasses; i++) {
    myItem = getTimingItem(svcClasses[i]);
    sprintf(printBuffer,
            "%s timing "
            "now %lu "
            "updated %d seconds ago "
            "rfio_open_w %d "
            "rfio_write %d "
            "rfio_close_w %d "
            "rfio_open_r %d "
            "rfio_read %d "
            "rfio_close_r %d "
            "stager_qry %d "
            "stager_rm %d\n",
            myItem->svcClass,
            (unsigned long)now,
            (int)(now - myItem->lastUpdate),
            (int)DELTA_TIME(myItem->rfio_open_write),
            (int)DELTA_TIME(myItem->rfio_write),
            (int)DELTA_TIME(myItem->rfio_close_write),
            (int)DELTA_TIME(myItem->rfio_open_read),
            (int)DELTA_TIME(myItem->rfio_read),
            (int)DELTA_TIME(myItem->rfio_close_read),
            (int)DELTA_TIME(myItem->stager_qry),
            (int)DELTA_TIME(myItem->stager_rm));

    rc = write(fd,printBuffer,strlen(printBuffer));
    sprintf(printBuffer,
            "%s errors "
            "since %lu "
            "now %lu "
            "rfio_open_w %d "
            "rfio_write %d "
            "rfio_close_w %d "
            "rfio_open_r %d "
            "rfio_read %d "
            "rfio_close_r %d "
            "stager_qry %d "
            "stager_rm %d "
            "corruptions %d\n",
            myItem->svcClass,
            (unsigned long)(time(NULL)-keepErrorTime),
            (unsigned long)time(NULL),
            countErrors(&(myItem->rfio_open_write_error)),
            countErrors(&(myItem->rfio_write_error)),
            countErrors(&(myItem->rfio_close_write_error)),
            countErrors(&(myItem->rfio_open_read_error)),
            countErrors(&(myItem->rfio_read_error)),
            countErrors(&(myItem->rfio_close_read_error)),
            countErrors(&(myItem->stager_qry_error)),
            countErrors(&(myItem->stager_rm_error)),
            countErrors(&(myItem->corruption_error)));
    rc = write(fd,printBuffer,strlen(printBuffer));
    unlockTimingList();
  }
  close(fd);
  return;
}

void *svcClassProbe(
                    void *arg
                    ) 
{
  char *mySvcClass = NULL;
  char *myRootPath = NULL;
  char *baseName = NULL;
  char *fullPath = NULL;
  char *path = NULL;
  Cuuid_t myUuid;
  int rc, i = 0, fd = -1, nbBytesWritten = -1, nbBytesRead = -1, size=0;
  int mySleepTime;
  char *myWriteBuffer, *myReadBuffer;

  if ( arg == NULL ) {
    log(LOG_ERR,"svcClassProbe() thread started without service class???\n");
    return(NULL);
  }
  mySvcClass = strdup((char *)arg);
  myRootPath  = (char *)malloc(strlen("rfio://") +
                               strlen(stageHost) + strlen("/") +
                               strlen("?svcClass=") + strlen(mySvcClass) +
                               strlen("&castorVersion=2&path=") +
                               strlen(directoryName)+strlen("/") +
                               strlen("c2probe/") +
                               strlen(stageHost)+strlen("/") +
                               strlen(mySvcClass)+1);
  if ( myRootPath == NULL ) {
    log(LOG_ERR,"malloc(): %s\n",sstrerror(errno));
    return(NULL);
  }
  sprintf(myRootPath,"rfio://%s/?svcClass=%s&castorVersion=2&path=%s/c2probe",
          stageHost,mySvcClass,directoryName);
  rc = rfio_mkdir(myRootPath,0755);
  if ( rc == -1 ) {
    log(LOG_ERR,"rfio_mkdir(%s): %s\n",myRootPath,rfio_serror());
    if ( rfio_serrno() != EEXIST ) return(NULL);
  }
  sprintf(myRootPath,"rfio://%s/?svcClass=%s&castorVersion=2&path=%s/c2probe/%s",
          stageHost,mySvcClass,directoryName,stageHost);
  rc = rfio_mkdir(myRootPath,0755);
  if ( rc == -1 ) {
    log(LOG_ERR,"rfio_mkdir(%s): %s\n",myRootPath,rfio_serror());
    if ( rfio_serrno() != EEXIST ) return(NULL);
  }
  sprintf(myRootPath,"rfio://%s/?svcClass=%s&castorVersion=2&path=%s/c2probe/%s/%s",
          stageHost,mySvcClass,directoryName,stageHost,mySvcClass);
  rc = rfio_mkdir(myRootPath,0755);
  if ( rc == -1 ) {
    log(LOG_ERR,"rfio_mkdir(%s): %s\n",myRootPath,rfio_serror());
    if ( rfio_serrno() != EEXIST ) return(NULL);
  }
  log(LOG_INFO,"svcClassProbe(%s) starting with root path %s\n",
      mySvcClass,myRootPath);
  if ( (baseName = (char *)malloc(256)) == NULL ) {
    log(LOG_ERR,"svcClassProbe(%s) malloc(): %s\n",mySvcClass,sstrerror(errno));
    return(NULL);
  }
  if ( (myWriteBuffer = (char *)malloc(bufferSize)) == NULL ) {
    log(LOG_ERR,"svcClassProbe(%s) malloc(%d): %s\n",mySvcClass,
        bufferSize,sstrerror(errno));
    return(NULL);
  }
  if ( (myReadBuffer = (char *)malloc(bufferSize)) == NULL ) {
    log(LOG_ERR,"svcClassProbe(%s) malloc(%d): %s\n",mySvcClass,
        bufferSize,sstrerror(errno));
    return(NULL);
  }

  for (i=0; i<bufferSize; i++) {
    myWriteBuffer[i] = (char)(random() % 128);
  }

  i=0;
  fullPath = NULL;
  mySleepTime = sleepTime * Cthread_self() / nbSvcClasses;
  while ( (i<nbLoops) || (nbLoops<0) ) {
    sleep(mySleepTime);
    mySleepTime = sleepTime;
    i++;
    Cuuid_create(&myUuid);
    Cuuid2string(baseName,256,&myUuid);
    if ( fullPath != NULL ) free(fullPath);
    fullPath = NULL;
    if ( (fullPath = (char *)malloc(strlen(myRootPath)+strlen(baseName)+2)) == NULL ) {
      log(LOG_ERR,"svcClassProbe(%s) malloc(): %s\n",mySvcClass,sstrerror(errno));
      return(NULL);
    }
    sprintf(fullPath,"%s/%s",myRootPath,baseName);

    /*
     * First: write the file
     */
    nbBytesWritten = 0;
    UPDATE_TIMING(mySvcClass,rfio_open_write_start);
    fd = rfio_open(fullPath,O_CREAT|O_TRUNC|O_WRONLY,0766);
    UPDATE_TIMING(mySvcClass,rfio_open_write_end);
    if ( fd == -1 ) {
      log(LOG_ERR,"rfio_open(%s): %s\n",fullPath,rfio_serror());
      UPDATE_ERROR(mySvcClass,rfio_open_write_error);
      continue;
    }
    UPDATE_TIMING(mySvcClass,rfio_write_start);
    while ( nbBytesWritten<nbBytesToWrite ) {
      size = nbBytesToWrite-nbBytesWritten;
      size = (size > bufferSize ? bufferSize : size);
      rc = rfio_write(fd,myWriteBuffer,size);
      if ( rc > 0 ) nbBytesWritten += rc;
      else {
        log(LOG_ERR,"rfio_write(%s,O_WRONLY): %s\n",fullPath,rfio_serror());
        UPDATE_ERROR(mySvcClass,rfio_write_error);
        break;
      }
    }
    UPDATE_TIMING(mySvcClass,rfio_write_end);
    UPDATE_TIMING(mySvcClass,rfio_close_write_start);
    rc = rfio_close(fd);
    UPDATE_TIMING(mySvcClass,rfio_close_write_end);
    if ( rc == -1 ) {
      log(LOG_ERR,"rfio_close(%s): %s\n",fullPath,rfio_serror());
      UPDATE_ERROR(mySvcClass,rfio_close_write_error);
      continue;
    }

    /*
     * Second: read it back
     */
    nbBytesRead = 0;
    UPDATE_TIMING(mySvcClass,rfio_open_read_start);
    fd = rfio_open(fullPath,O_RDONLY,0766);
    UPDATE_TIMING(mySvcClass,rfio_open_read_end);
    if ( fd == -1 ) {
      log(LOG_ERR,"rfio_open(%s,O_RDONLY): %s\n",fullPath,rfio_serror());
      UPDATE_ERROR(mySvcClass,rfio_open_read_error);
      continue;
    }
    UPDATE_TIMING(mySvcClass,rfio_read_start);
    while ( nbBytesRead<nbBytesToWrite ) {
      size = nbBytesToWrite-nbBytesRead;
      size = (size > bufferSize ? bufferSize : size);
      rc = rfio_read(fd,myReadBuffer,size);
      if ( rc > 0 ) nbBytesRead += rc;
      else {
        log(LOG_ERR,"rfio_read(%s): %s\n",fullPath,rfio_serror());
        UPDATE_ERROR(mySvcClass,rfio_read_error);
        break;
      }
      if ( rc == size ) {
        if ( memcmp(myReadBuffer,myWriteBuffer,rc) != 0 ) {
          log(LOG_ERR,"svcClassProbe(%s): data corruption after %d bytes?",
              mySvcClass,nbBytesRead);
          UPDATE_ERROR(mySvcClass,corruption_error);
        }       
      }
    }
    UPDATE_TIMING(mySvcClass,rfio_read_end);
    UPDATE_TIMING(mySvcClass,rfio_close_read_start);
    rc = rfio_close(fd);
    UPDATE_TIMING(mySvcClass,rfio_close_read_end);
    if ( rc == -1 ) {
      log(LOG_ERR,"rfio_close(%s): %s\n",fullPath,rfio_serror());
      UPDATE_ERROR(mySvcClass,rfio_close_read_error);
      continue;
    }

    /*
     * Third: query for it
     */
    path = strstr(fullPath,"path=");
    if ( path != NULL ) {
      path += strlen("path=");
      qryFile(mySvcClass,path);
    }

    /*
     * Fourth: remove it!
     */
    path = strstr(fullPath,"path=");
    if ( path != NULL ) {
      path += strlen("path=");
      rmFile(mySvcClass,path);
    }
  }
  
  return(NULL);
}

int main(int argc, char *argv[]) 
{
  char *svcClassStr = NULL;
  char *cmd = NULL;
  int thrPool = -1, nbThreads = -1;
  int ch, rc, i;
  TimingItem_t *item;
  struct passwd *pw;
  runUid = getuid();
  runGid = getgid();
  Coptind = 1;
  Copterr = 1;
  cmd = argv[0];
  while ((ch = Cgetopt_long(argc,argv,"h",longopts,NULL)) != EOF) {
    switch (ch) {
    case DirectoryName:
      directoryName = strdup(Coptarg);
      break;
    case StatFile:
      statFileName = strdup(Coptarg);
      break;
    case LogFile:
      logFileName = strdup(Coptarg);
      break;
    case BufferSize:
      bufferSize = atoi(Coptarg);
      break;
    case NbBytesToWrite:
      nbBytesToWrite = strutou64(Coptarg);
      break;
    case NbLoops:
      nbLoops = atoi(Coptarg);
      break;
    case SleepTime:
      sleepTime = atoi(Coptarg);
      break;
    case KeepErrorTime:
      keepErrorTime = atoi(Coptarg);
      break;
    case StageHost:
      stageHost = strdup(Coptarg);
      break;
    case SvcClasses:
      svcClassStr = strdup(Coptarg);
      rc = splitItemStr(
                        svcClassStr,
                        &svcClasses,
                        &nbSvcClasses
                        );
      if ( rc == -1 ) {
        fprintf(stderr,"Error parsing the service class string: %s\n",
                sstrerror(serrno));
        return(1);
      }
      break;
    case RunAsUser:
      pw = Cgetpwnam(Coptarg);
      if ( pw == NULL ) {
        fprintf(stderr,"Cgetpwname(): %s\n",sstrerror(serrno));
        return(1);
      }
      runUid = pw->pw_uid;
      runGid = pw->pw_gid;
      break;
    default:
      break;
    }
  }
  if ( setgid(runGid) == -1 ) {
    fprintf(stderr,"setguid(): %s\n",sstrerror(errno));
    return(1);
  }
  if ( setuid(runUid) == -1 ) {
    fprintf(stderr,"setuid(): %s\n",sstrerror(errno));
    return(1);
  }
  initlog("c2probe",LOG_INFO,logFileName);
  if ( help_flag != 0 || 
       directoryName == NULL ||
       svcClassStr == NULL ||
       stageHost == NULL) {
    if ( directoryName == NULL ) fprintf(stderr,"Directory name is required\n");
    if ( svcClassStr == NULL ) fprintf(stderr,"Service class list is required\n");
    if ( stageHost == NULL ) fprintf(stderr,"Stage host argument is required\n");
    usage(cmd);
    return(0);
  }

  fprintf(stdout,"%s starting with:\n\tdir=%s\n\tstatfile=%s\n\tlogfile=%s\n\tsvcclasses=%s\n\tuid,gid=%d,%d\n",cmd,directoryName,statFileName,logFileName,svcClassStr,runUid,runGid);

  if ( Cinitdaemon("c2probe",SIG_IGN) == -1 ) {
    exit(1);
  }
  
  thrPool = Cpool_create(nbSvcClasses,&nbThreads);
  if ( (thrPool == -1) || (nbSvcClasses > nbThreads) ) {
    log(LOG_ERR,"Cpool_init(): failed to get required number of threads (%d, got %d)\n",
        nbSvcClasses,nbThreads);
    exit(1);
  }

  for (i=0; i<nbSvcClasses; i++) {
    item = (TimingItem_t *)calloc(1,sizeof(TimingItem_t));
    if ( item == NULL ) {
      log(LOG_ERR,"calloc(1,%d): %s\n",sizeof(TimingItem_t),sstrerror(errno));
      exit(1);
    }
    item->svcClass = strdup(svcClasses[i]);
    CLIST_INSERT(timingList,item);    
    rc = Cpool_assign(thrPool,svcClassProbe,(void *)svcClasses[i],-1);
    if ( rc == -1 ) {
      log(LOG_ERR,"Cpool_assign(): %s\n",sstrerror(serrno));
      exit(1);
    }
  }
  for (;;) {
    sleep(10);
    dumpStat();
  }
  
  exit(0);
}

