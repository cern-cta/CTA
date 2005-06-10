/******************************************************************************
 *                      parallelFileAccess.c
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
 * @(#)$RCSfile: parallelFileAccess.c,v $ $Revision: 1.10 $ $Release$ $Date: 2005/06/10 14:48:31 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: parallelFileAccess.c,v $ $Revision: 1.10 $ $Date: 2005/06/10 14:48:31 $ CERN IT/FIO Olof Barring";
#endif /* lint */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <serrno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <rfio.h>
#include <serrno.h>
#include <log.h>
#include <stage_api.h>
#include <Cuuid.h>
#include <Cgetopt.h>
#include <Cthread_api.h>

static int success = 0;

int help_flag = 0;
int dropConnection = 0;
char *baseFileName = NULL;
FILE *dumpfp;

enum RunOptions
{
  DirectoryName,
  DumpFile,
  LogFile,
  NbParallelOpens,
  NbBuffersToWrite,
  NbLoops,
  SizeOfBuffer,
  DropConnection
} runOptions;

static struct Coptions longopts[] = 
{
  {"help",NO_ARGUMENT,&help_flag,'h'},
  {"NbParallelOpens",REQUIRED_ARGUMENT,0,NbParallelOpens},
  {"DirectoryName",REQUIRED_ARGUMENT,0,DirectoryName},
  {"DumpFile",REQUIRED_ARGUMENT,0,DumpFile},
  {"LogFile",REQUIRED_ARGUMENT,0,LogFile},
  {"NbBuffersToWrite",REQUIRED_ARGUMENT,0,NbBuffersToWrite},
  {"NbLoops",REQUIRED_ARGUMENT,0,NbLoops},
  {"SizeOfBuffer",REQUIRED_ARGUMENT,0,SizeOfBuffer},
  {"DropConnection",NO_ARGUMENT,&dropConnection,DropConnection},
  {NULL, 0, NULL, 0}
};

typedef struct TimingInfo 
{
  struct timeval threadLaunchedTime;
  struct timeval openEnterTime;
  struct timeval openExitTime;
  struct timeval closeEnterTime;
  struct timeval closeExitTime;
  int threadId;
  int fd;
  int openFailed;
  int open_serrno;
  int open_rfio_errno;
  int open_errno;
  int writeFailed;
  int write_serrno;
  int write_rfio_errno;
  int write_errno;
  int readFailed;
  int read_serrno;
  int read_rfio_errno;
  int read_errno;
} TimingInfo_t;
TimingInfo_t *timingInfo = NULL;

char *bufferToWrite = NULL;
int sizeOfBuffer = 0;
int nbBuffersToWrite = 0;

int startFlag = 0;
void *startFlagLock = NULL;

#define LOG_ERROR(TXT) { \
  log(LOG_ERR,"Error: %s, serrno=%d, rfio_errno=%d, errno=%d, %s\n", \
          TXT,serrno,rfio_errno,errno,rfio_serror());}

void log_callback(int level, char *msg) {
  int logLevel = LOG_INFO;
  if ( level == MSG_ERR ) logLevel = LOG_ERR;
  log(logLevel,"STAGE API LOG MESSAGE: %s\n",msg);
}

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

void dumpTiming(
                mode,
                nbThreads
                )
     char *mode;
     int nbThreads;
{
  int i;
  
  for (i=0; i<nbThreads; i++) {
    fprintf(dumpfp,"%s %d.%6.6d %d.%6.6d %d.%6.6d %d.%6.6d %d.%6.6d ",
            mode,
            timingInfo[i].threadLaunchedTime.tv_sec,
            timingInfo[i].threadLaunchedTime.tv_usec,
            timingInfo[i].openEnterTime.tv_sec,
            timingInfo[i].openEnterTime.tv_usec,
            timingInfo[i].openExitTime.tv_sec,
            timingInfo[i].openExitTime.tv_usec,
            timingInfo[i].closeEnterTime.tv_sec,
            timingInfo[i].closeEnterTime.tv_usec,
            timingInfo[i].closeExitTime.tv_sec,
            timingInfo[i].closeExitTime.tv_usec);
    fprintf(dumpfp,"%d %d %d %d %d %d %d %d %d %d %d %d %d %d\n",
            timingInfo[i].threadId,
            timingInfo[i].fd,
            timingInfo[i].openFailed,
            timingInfo[i].open_serrno,
            timingInfo[i].open_rfio_errno,
            timingInfo[i].open_errno,
            timingInfo[i].writeFailed,
            timingInfo[i].write_serrno,
            timingInfo[i].write_rfio_errno,
            timingInfo[i].write_errno,
            timingInfo[i].readFailed,
            timingInfo[i].read_serrno,
            timingInfo[i].read_rfio_errno,
            timingInfo[i].read_errno);

  }
  return;
}

int initStartFlag() 
{
  int rc;
  rc = Cthread_mutex_lock((void*)&startFlag);
  if ( rc == -1 ) {
    log(LOG_ERR,
        "rtcpd_InitProcCntl() Cthread_mutex_lock(%s): %s\n",
        "startFlag",sstrerror(serrno));
    return(-1);
  }
  rc = Cthread_mutex_unlock((void*)&startFlag);
  if ( rc == -1 ) {
    log(LOG_ERR,
        "rtcpd_InitProcCntl() Cthread_mutex_unlock(%s): %s\n",\
        "startFlag",sstrerror(serrno));
    return(-1);
  }
  startFlagLock = Cthread_mutex_lock_addr((void*)&startFlag);
  if ( startFlagLock == NULL ) {
    log(LOG_ERR,
        "rtcpd_InitProcCntl() Cthread_mutex_lock_addr(%s): %s\n",
        "startFlag",sstrerror(serrno));
    return(-1);
  }
  return(0);
}

void *fileWriteThread(
                     arg
                     )
     void *arg;
{
  int myIndex, fd, i, rc, save_serrno, save_rfio_errno, save_errno;
  char *myFile;
  char *myBuffer = NULL;

  myIndex = *(int *)arg;
  free(arg);
  stage_setlog(&log_callback);
  gettimeofday(&timingInfo[myIndex].threadLaunchedTime,NULL);
  myFile = (char *) malloc(strlen(baseFileName)+10);
  if ( myFile == NULL ) {
    LOG_ERROR("malloc()");
    return((void *)&success);
  }
  sprintf(myFile,"%s_%d",baseFileName,myIndex);

  rc = Cthread_mutex_lock_ext(startFlagLock);
  if ( rc == -1 ) {
    LOG_ERROR("Cthread_mutex_lock_ext()");
    free(myFile);
    return((void *)&success);
  }

  while (startFlag == 0 ) {
    rc = Cthread_cond_wait_ext(startFlagLock);
    if ( rc == -1 ) {
      LOG_ERROR("Cthread_cond_wait_ext()");
      free(myFile);
      return((void *)&success);
    }
  }
  (void)Cthread_mutex_unlock_ext(startFlagLock);
  
  gettimeofday(&timingInfo[myIndex].openEnterTime,NULL);
  serrno = rfio_errno = errno = 0;
  fd = timingInfo[myIndex].fd = rfio_open(myFile,O_TRUNC|O_CREAT|O_WRONLY,0644);
  if ( fd == -1 ) {
    timingInfo[myIndex].open_errno = save_errno = errno;
    timingInfo[myIndex].open_rfio_errno = save_rfio_errno = rfio_errno;
    timingInfo[myIndex].open_serrno = save_serrno = serrno;
    timingInfo[myIndex].openFailed = 1;
    log(LOG_ERR,"rfio_open(%s,O_WRONLY): serrno=%d, rfio_errno=%d, errno=%d %s\n",
        myFile,save_serrno,save_rfio_errno,save_errno,rfio_serror());
    free(myFile);
    return((void *)&success);
  }
  gettimeofday(&timingInfo[myIndex].openExitTime,NULL);

  myBuffer = bufferToWrite;
  for ( i=0; i<nbBuffersToWrite; i++ ) {
    rc = rfio_write(fd,myBuffer,sizeOfBuffer);
    if ( rc != sizeOfBuffer ) {
      timingInfo[myIndex].write_errno = errno;
      timingInfo[myIndex].write_rfio_errno = rfio_errno;
      timingInfo[myIndex].write_serrno = serrno;
      timingInfo[myIndex].writeFailed = 1;
      LOG_ERROR("rfio_write()");
      break; 
    }
  }
  
  if ( dropConnection == 0 ) {
    gettimeofday(&timingInfo[myIndex].closeEnterTime,NULL);
    rfio_close(fd);
    gettimeofday(&timingInfo[myIndex].closeExitTime,NULL);
  } else {
    close(fd);
  }
  free(myFile);
  return((void *)&success);
}

void *fileReadThread(
                     arg
                     )
     void *arg;
{
  int myIndex, fd, i, rc, save_serrno, save_rfio_errno, save_errno;
  char *myFile;
  char *myBuffer = NULL;

  myIndex = *(int *)arg;
  free(arg);
  stage_setlog(&log_callback);
  myFile = (char *) malloc(strlen(baseFileName)+10);
  if ( myFile == NULL ) {
    LOG_ERROR("malloc()");
    return((void *)&success);
  }
  sprintf(myFile,"%s_%d",baseFileName,myIndex);

  rc = Cthread_mutex_lock_ext(startFlagLock);
  if ( rc == -1 ) {
    LOG_ERROR("Cthread_mutex_lock_ext()");
    free(myFile);
    return((void *)&success);
  }

  while (startFlag == 0 ) {
    rc = Cthread_cond_wait_ext(startFlagLock);
    if ( rc == -1 ) {
      LOG_ERROR("Cthread_cond_wait_ext()");
      free(myFile);
      return((void *)&success);
    }
  }
  (void)Cthread_mutex_unlock_ext(startFlagLock);

  gettimeofday(&timingInfo[myIndex].openEnterTime,NULL);
  serrno = rfio_errno = errno = 0;
  fd = rfio_open(myFile,O_RDONLY,0644);
  timingInfo[myIndex].fd = fd;
  if ( fd == -1 ) {
    timingInfo[myIndex].open_errno = save_errno = errno;
    timingInfo[myIndex].open_rfio_errno = save_rfio_errno = rfio_errno;
    timingInfo[myIndex].open_serrno = save_serrno = serrno;
    timingInfo[myIndex].openFailed = 1;
    log(LOG_ERR,"rfio_open(%s,O_RDONLY): serrno=%d, rfio_errno=%d, errno=%d %s\n",
        myFile,save_serrno,save_rfio_errno,save_errno,rfio_serror());
    free(myFile);
    return((void *)&success);
  }
  gettimeofday(&timingInfo[myIndex].openExitTime,NULL);
  sleep(1);

  myBuffer = (char *) malloc(sizeOfBuffer);
  if ( myBuffer == NULL ) {
    LOG_ERROR("malloc()");
  } else {
    for ( i=0; i<nbBuffersToWrite; i++ ) {
      rc = rfio_read(fd,myBuffer,sizeOfBuffer);
      if ( rc != sizeOfBuffer ) {
        timingInfo[myIndex].read_errno = errno;
        timingInfo[myIndex].read_rfio_errno = rfio_errno;
        timingInfo[myIndex].read_serrno = serrno;
        timingInfo[myIndex].readFailed = 1;
        LOG_ERROR("rfio_read()");
        break; 
      }
    }
    free(myBuffer);
  }
  
  if ( dropConnection == 0 ) {
    gettimeofday(&timingInfo[myIndex].closeEnterTime,NULL);
    rfio_close(fd);
    gettimeofday(&timingInfo[myIndex].closeExitTime,NULL);
  } else {
    close(fd);
  }
  free(myFile);
  return((void *)&success);
}

int startThreads(
                 nbThreads,
                 threadRoutine
                 )
     int nbThreads;
     void *(*threadRoutine) _PROTO((void *));
{
  int rc, i, *arg;

  for ( i=0; i<nbThreads; i++ ) {
    arg = (int *)malloc(sizeof(int));
    if ( arg == NULL ) {
      LOG_ERROR("malloc()");
      return(-1);
    }
    *arg = i;
    rc = Cthread_create(threadRoutine,(void *)arg);
    if ( rc == -1 ) {
      LOG_ERROR("Cthread_create()");
      return(-1);
    }
    timingInfo[i].threadId = rc;
  }
  return(0);
}

int waitAllThreads(
                   nbThreads
                   )
     int nbThreads;
{
  int rc, i, *status;
  
  for ( i=0; i<nbThreads; i++ ) {
    status = NULL;
    rc = Cthread_join(timingInfo[i].threadId,&status);
    if ( rc == -1 ) {
      LOG_ERROR("Cthread_join()");
      return(-1);
    }
  }
  return(0);
}

int main(
         argc,
         argv
         )
     int argc;
     char **argv;
{
  int ch, rc, nbParallelOpens = 1, nbLoops = 1, i;
  char *cmd, *directoryName = NULL, *dumpFileName, *logfile = "";
  Cuuid_t myCuuid;
  char myCuuidStr[50];

  fclose(stdin);
  Coptind = 1;
  Copterr = 1;
  cmd = argv[0];
  while ((ch = Cgetopt_long(argc,argv,"h",longopts,NULL)) != EOF) {
    switch (ch) {
    case DirectoryName:
      directoryName = strdup(Coptarg);
      break;
    case DumpFile:
      dumpFileName = strdup(Coptarg);
      dumpfp = fopen(dumpFileName,"a+");      
      break;
    case LogFile:
      logfile = strdup(Coptarg);
      break;
    case NbParallelOpens:
      nbParallelOpens = atoi(Coptarg);
      break;
    case NbBuffersToWrite:
      nbBuffersToWrite = atoi(Coptarg);
      break;
    case NbLoops:
      nbLoops = atoi(Coptarg);
      break;
    case SizeOfBuffer:
      sizeOfBuffer = atoi(Coptarg);
      break;
    case DropConnection:
      dropConnection = 1;
      break;
    default:
      break;
    }
  }
  initlog(cmd,LOG_INFO,logfile);
  if ( help_flag != 0 || directoryName == NULL ) {
    if ( directoryName == NULL ) fprintf(stderr,"Directory name is required\n");
    usage(cmd);
    return(0);
  }
  if ( nbParallelOpens <= 0 ) {
    fprintf(stderr,"Number of parallel opens must be >0 (%d)\n",
            nbParallelOpens);
    usage(cmd);
    return(0);
  }

  baseFileName = (char *)calloc(1,strlen(directoryName)+50);
  if ( baseFileName == NULL ) {
    LOG_ERROR("calloc()");
    return(1);
  }
  Cuuid_create(&myCuuid);
  memset(myCuuidStr,'\0',sizeof(myCuuidStr));
  Cuuid2string(myCuuidStr,49,&myCuuid);
  sprintf(baseFileName,"%s/%s",directoryName,myCuuidStr);
  rc = initStartFlag();
  if ( rc == -1 ) return(1);

  timingInfo = (TimingInfo_t *)calloc(nbParallelOpens,
                                      sizeof(TimingInfo_t));
  if ( timingInfo == NULL ) {
    LOG_ERROR("calloc()");
    return(1);
  }

  if ( sizeOfBuffer > 0 ) {
    bufferToWrite = (char *)malloc(sizeOfBuffer);
    if ( bufferToWrite == NULL ) {
      LOG_ERROR("malloc()");
      return(1);
    }
  }

  for ( i=0; i<nbLoops; i++ ) {
    sprintf(baseFileName,"%s/%s_%6.6d",directoryName,myCuuidStr,i);
    startFlag = 0;
    /*
     * Start the writer threads
     */
    log(LOG_INFO,"Start %d write threads\n",nbParallelOpens);
    rc = startThreads(
                      nbParallelOpens,
                      fileWriteThread
                      );
    if ( rc == -1 ) {
      LOG_ERROR("startThreads()");
      return(1);
    }
    log(LOG_INFO,"All %d write threads started\n",nbParallelOpens);
    
    /*
     * Launch the 'open'
     */
    rc = Cthread_mutex_lock_ext(startFlagLock);
    if ( rc == -1 ) {
      LOG_ERROR("Cthread_mutex_lock_ext()");
      return(1);
    }
    
    startFlag = 1;
    log(LOG_INFO,"Send 'open' signal\n");
    rc = Cthread_cond_broadcast_ext(startFlagLock);
    if ( rc == -1 ) {
      LOG_ERROR("Cthread_cond_broadcast_ext()");
      return(1);
    }
    
    rc = Cthread_mutex_unlock_ext(startFlagLock);
    if ( rc == -1 ) {
      LOG_ERROR("Cthread_mutex_unlock_ext()");
      return(1);
    }
    
    log(LOG_INFO,"Wait for all writer threads to return\n");
    rc = waitAllThreads(nbParallelOpens);
    if ( rc == -1 ) {
      LOG_ERROR("waitAllThreads()");
      return(1);
    }
    log(LOG_INFO,"All writer threads to returned\n");
    dumpTiming("WRITE",nbParallelOpens);
    startFlag = 0;
    
    /*
     * Start the reader threads
     */
    log(LOG_INFO,"Start %d read threads\n",nbParallelOpens);
    rc = startThreads(
                      nbParallelOpens,
                      fileReadThread
                      );
    if ( rc == -1 ) {
      LOG_ERROR("startThreads()");
      return(1);
    }
    log(LOG_INFO,"All %d read threads started\n",nbParallelOpens);
    
    /*
     * Launch the 'open'
     */
    rc = Cthread_mutex_lock_ext(startFlagLock);
    if ( rc == -1 ) {
      LOG_ERROR("Cthread_mutex_lock_ext()");
      return(1);
    }
    
    startFlag = 1;
    log(LOG_INFO,"Send 'open' signal\n");
    rc = Cthread_cond_broadcast_ext(startFlagLock);
    if ( rc == -1 ) {
      LOG_ERROR("Cthread_cond_broadcast_ext()");
      return(1);
    }
    
    rc = Cthread_mutex_unlock_ext(startFlagLock);
    if ( rc == -1 ) {
      LOG_ERROR("Cthread_mutex_unlock_ext()");
      return(1);
    }
    
    log(LOG_INFO,"Wait for all reader threads to return\n");
    rc = waitAllThreads(nbParallelOpens);
    if ( rc == -1 ) {
      LOG_ERROR("waitAllThreads()");
      return(1);
    }
    log(LOG_INFO,"All reader threads to returned\n");
    dumpTiming("READ",nbParallelOpens);
  } /* for ( i=0; i<nbLoops; i++ ) */

  fclose(dumpfp);
    
}
