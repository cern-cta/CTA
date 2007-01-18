/******************************************************************************
 *                      fsprobe.c
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
 * @(#)$RCSfile: fsprobe.c,v $ $Revision: 1.1 $ $Release$ $Date: 2007/01/18 10:14:43 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#define _GNU_SOURCE
#include <getopt.h>

extern char *optarg;
extern int optind, opterr, optopt;


char *buffer = NULL;
char *readBuffer = NULL;
char *directoryName = NULL;
char *pathName = NULL;
char *logFileName = "/tmp/fsprobe.log";
int bufferSize = 1024*1024;
unsigned long long fileSize = (unsigned long long )
		 (2*((unsigned long long )1024*1024*1024));
int sleepTime = 3600;
int sleepBetweenBuffers = 1;
int runInForeground = 0;
long nbLoops = 0;
int help_flag = 0;

#define MAXTIMEBUFLEN 128
char timebuf[MAXTIMEBUFLEN];
size_t timebuflen;
time_t now;
struct tm timestamp;

const enum RunOptions
{
  Noop,
  PathName,
  LogFile,
  BufferSize,
  FileSize,
  NbLoops,
  SleepTime,
  IOSleepTime,
  Foreground
} runOptions;

const struct option longopts[] = 
{
  {"help",no_argument,&help_flag,'h'},
  {"PathName",required_argument,NULL,PathName},
  {"LogFile",required_argument,NULL,LogFile},
  {"BufferSize",required_argument,NULL,BufferSize},
  {"FileSize",required_argument,NULL,FileSize},
  {"NbLoops",required_argument,NULL,NbLoops},
  {"SleepTime",required_argument,NULL,SleepTime},
  {"IOSleepTime",required_argument,NULL,IOSleepTime},
  {"Foreground",no_argument,&runInForeground,Foreground},
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
            (longopts[i].has_arg == no_argument ? "" : longopts[i].name));
  }
  return;
}

void prepareTimeStamp(void)
{
	now = time(NULL);
	if ( localtime_r(&now, &timestamp) ) {
		timebuflen = strftime(timebuf, MAXTIMEBUFLEN, "%F %T ", &timestamp);
	} else {
		timebuflen = strlen(strcpy(timebuf, "localtime() NULL??? "));
	}
}

void myLog(char *str) 
{
	int fd;
	
	if ( str == NULL ) return;
	prepareTimeStamp();
  if ( (runInForeground != 0) && (strcmp(logFileName,"stderr") == 0) ) {
    fd = 2;
  } else {
    fd = open(logFileName,O_WRONLY|O_CREAT|O_APPEND,0644);
  }
  
	if ( fd == -1 ) {
		fprintf(stderr, "%s %s", timebuf, str);
		return;
	}
	(void) write(fd, timebuf, timebuflen);
	write(fd,str,strlen(str));
	close(fd);
	return;
}

int initBuffers()
{
	int fdRandom = -1, i, randSize, rcRandom;
	
	buffer = (char *)malloc(bufferSize);
	if ( buffer == NULL ) {
		fprintf(stderr,"Cannot initialize buffer: malloc(%d) -> %s\n",bufferSize,
				    strerror(errno));
		return(-1);
	}
	readBuffer = (char *)malloc(bufferSize);
	if ( readBuffer == NULL ) {
		fprintf(stderr,"Cannot initialize buffer: malloc(%d) -> %s\n",bufferSize,
				    strerror(errno));
		return(-1);
	}
	fdRandom = open("/dev/urandom",O_RDONLY,0644);
	if ( fdRandom < 0 ) {
		fprintf(stderr,"open(/dev/urandom): %s\n",strerror(errno));
		return(-1);
	}
	i = randSize = 0;
	
	while ( randSize < bufferSize ) {
		rcRandom = read(fdRandom,buffer+i*512,512);
		if ( rcRandom < 0 ) {
			fprintf(stderr,"read(): %s\n",strerror(errno));
			return(-1);
		}
		i++;
		randSize += 512;
	}
	close(fdRandom);
	
	return(0);
}

int putInBackground() 
{
	pid_t pid;
	int fdnull = -1, i, maxfds;
	char logbuf[2048];

	if ( (pid = fork()) < 0 ) {
		fprintf(stderr,"failed to go to background, fork(): %s\n",strerror(errno));
		return(-1);
	} else {
		if ( pid > 0 ) {
			printf("starting in background, pid=%d...\n",(int)pid);
			exit(0);
		}
		fdnull = open("/dev/null",O_RDWR);
		maxfds = getdtablesize();
		setsid();
		close(0);
		dup2(fdnull,1);
		dup2(fdnull,2);
		for ( i=3; i<maxfds; i++ ) {
			if ( i != fdnull ) close(i);
		}
	}
	sprintf(logbuf, "fsprobe 0.2007011802 operational.\n");
	myLog(logbuf);
	return(0);
}

int writeFile() 
{
	int fd, rc, j, bytesToWrite;
	unsigned long long bytesWritten;
	char logbuf[2048];
	
	fd = open64(pathName,O_WRONLY|O_TRUNC|O_CREAT,0644);
	if ( fd == -1 ) {
		sprintf(logbuf,"open(%s) for write: %s\n",pathName,strerror(errno));
		myLog(logbuf);
		return(-1);
	}

	bytesWritten = 0;
	while ( bytesWritten < fileSize ) {
		sleep(sleepBetweenBuffers);
		j = 0;
		bytesToWrite = bufferSize;
		if ( fileSize-bytesWritten < (unsigned long long)bufferSize ) {
			bytesToWrite = (int)(fileSize-bytesWritten);
		}
		while ( j<bytesToWrite ) {
			rc = write(fd,buffer + j,bytesToWrite - j);
			if ( rc == -1 ) {
				sprintf(logbuf,"write(%s): %s\n",pathName,strerror(errno));
				myLog(logbuf);
				close(fd);
				return(-1);
			}
			j+=rc;
		}
		bytesWritten += (unsigned long long)bytesToWrite;
	}
	rc = close(fd);
	if ( rc == -1 ) {
		sprintf(logbuf,"close(%s): %s\n",pathName,strerror(errno));
		myLog(logbuf);
		return(-1);
	}
	return(0);
}

/* NOTE(fuji): There is no guarantee that read() will not read cached data,
 * i.e. corruptions on disk are not guaranteed to be exposed if the kernel
 * cache doesn't have a high turnover rate, IOW I/O load.  Currently, we
 * assume `sleepTime' number of seconds elapsed between write and read are
 * sufficiently long for the cache to change.
 */
int checkFile() 
{
	int fd, rc, i, j, bytesToRead, diffCount;
	unsigned long long bytesRead;
	char logbuf[2048];
	
	fd = open64(pathName,O_RDONLY,0644);
	if ( fd == -1 ) {
		sprintf(logbuf,"open(%s) for read: %s\n",pathName,strerror(errno));
		myLog(logbuf);
		return(-1);
	}

	bytesRead = 0;
	while ( bytesRead < fileSize ) {
		sleep(sleepBetweenBuffers);
		j = 0;
		bytesToRead = bufferSize;
		if ( fileSize-bytesRead < (unsigned long long)bufferSize ) {
			bytesToRead = (int)(fileSize-bytesRead);
		}
		while ( j<bytesToRead ) {
			rc = read(fd,readBuffer + j,bytesToRead - j);
			if ( rc == -1 ) {
				sprintf(logbuf,"read(%s): %s\n",pathName,strerror(errno));
				myLog(logbuf);
				close(fd);
				return(-1);
			}
			j+=rc;
		}

		diffCount = 0;
		for (i = 0; i < bytesToRead; i++) {
			if ( *(buffer+i) == *(readBuffer+i) ) {
				continue;
			}
			diffCount++;
			sprintf(logbuf, "diff: bufpos %d offset %llu expected %02x got %02x\n",
              i, bytesRead+i, 
              (unsigned char)*(buffer+i), 
              (unsigned char)*(readBuffer+i) );
			myLog(logbuf);
		}
		if ( diffCount ) {
			sprintf(logbuf, "total %d differing bytes found\n", diffCount);
			myLog(logbuf);
		}

		rc = memcmp(buffer,readBuffer,bytesToRead);
		if ( rc != 0 ) {
			sprintf(logbuf,"Corruption found in %s after %llu bytes\n",
				      pathName,bytesRead);
			myLog(logbuf);
			close(fd);
			sprintf(logbuf,
				      "tail /tmp/fsprobe.log |"
				      "mail -s corruption Olof.Barring@cern.ch Peter.Kelemen@cern.ch"
				      );
			system(logbuf);
			return(-2);
		}

		/* NOTE(fuji): Only do accounting here so that offsets are
		 * correct in any messages logged in above code. */
		bytesRead += (unsigned long long)bytesToRead;
	}
	rc = close(fd);
	if ( rc == -1 ) {
		sprintf(logbuf,"close(%s): %s\n",pathName,strerror(errno));
		myLog(logbuf);
		return(-1);
	}
	return(0);
}

int main(int argc, char *argv[]) 
{
	struct stat64 st;
	int rc;
  long cycle;
  char corruptPathName[1024];
  char *cmd, ch;
  
  optind = 1;
  opterr = 1;
  cmd = argv[0];
  while ((ch = getopt_long(argc,argv,"h",longopts,NULL)) != EOF) {
    switch (ch) {
    case PathName:
      pathName = strdup(optarg);
      break;
    case LogFile:
      logFileName = strdup(optarg);
      break;
    case BufferSize:
      bufferSize = atoi(optarg);
      break;
    case FileSize:
      fileSize = atoll(optarg);
      break;
    case NbLoops:
      nbLoops = atol(optarg);
      break;
    case SleepTime:
      sleepTime = atoi(optarg);
      break;
    case IOSleepTime:
      sleepBetweenBuffers = atoi(optarg);
      break;
    case 'h':
      usage(cmd);
    default:
      break;
    }
  }
	if ( pathName == NULL ) {
		fprintf(stderr,"Please provide a directory name with --FileName!!!\n");
    usage(cmd);
		exit(1);
	}

	/*
	 * Check that we can use the provided path
	 */
	rc = stat64(pathName,&st);
	if ( rc == 0 ) {
		if ( !S_ISREG(st.st_mode) ) {
			fprintf(stderr,"Cannot use path %s, not a regular filename\n",pathName);
			exit(1);
		}
	}
	rc = open64(pathName,O_WRONLY|O_CREAT,0644);
	if ( rc == -1 ) {
		fprintf(stderr,"Error opening path %s: %s\n",pathName,strerror(errno));
		exit(1);
	}
	close(rc);
	
	rc = unlink(pathName);
	if ( rc == -1 ) {
		fprintf(stderr,"Error removing path %s: %s\n",pathName,strerror(errno));
		exit(1);
	}

	/*
	 * Path is OK. Initialize...
	 */
	rc = initBuffers();
	if ( rc == -1 ) exit(1);

  if ( runInForeground == 0 ) {
    rc = putInBackground();
    if ( rc == -1 ) exit(1);
  }

  cycle=0;
	while ( (nbLoops == 0) || (cycle<nbLoops) ) {
    cycle++;
		rc = writeFile();
		if ( rc == 0 ) {
			rc = checkFile();
      if ( rc == -2 ) {      /* diff found */
        sprintf(corruptPathName,"%s.%lu",pathName,cycle);
        (void)rename(pathName,corruptPathName);
      }
		}
    (void)unlink(pathName);
		sleep(sleepTime);
	}
	exit(0);
}

