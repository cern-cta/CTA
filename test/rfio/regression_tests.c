/*
 * Copyright (C) 2004 by CERN-IT/ADC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: regression_tests.c,v $ $Revision: 1.1 $ $Date: 2004/03/01 16:18:20 $ CERN IT/ADC Olof Barring";
#endif /* lint */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#if !defined(_WIN32)
#include <dirent.h>
#endif
#include <pwd.h>
#include <grp.h>
#define RFIO_KERNEL 1
#include <rfio.h>
#include <serrno.h>

FILE *output;
time_t _tt;
char header[] = "--------------------------- start RFIO call -----------------------------------";
char footer[] = "----------------------------- end RFIO call -----------------------------------";
#define CALL_RFIO(X,Y) { fprintf(output,"\n%s\n",header); \
                        if ( strstr(#X,"test_file") != NULL ) \
                           fprintf(output,"   test_file = %s (%d,%d)\n",\
                                   test_file,\
				                           strlen(test_file),sizeof(test_file));\
                        if ( strstr(#X,"test_link") != NULL ) \
                           fprintf(output,"   test_link = %s (%d,%d)\n",\
                                   test_link,\
                                   strlen(test_link),sizeof(test_link));\
                        if ( strstr(#X,"file_buf") != NULL ) \
                           fprintf(output,"   file_buf = %s (%d,%d)\n",\
                                   file_buf,\
                                   strlen(file_buf),sizeof(file_buf));\
                        fprintf(output,"%d -> execute %s = %s ... ",\
                                time(&_tt),#Y,#X); \
			                  errno = serrno =0; \
                        if ( ( Y = X ) == -1 )  { \
			                      fprintf(output,"-> %s = 0x%x\n",#Y,Y); \
                            fprintf(output,"   %s: %s ...\n",#X,\
                                    rfio_serror());} \
			                  else fprintf(output," %s = 0x%x\n",#Y,Y);\
                        fprintf(output,"%s\n",footer);}

#define PCALL_RFIO(X,Y) { fprintf(output,"\n%s\n",header); \
                        if ( strstr(#X,"test_file") != NULL ) \
                           fprintf(output,"   test_file = %s (%d,%d)\n",\
                                   test_file,\
				                           strlen(test_file),sizeof(test_file));\
                        if ( strstr(#X,"test_link") != NULL ) \
                           fprintf(output,"   test_link = %s (%d,%d)\n",\
                                   test_link,\
				                           strlen(test_link),sizeof(test_link));\
                        if ( strstr(#X,"file_buf") != NULL ) \
                           fprintf(output,"   file_buf = %s (%d,%d)\n",\
                                   file_buf,\
                                   strlen(file_buf),sizeof(file_buf));\
                        fprintf(output,"%d -> execute %s = %s ... ",\
                                time(&_tt),#Y,#X); \
                        errno = serrno = 0; \
                        if ( ( Y = X ) == NULL ) { \
                            fprintf(output,"-> %s = 0x%x\n",#Y,Y); \
                            fprintf(output,"   %s: %s ...\n",#X,\
                                    rfio_serror());} \
                        else fprintf(output,"-> %s = non-NULL\n",#Y); \
                        fprintf(output,"%s\n",footer);}

#define TEST_SIZE(Z,X,Y) {if ( X.st_size == Y ) \
                            fprintf(output,"   %s returns correct size=%d\n",\
                                    #Z,Y); \
                          else \
                            fprintf(output,"   %s returns wrong size=%d, should be %d\n",\
                                    #Z,X.st_size,Y);}

#define SEND_TO_RFIOD(X,Y,Z) {fprintf(output,"\n%s\n",header); \
                              fprintf(output,"%d -> send(%s,%s) ... ", \
				    time(&_tt),magstr[X],reqstr[Y]); \
   				if ( (Z = netwrite(fd,buf,RQSTSIZE)) != RQSTSIZE ) { \
			          fprintf(output,"-> %s = 0x%x, should be 0x%x\n",#Z,Z,RQSTSIZE); \
                                  fprintf(output,"   send(%s,%s): %s ...\n",\
                                          magstr[X],reqstr[Y],rfio_serror());} \
			        else fprintf(output,"-> %s = 0x%x\n",#Z,Z);}


#define DO(X) {int __i; for (__i=0; __i<X; __i++) 
#define ENDDO(X) }


static long state1[32] = {
  3,
  0x9a319039, 0x32d9c024, 0x9b663182, 0x5da1f342,
  0x7449e56b, 0xbeb1dbb0, 0xab5c5918, 0x946554fd,
  0x8c2e680f, 0xeb3d799f, 0xb11ee0b7, 0x2d436b86,
  0xda672e2a, 0x1588ca88, 0xe369735d, 0x904f35f7,
  0xd7158fd6, 0x6fa6f051, 0x616e6b96, 0xac94efdc,
  0xde3b81e0, 0xdf0a6fb5, 0xf103bc02, 0x48f340fb,
  0x36413f93, 0xc622c298, 0xf5a42ab8, 0x8a88d77b,
  0xf5ad9d0e, 0x8999220b, 0x27fb47b9
};

static char host[30],test_file[1024],test_link[1024],file_buf[1024];

int main(int argc, char *argv[]) {
  char *test_path,*p,*p1;
  int rc,n;
  int bufsize = 1024*1024;
  struct stat st;
  mode_t mode = 0755;
  time_t tt;
  unsigned seed;
  uid_t uid;
  gid_t gid;

  output = stdout;
  if ( argc < 2 ) {
    fprintf(stderr,"Usage: %s [test-directory] [bufsize] [output-file]\n",
            argv[0]);
    return(2);
  }

  test_path = argv[1];
  if ( argc > 2 ) bufsize = atoi(argv[2]);
  if ( argc > 3 ) output = fopen(argv[3],"w");
  if ( rfio_stat(test_path,&st) ) {
    fprintf(stderr,"rfio_stat(%s): %s\n",test_path,rfio_serror());
    if ( ( rc = rfio_mkdir(test_path,mode) ) ) {
      fprintf(stderr,"rfio_mkdir(%s,0%o): %s\n",test_path,mode,rfio_serror());
      return(2);
    }
  }
  seed = 1;
  n = 128;
  initstate(seed,state1,n);
  setstate(state1);

  
  DO(2) {
    time(&tt);
    uid = getuid();
    gid = getgid();
    gethostname(host,30);
    sprintf(test_file,"%s/%s.%d.%d-%d",test_path,host,uid,gid,tt);

    p=NULL;
    rc = test_transfers(bufsize);
    rc = test_metadataop(test_path);
    /* 
     * Uncomment the following test if you want to test server
     * against dropped connection etc.
     */
    /* rc = test_exceptions(test_path); */

    break;
  } ENDDO(2);
  fclose(output);
  return(0);
}
  
int test_transfers(int bufsize) {
  long *buffer;
  int v,n,fd,rc,j,size,cnt;
  mode_t mode = 0755;
  struct stat st;
  
  /*
   * Test normal buffer mode
   */
  v = RFIO_READBUF;
  rfiosetopt(RFIO_READOPT,&v,4); 
  
  buffer = (long *)malloc(bufsize);
  if ( buffer == NULL ) {
    perror("malloc()");
    exit(2);
  }

  CALL_RFIO(rfio_open(test_file,O_CREAT | O_WRONLY, 0755),fd);
  size = 0;
  for (n = 0; n<10; n++) {
    for (j=0; j<bufsize/sizeof(long); j++) buffer[j] = random();
    CALL_RFIO(rfio_write(fd,buffer,bufsize),rc);
    size+=bufsize;
  }
  CALL_RFIO(rfio_fstat(fd,&st),rc);
  TEST_SIZE(rfio_fstat(fd,&st),st,size);
  CALL_RFIO(rfio_close(fd),rc);
  
  CALL_RFIO(rfio_open(test_file,O_RDONLY, 0755),fd);
  if ( fd >= 0 ) {
    size = 0;
    for (;;) {
      CALL_RFIO(rfio_read(fd,buffer,bufsize),rc);
      if ( rc == 0 ) break;
      size+=rc;
    }
    CALL_RFIO(rfio_close(fd),rc);
  } else {
    fprintf(output,"exceptional return %d from rfio_open(%s,%d,%d)\n",
            fd,test_file,O_RDONLY,0755);
    exit(2);
  }
  CALL_RFIO(rfio_stat(test_file,&st),rc);
  TEST_SIZE(rfio_stat(fd,&st),st,size);
  
  /*
   * Test V3 stream mode
   */
  CALL_RFIO(rfio_unlink(test_file),rc);
  v = RFIO_STREAM;
  rfiosetopt(RFIO_READOPT,&v,4); 
  
  CALL_RFIO(rfio_open(test_file,O_CREAT | O_WRONLY, 0755),fd);
  if ( fd >= 0 ) {
    size = 0;
    for (n = 0; n<10; n++) {
      for (j=0; j<bufsize/sizeof(long); j++) buffer[j] = random();
      rc = 0;
      cnt = 0;
      while ( cnt < bufsize ) {
        CALL_RFIO(rfio_write(fd,buffer+cnt,bufsize-cnt),rc);
        if ( rc < 0 ) break;
        cnt += rc;
        size+=rc;
      }
    }
    CALL_RFIO(rfio_close(fd),rc);
  } else {
    fprintf(output,"exceptional return %d from rfio_open(%s,%d,%d)\n",
            fd,test_file,O_RDONLY,0755);
    exit(2);
  }
  CALL_RFIO(rfio_stat(test_file,&st),rc);
  TEST_SIZE(rfio_stat(fd,&st),st,size);
  
  CALL_RFIO(rfio_open(test_file,O_RDONLY, 0755),fd);
  if ( fd >= 0 ) {
    size = 0;
    for (;;) {
      rc = 0;
      n = 0;
      while ( rc < bufsize ) {
        CALL_RFIO(rfio_read(fd,buffer+n,bufsize-n),rc);
        if ( rc <= 0 ) break;
        n += rc;
      }
      if ( rc <= 0 ) break;
    }
    CALL_RFIO(rfio_close(fd),rc);
  } else {
    fprintf(output,"exceptional return %d from rfio_open(%s,%d,%d)\n",
            fd,test_file,O_RDONLY,0755);
    exit(2);
  }
  
  /*
   * Reset to normal buffer mode
   */
  v = RFIO_READBUF;
  rfiosetopt(RFIO_READOPT,&v,4); 
  free(buffer);
  return(0);
}

int test_metadataop(char *test_path) {
  RDIR *dirp;
  struct dirent *de;
  mode_t mode = 0755;
  char *p1,*p;
  int rc,fd;
  time_t tt;
  struct stat st;
  uid_t uid;
  gid_t gid;

  uid = getuid();
  gid = getgid();
  time(&tt);

  CALL_RFIO(rfio_unlink(test_file),rc);
  CALL_RFIO(rfio_mkdir(test_file,mode),rc);
  p = &test_file[strlen(test_file)];
  strcat(test_file,"/test");

  CALL_RFIO(rfio_open(test_file,O_CREAT | O_WRONLY, 0755),fd);
  CALL_RFIO(rfio_close(fd),rc);
  
  CALL_RFIO(rfio_chown(test_file,0,0),rc);
  
  CALL_RFIO(rfio_stat(test_file,&st),rc);
  if ( st.st_uid || st.st_gid ) {
    fprintf(output,"   %s: owner=%d (should be 0), group=%d (should be 0)\n",
            test_file,st.st_uid,st.st_gid);
  }
  
  *p = '\0';
  CALL_RFIO(rfio_chmod(test_file,0600),rc);
  *p = '/';
  CALL_RFIO(rfio_stat(test_file,&st),rc);
  *p = '\0';
  CALL_RFIO(rfio_chmod(test_file,0755),rc);
  *p = '/';
  CALL_RFIO(rfio_stat(test_file,&st),rc);
  
  sprintf(test_link,"%s/%s.%d.%d-%d-link",test_path,host,uid,gid,tt);
  CALL_RFIO(rfio_symlink(test_file,test_link),rc);
  CALL_RFIO(rfio_readlink(test_link,file_buf,sizeof(file_buf)),rc);
  if ( (p1 = strstr(test_file,":/")) == NULL ) p1 = test_file;
  else p1++;
  if ( !strcmp(p1,file_buf) ) fprintf(output,"   rfio_readlink(%s) gives correctly %s\n",
				      test_link,file_buf);
  else fprintf(output,"   rfio_readlink(%s) returns %s, should be %s\n",
               test_link,file_buf,p1);
  CALL_RFIO(rfio_chmod(test_file,0400),rc);
  CALL_RFIO(rfio_stat(test_link,&st),rc);
  CALL_RFIO(rfio_lstat(test_link,&st),rc);
  CALL_RFIO(rfio_readlink(test_link,file_buf,sizeof(file_buf)),rc);
  CALL_RFIO(rfio_chmod(test_file,0755),rc);
  *p = '\0';
  CALL_RFIO(rfio_chmod(test_file,0600),rc);
  *p = '/';
  CALL_RFIO(rfio_stat(test_link,&st),rc);
  CALL_RFIO(rfio_lstat(test_link,&st),rc);
  CALL_RFIO(rfio_readlink(test_link,file_buf,sizeof(file_buf)),rc);
  *p = '\0';
  CALL_RFIO(rfio_chmod(test_file,0755),rc);
  *p = '/';
  
  strcpy(file_buf,test_link);
  sprintf(test_link,"%s/%s.%d.%d-%d-link1",test_path,host,uid,gid,tt);
  CALL_RFIO(rfio_rename(file_buf,test_link),rc);
  CALL_RFIO(rfio_readlink(test_link,file_buf,sizeof(file_buf)),rc);
  if ( (p1 = strstr(test_file,":/")) == NULL ) p1 = test_file;
  else p1++;
  if ( !strcmp(p1,file_buf) ) fprintf(output,"   rfio_readlink(%s) gives correctly %s\n",
				      test_link,file_buf);
  else fprintf(output,"   rfio_readlink(%s) returns %s, should be %s\n",
               test_link,file_buf,p1);
  
  *p = '\0';
  dirp = (RDIR *)NULL;
  de = (struct dirent *)NULL;
  PCALL_RFIO(rfio_opendir(test_file),dirp);
  DO(2) {
    for (;;) {
      PCALL_RFIO(rfio_readdir(dirp),de);
#ifdef linux
      if ( de == (struct dirent *)NULL || 
           *de->d_name == '\0' || 
           de->d_reclen == 0 ) break;
#else
      if ( de == (struct dirent *)NULL || 
           *de->d_name == '\0' || 
           de->d_namlen == 0 ) break;
#endif /* linux */
      sprintf(file_buf,"%s/%s",test_file,de->d_name);
      CALL_RFIO(rfio_access(file_buf,R_OK | W_OK | X_OK |F_OK),rc);
      CALL_RFIO(rfio_chmod(file_buf,0755),rc);
      CALL_RFIO(rfio_unlink(file_buf),rc);
    }
    if ( de == NULL ) {
      fprintf(output,"   exceptional return from rfio_readdir(%s): %s\n",
              test_file,rfio_serror());
    }
    CALL_RFIO(rfio_rewinddir(dirp),rc);
  } ENDDO(2);
  CALL_RFIO(rfio_closedir(dirp),rc);
  
  CALL_RFIO(rfio_rmdir(test_file),rc);
  return(0);
}

int test_exceptions(char *test_path) {
  int fd,rt,reqtype,magic,i,j,rc;
  char *remhost,*filename;
  int reqtypes[] =  {RQST_ERRMSG, RQST_OPEN, RQST_READ, RQST_WRITE, RQST_CLOSE, 
                     RQST_READAHEAD, RQST_FIRSTREAD, RQST_STAT, RQST_FSTAT, 
                     RQST_LSEEK, RQST_FIRSTSEEK, RQST_PRESEEK, RQST_LASTSEEK, 
                     RQST_XYOPEN, RQST_XYREAD, RQST_XYWRIT, RQST_XYCLOS, 
                     RQST_SYMLINK, RQST_STATFS, RQST_LSTAT, RQST_POPEN, 
                     RQST_PCLOSE ,RQST_FREAD, RQST_FWRITE, RQST_ACCESS, 
                     RQST_CHKCON, RQST_READLINK, RQST_MKDIR, RQST_CHOWN, 
                     RQST_RENAME, RQST_LOCKF, RQST_MSTAT, RQST_END, 
                     RQST_OPEN_V3, RQST_CLOSE_V3, RQST_READ_V3, RQST_WRITE_V3, 
                     RQST_OPENDIR, RQST_READDIR, RQST_CLOSEDIR, RQST_REWINDDIR, 
                     RQST_RMDIR, RQST_CHMOD, RQST_STAT_SEC, RQST_MSTAT_SEC, 
                     RQST_LSTAT_SEC};
  char reqstr[][20] = {"RQST_ERRMSG", "RQST_OPEN", "RQST_READ", "RQST_WRITE", 
                       "RQST_CLOSE", "RQST_READAHEAD", "RQST_FIRSTREAD", 
                       "RQST_STAT", "RQST_FSTAT", "RQST_LSEEK", 
                       "RQST_FIRSTSEEK", "RQST_PRESEEK", "RQST_LASTSEEK", 
                       "RQST_XYOPEN", "RQST_XYREAD", "RQST_XYWRIT", 
                       "RQST_XYCLOS", "RQST_SYMLINK", "RQST_STATFS", 
                       "RQST_LSTAT", "RQST_POPEN", "RQST_PCLOSE", "RQST_FREAD", 
                       "RQST_FWRITE", "RQST_ACCESS", "RQST_CHKCON", 
                       "RQST_READLINK", "RQST_MKDIR", "RQST_CHOWN", "RQST_RENAME",
                       "RQST_LOCKF", "RQST_MSTAT", "RQST_END", "RQST_OPEN_V3", 
                       "RQST_CLOSE_V3", "RQST_READ_V3", "RQST_WRITE_V3", 
                       "RQST_OPENDIR", "RQST_READDIR", "RQST_CLOSEDIR", 
                       "RQST_REWINDDIR", "RQST_RMDIR", "RQST_CHMOD", 
                       "RQST_STAT_SEC", "RQST_MSTAT_SEC", "RQST_LSTAT_SEC", ""}; 

  int magics[]      = { RFIO_MAGIC , B_RFIO_MAGIC};
  char magstr[][20] = {"RFIO_MAGIC","B_RFIO_MAGIC",""};
  char     buf[256];
  char *p;
  int len;

  if ( rfio_parseln(test_path,&remhost,&filename,NORDLINKS) ) {
    for (i=0; *reqstr[i] != '\0'; i++) {
      for (j=0; *magstr[j] != '\0'; j++) {
        reqtype = reqtypes[i];
        if ( reqtype == RQST_READAHEAD || reqtype == RQST_XYCLOS || 
             reqtype == RQST_OPEN_V3   || reqtype == RQST_CLOSE_V3 ||
             reqtype == RQST_READ_V3   || reqtype == RQST_WRITE_V3 ) continue;
        magic = magics[j];
        p = buf;
        marshall_WORD(p,magic);
        marshall_WORD(p,reqtype);
        len = 0;
        marshall_LONG(p,len);
        CALL_RFIO(rfio_connect(remhost,&rt),fd);
        if ( fd >= 0 ) {
          alarm(1);
          SEND_TO_RFIOD(j,i,rc);
          alarm(0);
          close(fd);
        }
      }
    }
  }
  return(0);
}
