#include <time.h>
#include <string.h>
#include <stdio.h>
#include <log.h>
#include <Cthread_api.h>
#define localtime(X) localtime_r(X,&ts)

int sub1(void *arg) {
    int Tid;
    Tid = Cthread_self();
    perror("Cthread_self()");
    log(LOG_INFO,"sub1 called with arg=%s, Tid = %d\n",(char *)arg,Tid);
    return(0);
}

int main(int argc, char *argv[]) {
    char buf[BUFSIZ];
    struct tm ts;
    struct tm *tp;
    time_t tt;

    initlog("tt5",LOG_DEBUG,"");
    tt = atoi(argv[1]);
    if ( tt <= 0 ) tt = time(NULL);
    tp = localtime(&tt);

    fprintf(stderr,"clock=%d, format =%s\n",(int)tt,argv[2]);
    (void)strftime(buf,BUFSIZ-1,argv[2],tp); 
    fprintf(stderr,"strftime(): %s\n",buf);
    (void) strcpy(buf,ctime(&tt)+strlen("Ddd "));
    buf[strlen(buf)-1-strlen(" YYYY")] = '\0';
    fprintf(stderr,"ctime(): %s\n",buf);

    log(LOG_ERR,"Hello world\n");

    Cthread_create(sub1,argv[2]);
    sleep(2);
    log(LOG_DEBUG,"argv[1]=%s, argv[2]=%s\n",argv[1],argv[2]);
    return(0);
}
