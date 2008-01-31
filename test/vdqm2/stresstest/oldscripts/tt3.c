#include <sys/types.h>
#include <pwd.h>
#include <trace.h>
#include <Cthread_api.h>
#include "getacct.h"
char *ypgetacct(struct passwd *, char *, char *);

int sub2(char *crap) {
	TRACE(2,"sub2","my stuff works with crap=%s",crap);
	sleep(1);
	TRACE(3,"sub2","or even at this level");
	return(0);
}

void *sub3(void *crap) {
        struct passwd *pwd = NULL;
        char *account = NULL;
        char *p, buf[1024];

	INIT_TRACE((char *)crap);

	TRACE(2,"sub3","I'm in a thread %d with arg=%s",Cthread_self(),(char *)crap);
        account = getenv(ACCOUNT_VAR);

        if (account != NULL) {
                if (strcmp(account, EMPTY_STR) == 0) account = NULL;
        }
        if ((pwd = getpwuid(getuid())) == NULL) return(NULL);

        p = ypgetacct(pwd,account,buf);
	sleep(1);
	TRACE(1,"sub3","my account is %s",p);
	sleep(1);
	TRACE(1,"sub3","and I'm still here");
	TRACE(3,"sub3","but probably not here");
	TRACE(1,"sub3","end of story");
	END_TRACE();
	return(NULL);
}

int main(int argc, char *argv[]) {
	struct passwd *pwd = NULL;
	char *account = NULL;
	char *p, buf[1024];
	INIT_TRACE(argv[0]);

	account = getenv(ACCOUNT_VAR);

   	if (account != NULL) {
        	if (strcmp(account, EMPTY_STR) == 0) account = NULL;
    	}
    	if ((pwd = getpwuid(getuid())) == NULL) return(NULL);

	p = ypgetacct(pwd,account,buf);

	TRACE(1,"main","here we are with argv[1]=%s, p=%s",argv[1],p);
	
	Cthread_create(sub3,(void *)argv[3]);
	sub2(argv[2]);
	sleep(1);
	TRACE(3,"main","again here after sub2(%s)",argv[2]);
        p = ypgetacct(pwd,account,buf);
	TRACE(2,"main","my account is still %s",p);
	END_TRACE();
	return(0);
}
