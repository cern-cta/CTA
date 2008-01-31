#include <stdio.h>
#include <Cthread_api.h>
#include <serrno.h>
char *getconfent();
/* #define getconfent(X,Y,Z) getconfent(__FILE__,__LINE__,X,Y,Z) */
typedef struct th_arg {
	char category[80];
	char name[80];
	int flag;
} th_arg_t;
static int nisse = 0;

const int crap = 0; 

void *th1(void *arg) {
        char *category, *name, *p;
	int flag;
	th_arg_t *tharg;

	tharg = (th_arg_t *)arg;
	category = tharg->category;
	name = tharg->name;
	flag = tharg->flag;
        p = getconfent(category,name,flag);
        fprintf(stderr,"th1 (0x%Lx) %s:%s -> %s\n",p,category,name,p);
	fprintf(stderr,"th1: serrno = %d\n",serrno);
	serrno = 11;
        p = NULL;
        Cglobals_get(&nisse,&p,80);
        strcpy(p,"nisse e finast");
        fprintf(stderr,"(0x%lx) = %s\n",(unsigned long int)p,p);
	
        p = getconfent(category,name,flag);
        fprintf(stderr,"th1 (0x%Lx) %s:%s -> %s\n",p,category,name,p);

	sleep(1);
	fprintf(stderr,"th1: serrno = %d\n",serrno);
        Cglobals_get(&nisse,&p,80);
        fprintf(stderr,"(0x%lx) = %s\n",(unsigned long int)p,p);

	return((void *)&crap);
}

int main(int argc, char *argv[]) {
	char *category, *name, *p;
	int flag;
	th_arg_t *tharg;

	tharg = (th_arg_t *)malloc(sizeof(th_arg_t));
	serrno = 10;
	fprintf(stderr,"mth: serrno = %d\n",serrno);

	category = argv[1];
	name = argv[2];
	flag = atoi(argv[3]);

	strcpy(tharg->category,category);
	strcpy(tharg->name,name);
	tharg->flag = flag;
        p = getconfent(category,name,flag);
        fprintf(stderr,"(0x%lx) %s:%s -> %s\n",p,category,name,p);
	nisse = -1;
	p = NULL;
	Cglobals_get(&nisse,&p,80);
	strcpy(p,"nisse e fin");
	fprintf(stderr,"(0x%lx) = %s\n",(unsigned long int)p,p);

	Cthread_create_detached(th1,(void *)tharg);
        p = getconfent(category,name,flag);
        fprintf(stderr,"(0x%lx) %s:%s -> %s\n",p,category,name,p);

	p = getconfent(category,name,flag);
	fprintf(stderr,"(0x%lx) %s:%s -> %s\n",p,category,name,p);
        p = getconfent(category,name,flag);
        fprintf(stderr,"(0x%lx) %s:%s -> %s\n",p,category,name,p);
        strcpy(p,"nisse e kul");
        fprintf(stderr,"(%s:%d) p=%s\n",__FILE__,__LINE__,p);
	sleep(2);
        fprintf(stderr,"(%s:%d) p=%s\n",__FILE__,__LINE__,p);

        p = getconfent(category,name,flag);
        fprintf(stderr,"(0x%lx) %s:%s -> %s\n",p,category,name,p);
	sleep(2);
	fprintf(stderr,"mth: serrno = %d\n",serrno);
	sleep(5);
	Cglobals_get(&nisse,&p,80);
        fprintf(stderr,"(0x%lx) = %s\n",(unsigned long int)p,p);
        exit(0);
}	
