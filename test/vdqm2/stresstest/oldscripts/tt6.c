#include <stdio.h>
#include <stdlib.h>
#define CLIST_ITERATE_BEGIN(X,Y) {if ( (X) != NULL ) {(Y)=(X); do {
#define CLIST_ITERATE_END(X,Y) Y=(Y)->next; } while ((X) != (Y));}}
#define CLIST_INSERT(X,Y) {if ((X) == NULL) {X=(Y); (X)->next = (X)->prev = (X);} \
else {(Y)->next=(X); (Y)->prev=(X)->prev; (Y)->next->prev=(Y); (Y)->prev->next=(Y);}}
#define CLIST_DELETE(X,Y) {if ((Y) != NULL) {if ( (Y) == (X) ) (X)=(X)->next; if((Y)->prev != (Y) && (Y)->next != (Y)) {\
(Y)->prev->next = (Y)->next; (Y)->next->prev = (Y)->prev;} else {(X)=NULL;}}}


typedef struct crap {
    void *crapule;
    int nisse;
    char value[10];
    struct crap *next;
    struct crap *prev;
} crap_t;

int new_list(crap_t **b,char *value) {
   if ( b == NULL || value == NULL ) return(-1);
   *b = (crap_t *)calloc(1,sizeof(crap_t));
   if ( *b == NULL ) {perror("malloc()"); return(-1);}
   strcpy((*b)->value,value);
   return(0);
}
int add_list(crap_t **a, crap_t *b) {
   CLIST_INSERT(*a,b);
}
int delete_list(crap_t **a, crap_t *b) {
   CLIST_DELETE(*a,b);
}

int print_list(crap_t *a) {
    crap_t *b;
    fprintf(stderr,"\n\nPrint list at 0x%lx\n",a);
    CLIST_ITERATE_BEGIN(a,b) {
       fprintf(stderr,"0x%lx->next = 0x%lx, 0x%lx->prev = 0x%lx\n",b,b->next,b,b->prev);
       fprintf(stderr,"\t0x%lx->value = %s\n",b,b->value);
    } CLIST_ITERATE_END(a,b);
}

int main(int argc, char *argv[]) {
    crap_t *otto = NULL;
    crap_t *a,*b;
    int i;

    for (i=1; i<argc; i++) {
         new_list(&a,argv[i]);
         add_list(&otto,a);
         print_list(otto);
    }

    CLIST_ITERATE_BEGIN(otto,a) {
        if ( !strcmp(a->value,"ADD") ) {
            new_list(&b,"NEW");
            add_list(&otto,b);
        }
        if ( !strcmp(a->value,"DEL") ) {
            b = a;
            a = a->prev;
            fprintf(stderr,"Delete element at 0x%lx (%s)\n",b,b->value);
            delete_list(&otto,b);
            free(b);
        }
    } CLIST_ITERATE_END(otto,a);
    fprintf(stderr,"At end:\n");
    print_list(otto);
}
