#include <Cthread_api.h>
#include <stdlib.h>
#include <stdio.h>
#include <Cglobals.h>   /* Get Cglobals_get prototype */
#include <serrno.h>

static int my_key = -1; /* Our static key, integer, init value -1 */
#define my_var (*C__my_var())

static int my_var_static; /* If Cglobals_get error in order not to crash */
void *doit _PROTO((void *));

int doit_v = 0;
#define NTHREAD 100

int *C__my_var()
{
  int *var;
  /* Call Cglobals_get */
  switch (Cglobals_get(&my_key,
                       (void **) &var,
                       sizeof(int)
                       )) {
  case -1:
    fprintf(stderr,"[%d] Cglobals_get error\n", Cthread_self());
    break;
  case 0:
    fprintf(stderr,"[%d] Cglobals_get OK\n", Cthread_self());
    break;
  case 1:
    fprintf(stderr,"[%d] Cglobals_get OK and first call\n", Cthread_self());
    break;
  default:
    fprintf(stderr,"[%d] Cglobals_get unknown return code\n", Cthread_self());
    break;
  }
  /* If error, var will be NULL */
  if (var == NULL) {
    fprintf(stderr,"[%d] Cglobals_get error : RETURN static ADDRESS!!!!!!!!!!!!\n", Cthread_self());
    return(&my_var_static);
  }
  return(var);
}

int main()
{
  int i;

  fprintf(stdout, "[%d] ---> Before any Cthread call\n", -1);
  fprintf(stdout, "[%d] Current my_var value is: %d\n", -1, my_var);
  fprintf(stdout, "[%d] Set my_var value to: %d\n", -1, 12);
  my_var = 12;
  fprintf(stdout, "[%d] Current my_var value is: %d\n", -1, my_var);
  fprintf(stdout, "[%d] Testing consistency\n", -1);
  if (my_var != 12) {
    fprintf(stdout, "[%d] Cglobals_get worked ok\n", -1);
    exit(1);
  }
  sleep(1);
  for (i = 0; i < NTHREAD; i++) {
    Cthread_create(&doit, &doit_v);
    doit_v++;
  }
  fprintf(stdout, "[%d] ---> After all Cthread_create calls\n", -1);
  fprintf(stdout, "[%d] Current my_var value is: %d\n", -1, my_var);
  fprintf(stdout, "[%d] Set my_var value to: %d\n", -1, NTHREAD * 10000 + 12);
  my_var = NTHREAD * 10000 + 12;
  fprintf(stdout, "[%d] Current my_var value is: %d\n", -1, my_var);
  fprintf(stdout, "[%d] Testing consistency\n", -1);
  if (my_var != (NTHREAD * 10000 + 12)) {
    fprintf(stdout, "[%d] Cglobals_get worked ok\n", -1);
    exit(1);
  }
  sleep(1);
  exit(0);
}

void *doit(arg)
     void *arg;
{
  int Tid;
  int doit = * (int *) arg;
  Cglobals_getTid(&Tid);
  my_var = (Tid + 1) * 100 + 12;
  fprintf(stdout, "[%d] my_var value is: %d (should be %d)\n", Cthread_self(), my_var, (Tid + 1) * 100 + 12);
  fprintf(stdout, "[%d] second call -- my_var value is: %d (should be %d)\n", Cthread_self(), my_var, (Tid + 1) * 100 + 12);
  fprintf(stdout, "[%d] Testing consistency\n", Cthread_self());
  if (my_var != ((Tid + 1) * 100 + 12)) {
    fprintf(stdout, "[%d] !!!!!!!!! ERROR !!!!!!!!!\n", Cthread_self());
    exit(1);
  } else {
    fprintf(stdout, "[%d] Cglobals_get worked ok\n", Cthread_self());
  }
  return(0);
}


  
