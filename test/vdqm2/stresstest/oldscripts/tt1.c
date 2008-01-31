#include <stdio.h>
#include <serrno.h>

int main(int argc, char *argv[]) {

    serrno = atoi(argv[1]);
    fprintf(stderr,"(%s:%d) serrno=0x%lx, %d\n",__FILE__,__LINE__,&serrno,serrno);
    tt2(serrno);
    fprintf(stderr,"(%s:%d) serrno=0x%lx, %d\n",__FILE__,__LINE__,&serrno,serrno)
;
    return(0);
}
