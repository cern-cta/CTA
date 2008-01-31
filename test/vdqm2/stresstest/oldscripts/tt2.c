#include <stdio.h>
#include <serrno.h>

int tt2(int crap) {
   fprintf(stderr,"(%s:%d) crap=%d\n",__FILE__,__LINE__,crap);
   fprintf(stderr,"(%s:%d) serrno=0x%lx, %d\n",__FILE__,__LINE__,&serrno,serrno)
;
   serrno = crap+10;
   fprintf(stderr,"(%s:%d) serrno=0x%lx, %d\n",__FILE__,__LINE__,&serrno,serrno)
;

   return(0);
}
