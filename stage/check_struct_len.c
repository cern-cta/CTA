/*
 * $Id: check_struct_len.c,v 1.3 2000/01/10 09:04:03 jdurand Exp $
 */

#include <stddef.h>            /* for NULL */
#include <sys/types.h>
#include "stage.h"
#include "stage_shift.h"

/* offset can be undef sometimes */
#ifndef offsetof
#define offsetof(s_name, s_member) \
        ((size_t)((char *)&((s_name *)NULL)->s_member - (char *)NULL))
#endif

int main() {
  struct stgcat_entry_old  stcp_old;
  struct stgcat_entry      stcp;
  struct stgpath_entry_old stpp_old;
  struct stgpath_entry     stpp;

  printf("... Old/New stgcat               length: %3d / %3d\n",
         (int) sizeof(stcp_old),
         (int) sizeof(stcp));
  printf("... Old/New stgcat.blksize       length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.blksize),
         (int) sizeof(stcp.blksize),
         (int) offsetof(struct stgcat_entry_old,blksize),
         (int) offsetof(struct stgcat_entry,blksize));
  printf("... Old/New stgcat.filler        length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.filler),
         (int) sizeof(stcp.filler),
         (int) offsetof(struct stgcat_entry_old,filler),
         (int) offsetof(struct stgcat_entry,filler));
  printf("... Old/New stgcat.charconv      length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.charconv),
         (int) sizeof(stcp.charconv),
         (int) offsetof(struct stgcat_entry_old,charconv),
         (int) offsetof(struct stgcat_entry,charconv));
  printf("... Old/New stgcat.keep          length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.keep),
         (int) sizeof(stcp.keep),
         (int) offsetof(struct stgcat_entry_old,keep),
         (int) offsetof(struct stgcat_entry,keep));
  printf("... Old/New stgcat.lrecl         length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.lrecl),
         (int) sizeof(stcp.lrecl),
         (int) offsetof(struct stgcat_entry_old,lrecl),
         (int) offsetof(struct stgcat_entry,lrecl));
  printf("... Old/New stgcat.nread         length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.nread),
         (int) sizeof(stcp.nread),
         (int) offsetof(struct stgcat_entry_old,nread),
         (int) offsetof(struct stgcat_entry,nread));
  printf("... Old/New stgcat.poolname      length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.poolname),
         (int) sizeof(stcp.poolname),
         (int) offsetof(struct stgcat_entry_old,poolname),
         (int) offsetof(struct stgcat_entry,poolname));
  printf("... Old/New stgcat.recfm         length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.recfm),
         (int) sizeof(stcp.recfm),
         (int) offsetof(struct stgcat_entry_old,recfm),
         (int) offsetof(struct stgcat_entry,recfm));
  printf("... Old/New stgcat.size          length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.size),
         (int) sizeof(stcp.size),
         (int) offsetof(struct stgcat_entry_old,size),
         (int) offsetof(struct stgcat_entry,size));
  printf("... Old/New stgcat.ipath         length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.ipath),
         (int) sizeof(stcp.ipath),
         (int) offsetof(struct stgcat_entry_old,ipath),
         (int) offsetof(struct stgcat_entry,ipath));
  printf("... Old/New stgcat.t_or_d        length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.t_or_d),
         (int) sizeof(stcp.t_or_d),
         (int) offsetof(struct stgcat_entry_old,t_or_d),
         (int) offsetof(struct stgcat_entry,t_or_d));
  printf("... Old/New stgcat.group         length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.group),
         (int) sizeof(stcp.group),
         (int) offsetof(struct stgcat_entry_old,group),
         (int) offsetof(struct stgcat_entry,group));
  printf("... Old/New stgcat.user          length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.user),
         (int) sizeof(stcp.user),
         (int) offsetof(struct stgcat_entry_old,user),
         (int) offsetof(struct stgcat_entry,user));
  printf("... Old/New stgcat.uid           length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.uid),
         (int) sizeof(stcp.uid),
         (int) offsetof(struct stgcat_entry_old,uid),
         (int) offsetof(struct stgcat_entry,uid));
  printf("... Old/New stgcat.gid           length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.gid),
         (int) sizeof(stcp.gid),
         (int) offsetof(struct stgcat_entry_old,gid),
         (int) offsetof(struct stgcat_entry,gid));
  printf("... Old/New stgcat.mask          length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.mask),
         (int) sizeof(stcp.mask),
         (int) offsetof(struct stgcat_entry_old,mask),
         (int) offsetof(struct stgcat_entry,mask));
  printf("... Old/New stgcat.reqid         length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.reqid),
         (int) sizeof(stcp.reqid),
         (int) offsetof(struct stgcat_entry_old,reqid),
         (int) offsetof(struct stgcat_entry,reqid));
  printf("... Old/New stgcat.status        length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.status),
         (int) sizeof(stcp.status),
         (int) offsetof(struct stgcat_entry_old,status),
         (int) offsetof(struct stgcat_entry,status));
  printf("... Old/New stgcat.actual_size   length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.actual_size),
         (int) sizeof(stcp.actual_size),
         (int) offsetof(struct stgcat_entry_old,actual_size),
         (int) offsetof(struct stgcat_entry,actual_size));
  printf("... Old/New stgcat.c_time        length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.c_time),
         (int) sizeof(stcp.c_time),
         (int) offsetof(struct stgcat_entry_old,c_time),
         (int) offsetof(struct stgcat_entry,c_time));
  printf("... Old/New stgcat.a_time        length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.a_time),
         (int) sizeof(stcp.a_time),
         (int) offsetof(struct stgcat_entry_old,a_time),
         (int) offsetof(struct stgcat_entry,a_time));
  printf("... Old/New stgcat.nbaccesses    length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.nbaccesses),
         (int) sizeof(stcp.nbaccesses),
         (int) offsetof(struct stgcat_entry_old,nbaccesses),
         (int) offsetof(struct stgcat_entry,nbaccesses));
  printf("... Old/New stgcat.u1            length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1),
         (int) sizeof(stcp.u1),
         (int) offsetof(struct stgcat_entry_old,u1),
         (int) offsetof(struct stgcat_entry,u1));
  printf("... Old/New stgcat.u1.t          length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.t),
         (int) sizeof(stcp.u1.t),
         (int) offsetof(struct stgcat_entry_old,u1.t),
         (int) offsetof(struct stgcat_entry,u1.t));
  printf("... Old/New stgcat.u1.t.den      length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.t.den),
         (int) sizeof(stcp.u1.t.den),
         (int) offsetof(struct stgcat_entry_old,u1.t.den),
         (int) offsetof(struct stgcat_entry,u1.t.den));
  printf("... Old/New stgcat.u1.t.dgn      length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.t.dgn),
         (int) sizeof(stcp.u1.t.dgn),
         (int) offsetof(struct stgcat_entry_old,u1.t.dgn),
         (int) offsetof(struct stgcat_entry,u1.t.dgn));
  printf("... Old/New stgcat.u1.t.fid      length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.t.fid),
         (int) sizeof(stcp.u1.t.fid),
         (int) offsetof(struct stgcat_entry_old,u1.t.fid),
         (int) offsetof(struct stgcat_entry,u1.t.fid));
  printf("... Old/New stgcat.u1.t.filstat  length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.t.filstat),
         (int) sizeof(stcp.u1.t.filstat),
         (int) offsetof(struct stgcat_entry_old,u1.t.filstat),
         (int) offsetof(struct stgcat_entry,u1.t.filstat));
  printf("... Old/New stgcat.u1.t.fseq     length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.t.fseq),
         (int) sizeof(stcp.u1.t.fseq),
         (int) offsetof(struct stgcat_entry_old,u1.t.fseq),
         (int) offsetof(struct stgcat_entry,u1.t.fseq));
  printf("... Old/New stgcat.u1.t.lbl      length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.t.lbl),
         (int) sizeof(stcp.u1.t.lbl),
         (int) offsetof(struct stgcat_entry_old,u1.t.lbl),
         (int) offsetof(struct stgcat_entry,u1.t.lbl));
  printf("... Old/New stgcat.u1.t.retentd  length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.t.retentd),
         (int) sizeof(stcp.u1.t.retentd),
         (int) offsetof(struct stgcat_entry_old,u1.t.retentd),
         (int) offsetof(struct stgcat_entry,u1.t.retentd));
  printf("... Old/New stgcat.u1.t.tapesrvr length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.t.tapesrvr),
         (int) sizeof(stcp.u1.t.tapesrvr),
         (int) offsetof(struct stgcat_entry_old,u1.t.tapesrvr),
         (int) offsetof(struct stgcat_entry,u1.t.tapesrvr));
  printf("... Old/New stgcat.u1.t.E_Tflags length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.t.E_Tflags),
         (int) sizeof(stcp.u1.t.E_Tflags),
         (int) offsetof(struct stgcat_entry_old,u1.t.E_Tflags),
         (int) offsetof(struct stgcat_entry,u1.t.E_Tflags));
  printf("... Old/New stgcat.u1.t.vid      length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.t.vid),
         (int) sizeof(stcp.u1.t.vid),
         (int) offsetof(struct stgcat_entry_old,u1.t.vid),
         (int) offsetof(struct stgcat_entry,u1.t.vid));
  printf("... Old/New stgcat.u1.t.vsn      length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.t.vsn),
         (int) sizeof(stcp.u1.t.vsn),
         (int) offsetof(struct stgcat_entry_old,u1.t.vsn),
         (int) offsetof(struct stgcat_entry,u1.t.vsn));
  printf("... Old/New stgcat.u1.d          length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.d),
         (int) sizeof(stcp.u1.d),
         (int) offsetof(struct stgcat_entry_old,u1.d),
         (int) offsetof(struct stgcat_entry,u1.d));
  printf("... Old/New stgcat.u1.d.xfile    length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.d.xfile),
         (int) sizeof(stcp.u1.d.xfile),
         (int) offsetof(struct stgcat_entry_old,u1.d.xfile),
         (int) offsetof(struct stgcat_entry,u1.d.xfile));
  printf("... Old/New stgcat.u1.d.Xparm    length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.d.Xparm),
         (int) sizeof(stcp.u1.d.Xparm),
         (int) offsetof(struct stgcat_entry_old,u1.d.Xparm),
         (int) offsetof(struct stgcat_entry,u1.d.Xparm));
  printf("... Old/New stgcat.u1.m          length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.m),
         (int) sizeof(stcp.u1.m),
         (int) offsetof(struct stgcat_entry_old,u1.d),
         (int) offsetof(struct stgcat_entry,u1.d));
  printf("... Old/New stgcat.u1.m.xfile    length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stcp_old.u1.m.xfile),
         (int) sizeof(stcp.u1.m.xfile),
         (int) offsetof(struct stgcat_entry_old,u1.m.xfile),
         (int) offsetof(struct stgcat_entry,u1.m.xfile));

  printf("... Old/New stgpath              length: %3d / %3d\n",
         (int) sizeof(stpp_old),
         (int) sizeof(stpp));
  printf("... Old/New stgpath.reqid        length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stpp_old.reqid),
         (int) sizeof(stpp.reqid),
         (int) offsetof(struct stgpath_entry_old,reqid),
         (int) offsetof(struct stgpath_entry,reqid));
  printf("... Old/New stgpath.upath        length: %3d / %3d     offset: %3d / %3d\n",
         (int) sizeof(stpp_old.upath),
         (int) sizeof(stpp.upath),
         (int) offsetof(struct stgpath_entry_old,upath),
         (int) offsetof(struct stgpath_entry,upath));

  return(0);
}
