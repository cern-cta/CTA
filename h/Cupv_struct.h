/*
 * $Id: Cupv_struct.h,v 1.2 2002/05/28 17:24:42 bcouturi Exp $
 */

/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: Cupv_struct.h,v $ $Revision: 1.2 $ $Date: 2002/05/28 17:24:42 $ CERN IT-PDP/DM Ben Couturier
 */
 
#ifndef _CUPV_STRUCT_H
#define _CUPV_STRUCT_H
 
struct Cupv_userpriv {
  int uid;
  int gid;
  char srchost[MAXREGEXPLEN + 1];
  char tgthost[MAXREGEXPLEN + 1];
  int privcat;
};

#endif







