/*
 * $Id: Cupv_struct.h,v 1.3 2002/06/07 15:56:17 bcouturi Exp $
 */

/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: Cupv_struct.h,v $ $Revision: 1.3 $ $Date: 2002/06/07 15:56:17 $ CERN IT-PDP/DM Ben Couturier
 */
 
#ifndef _CUPV_STRUCT_H
#define _CUPV_STRUCT_H
 
#include "Castor_limits.h"

struct Cupv_userpriv {
  int uid;
  int gid;
  char srchost[CA_MAXREGEXPLEN + 1];
  char tgthost[CA_MAXREGEXPLEN + 1];
  int privcat;
};

#endif







