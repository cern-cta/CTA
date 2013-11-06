/*
 * $Id: Cns_struct.h,v 1.2 2006/01/26 15:32:41 bcouturi Exp $
 */

/*
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 */

#ifndef _CNS_STRUCT_H
#define _CNS_STRUCT_H

			/* structures common to Name server client and server */

struct Cns_acl {
	unsigned char	a_type;
	int		a_id;
	unsigned char	a_perm;
};
#endif
