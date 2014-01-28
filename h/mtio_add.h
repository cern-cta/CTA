/*
** $Id: mtio_add.h,v 1.1 2009/08/06 15:28:04 wiebalck Exp $
*/

/*
** This file defines the MTWEOFI ioctl to be able to write buffered tape marks.
** This definition is not in the official kernel yet, but will hopefully
** make it there at some point. For the time being, this header needs to be included
** instead. Something along the lines of 
**
** #include <linux/mtio.h>
** #ifndef MTWEOFI
** #include "mtio_add.h"
** #endif
**
** should make sure that the code is forward compatible.
**
** In order to successfully use this ioctls, loading the appropriate st driver is required.
*/ 

#ifndef _MTIO_ADD_H
#define _MTIO_ADD_H

/* in case MTWEOFI is not defined in linux/mtio.h */
#ifndef MTWEOFI
#define MTWEOFI 35
#endif

#endif
