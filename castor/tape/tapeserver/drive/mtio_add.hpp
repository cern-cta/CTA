/*
* Define the write immediate file mark operation code in case it is absent from mtio.h
*/ 

#ifndef _MTIO_ADD_H
#define _MTIO_ADD_H

/* in case MTWEOFI is not defined in linux/mtio.h */
#ifndef MTWEOFI
#define MTWEOFI 35
#endif

#endif
