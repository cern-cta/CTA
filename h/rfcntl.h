/*
 * $Id: rfcntl.h,v 1.3 2009/05/13 10:05:35 sponcec3 Exp $
 */

/*
 * @(#)rfcntl.h 1.10 06/04/98  CERN CN-SW/DC Antoine Trannoy, Jean-Philippe Baud
 */

/*
 * Copyright (C) 1990-1998 by CERN/CN/SW/DC
 * All rights reserved
 */

/* rfcntl.h  Macros to convert flags to a single set */

/*
 * The SHIFT set of flags is the one defined in Irix (SGI), Unicos (CRAY) and on Apollo machines.
 * These flags do not have the same value on linux machines, or Macs.
 *
 *           SHIFT    LINUX    APPLE
 *
 * O_RDONLY  000000   000000   000000
 * O_WRONLY  000001   000001   000001
 * O_RDWR    000002   000002   000002
 * O_NDELAY  000004   004000   000004
 * 0_APPEND  000010   002000   000010
 * O_CREAT   000400   000100   001000
 * O_TRUNC   001000   001000   002000
 * O_EXCL    002000   000200   004000
 */

#if defined(linux) || defined(_WIN32) || defined(__APPLE__)

#if defined(_WIN32)
#define htonopnflg(A)  (A)
#define ntohopnflg(A)  (A)
#endif /* WIN32 */

#if defined(linux)
#define htonopnflg(A)  (((A) & 004000) >> 9) | (((A) & 002000) >> 7) |  \
  (((A) & 000100) << 2) | (((A) & 000200) << 3) |                       \
  ((A) & 001003)
#define ntohopnflg(A)  (((A) & 000004) << 9) | (((A) & 000010) << 7) |  \
  (((A) & 000400) >> 2) | (((A) & 002000) >> 3) |                       \
  ((A) & 001003)
#endif /* linux */

#if defined(__APPLE__)
#define htonopnflg(A)  (((A) & 007000) >> 1) | ((A) & 000017)
#define ntohopnflg(A)  (((A) & 003400) << 1) | ((A) & 000017)
#endif /* APPLE */


#endif /* linux || _WIN32 || APPLE */
