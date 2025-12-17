/*
 * SPDX-FileCopyrightText: 1990 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <sys/types.h>

constexpr int SEBASEOFF = 1000;  //!< Base offset for special err

enum Serrno {
  SENOERR = SEBASEOFF,               //!< No error
  SENOSHOST = SEBASEOFF + 1,         //!< Host not known
  SENOSSERV = SEBASEOFF + 2,         //!< Service unknown
  SENOTRFILE = SEBASEOFF + 3,        //!< Not a remote file
  SETIMEDOUT = SEBASEOFF + 4,        //!< Has timed out
  SEBADFFORM = SEBASEOFF + 5,        //!< Bad fortran format specifier
  SEBADFOPT = SEBASEOFF + 6,         //!< Bad fortran option specifier
  SEINCFOPT = SEBASEOFF + 7,         //!< Incompatible fortran options
  SENAMETOOLONG = SEBASEOFF + 8,     //!< File name too long
  SENOCONFIG = SEBASEOFF + 9,        //!< Can't open configuration file
  SEBADVERSION = SEBASEOFF + 10,     //!< Version ID mismatch
  SEUBUF2SMALL = SEBASEOFF + 11,     //!< User buffer too small
  SEMSGINVRNO = SEBASEOFF + 12,      //!< Invalid reply number
  SEUMSG2LONG = SEBASEOFF + 13,      //!< User message too long
  SEENTRYNFND = SEBASEOFF + 14,      //!< Entry not found
  SEINTERNAL = SEBASEOFF + 15,       //!< Internal error
  SECONNDROP = SEBASEOFF + 16,       //!< Connection closed by rem. end
  SEBADIFNAM = SEBASEOFF + 17,       //!< Can't get interface name
  SECOMERR = SEBASEOFF + 18,         //!< Communication error
  SENOMAPDB = SEBASEOFF + 19,        //!< Can't open mapping database
  SENOMAPFND = SEBASEOFF + 20,       //!< No user mapping
  SERTYEXHAUST = SEBASEOFF + 21,     //!< Retry count exhausted
  SEOPNOTSUP = SEBASEOFF + 22,       //!< Operation not supported
  SEWOULDBLOCK = SEBASEOFF + 23,     //!< Resource temporarily unavailable
  SEINPROGRESS = SEBASEOFF + 24,     //!< Operation now in progress
  SECTHREADINIT = SEBASEOFF + 25,    //!< Cthread initialization error
  SECTHREADERR = SEBASEOFF + 26,     //!< Thread interface call error
  SESYSERR = SEBASEOFF + 27,         //!< System error
  SENOTADMIN = SEBASEOFF + 32,       //!< requestor is not administrator
  SEUSERUNKN = SEBASEOFF + 33,       //!< User unknown
  SEDUPKEY = SEBASEOFF + 34,         //!< Duplicate key value
  SEENTRYEXISTS = SEBASEOFF + 35,    //!< Entry already exists
  SEGROUPUNKN = SEBASEOFF + 36,      //!< Group Unknown
  SECHECKSUM = SEBASEOFF + 37,       //!< Bad checksum
  SESVCCLASSNFND = SEBASEOFF + 38,   //!< This service class is not available for this host
  SESQLERR = SEBASEOFF + 39,         //!< Got SQL exception from database
  SELOOP = SEBASEOFF + 40,           //!< Too many symbolic links
  SENOPORTINRANGE = SEBASEOFF + 41,  //!< No port in range
  SENOVALUE = SEBASEOFF + 42,        //!< No value
  SEINVALIDCONFIG = SEBASEOFF + 43,  //!< Invalid configuration
  SEPYTHONEXEC = SEBASEOFF + 44,     //!< Failed to execute Python
  SEMISSINGOPER = SEBASEOFF + 45,    //!< Missing operand
  SEMISMATCH = SEBASEOFF + 46,       //!< Mismatch
  SEREQUESTFAILED = SEBASEOFF + 47,  //!< Request failed
  SEINVALIDNBARGS = SEBASEOFF + 48,  //!< Invalid number of arguments
  SEALREADYINIT = SEBASEOFF + 49,    //!< Already initialized
  SECMDLNNOTPRSD = SEBASEOFF + 50,   //!< Command line not parsed
  SEACCPTCONNINTR = SEBASEOFF + 51,  //!< Accept connection was interrupted
  SEBADALLOC = SEBASEOFF + 52,       //!< Failed to allocate memory
  SENOTANOWNER = SEBASEOFF + 53,     //!< Not an owner
  SEMAXERR = SEBASEOFF + 53          //!< Maximum error number
};

/**
 * RMC (Remote SCSI media changer server) errors
 */
constexpr int ERMBASEOFF = 2200;  //!< RMC error base offset

enum RMCError {
  ERMCNACT = ERMBASEOFF + 1,    //!< Remote SCSI media changer server not active or service being drained
  ERMCRBTERR = ERMBASEOFF + 2,  //!< Remote SCSI media changer error
  ERMCUNREC = ERMCRBTERR + 1,   //!< Remote SCSI media changer unrec. error
  ERMCSLOWR = ERMCRBTERR + 2,   //!< Remote SCSI media changer error (slow retry)
  ERMCFASTR = ERMCRBTERR + 3,   //!< Remote SCSI media changer error (fast retry)
  ERMCDFORCE = ERMCRBTERR + 4,  //!< Remote SCSI media changer error (demount force)
  ERMCDDOWN = ERMCRBTERR + 5,   //!< Remote SCSI media changer error (drive down)
  ERMCOMSGN = ERMCRBTERR + 6,   //!< Remote SCSI media changer error (ops message)
  ERMCOMSGS = ERMCRBTERR + 7,   //!< Remote SCSI media changer error (ops message + retry)
  ERMCOMSGR = ERMCRBTERR + 8,   //!< Remote SCSI media changer error (ops message + wait)
  ERMCUNLOAD = ERMCRBTERR + 9,  //!< Remote SCSI media changer error (unload + demount)
  ERMMAXERR = ERMBASEOFF + 11
};

/*
 * Multi-thread (MT) environment
 */
int* C__serrno(void);

/*
 * Thread safe serrno. Note, C__serrno is defined in Cglobals.c rather
 * rather than serror.c .
 */
#define serrno (*C__serrno())

/**
 * Writes the string representation of the specified error code into the
 * specified buffer.
 *
 * The string representation is truncated if the specified buffer is too small.
 * In case of such a truncation the string written to the specified buffer is
 * null terminated.
 *
 * @param n The numeric error code.
 * @param buf The buffer to which the string representation should be written.
 * @param buflen The length of the buffer.
 * @return 0 on success or -1 on failure.  In the case of a failure both errno
 * and serrno are set to indicate the type of error.
 */
int sstrerror_r(const int n, char* const buf, const size_t buflen);

/**
 * Returns the string representation of the specified error code.
 *
 * Allocates some thread specfic memory if needed using Cglobals_get() and then
 * passes that memory to sstrerror_r along with the specified error code.
 *
 * A string is always returned.  This function never returns NULL.
 *
 * @param n The numeric error code.
 * @return The string representation.
 */
const char* sstrerror(const int n);

void sperror(char*);

extern const char* sys_serrlist[];  //!< Error text array
