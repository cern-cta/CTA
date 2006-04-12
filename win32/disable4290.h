/*
 *  $Id: disable4290.h,v 1.1 2006/04/12 08:48:35 motiakov Exp $
 */

/*
 * CERN IT-ADC Vitaly Motyakov
 */

/*
 * Copyright (C) 1999-2006 by CERN/IT/ADC
 * All rights reserved
 */

#ifndef _DISABLE4290_WIN32_H
#define _DISABLE4290_WIN32_H

// This disables the Microsoft C++ compiler C4290 warning. 
// According to Microsoft:
//
// Compiler Warning C4290:
//
// C++ exception specification ignored except to indicate a function is
// not __declspec(nothrow)
//
// A function is declared using exception specification, which Visual C++
// accepts but does not implement. Code with exception specifications that
// are ignored during compilation may need to be recompiled and linked to be
// reused in future versions supporting exception specifications.
//


#pragma warning(disable : 4290)

#endif
