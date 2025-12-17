/*
 * SPDX-FileCopyrightText: 1999 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <netdb.h>

//! Maximum length for a hostname
constexpr int CA_MAXHOSTNAMELEN = 63;

struct hostent* Cgethostbyname(const char*);
struct hostent* Cgethostbyaddr(const void*, size_t, int);
struct servent* Cgetservbyname(const char*, const char*);
