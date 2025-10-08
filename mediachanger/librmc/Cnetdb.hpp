/*
 * SPDX-FileCopyrightText: 1999 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "osdep.hpp"
#include <netdb.h>

struct hostent* Cgethostbyname(const char*);
struct hostent* Cgethostbyaddr(const void*, size_t, int);
struct servent* Cgetservbyname(const char*, const char*);
