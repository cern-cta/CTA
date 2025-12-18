/*
 * SPDX-FileCopyrightText: 2003 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <unistd.h>

int netread(int, char*, int);                     //!< Network receive function
int netwrite(const int, const char*, const int);  //!< Network send function
const char* neterror();                           //!< Network error function

ssize_t netread_timeout(int, void*, ssize_t, int);
ssize_t netwrite_timeout(int, void*, ssize_t, int);
int netconnect_timeout(int, struct sockaddr*, size_t, int);
