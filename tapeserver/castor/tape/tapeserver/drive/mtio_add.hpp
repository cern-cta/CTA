/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
/*
* Define the write immediate file mark operation code in case it is absent from mtio.h
*/

#pragma once

/* in case MTWEOFI is not defined in linux/mtio.h */
#ifndef MTWEOFI
#define MTWEOFI 35
#endif

