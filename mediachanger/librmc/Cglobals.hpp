/*
 * SPDX-FileCopyrightText: 1999 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

void Cglobals_init(int (*)(int*, void**), int (*)(int*, void*), int (*)(void));
int Cglobals_get(int*, void**, size_t size);
void Cglobals_getTid(int*);
