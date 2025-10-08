/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

// This file should be included before the relevant c++ headers. It will introduce proper helgrind annotations
// in all templated code (like shared pointers)
// See https://gcc.gnu.org/onlinedocs/libstdc++/manual/debug.html (Data race hunting)

#include <valgrind/helgrind.h>
#undef _GLIBCXX_SYNCHRONIZATION_HAPPENS_BEFORE
#define _GLIBCXX_SYNCHRONIZATION_HAPPENS_BEFORE(A) ANNOTATE_HAPPENS_BEFORE(A)
#undef _GLIBCXX_SYNCHRONIZATION_HAPPENS_AFTER
#define _GLIBCXX_SYNCHRONIZATION_HAPPENS_AFTER(A)  ANNOTATE_HAPPENS_AFTER(A)
#undef _GLIBCXX_EXTERN_TEMPLATE
#define _GLIBCXX_EXTERN_TEMPLATE -1


