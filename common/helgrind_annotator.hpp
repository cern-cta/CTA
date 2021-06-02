/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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


