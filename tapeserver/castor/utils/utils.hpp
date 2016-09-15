/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "common/exception/InvalidArgument.hpp"
#include "common/exception/InvalidConfigEntry.hpp"
#include "common/exception/Exception.hpp"

#include <ostream>
#include <string>
#include <sstream>
#include <sys/time.h>
#include <time.h>
#include <vector>

namespace castor {
namespace utils  {

/**
 * Safely copies source string into destination string.  The destination
 * will always be null terminated if this function is successful.
 *
 * @param dst     Destination string.
 * @param dstSize The size of the destination string including the terminating
 *                null character.
 * @param src     Source string.
 * destination.
 */
void copyString(char *const dst, const size_t dstSize, const std::string &src);

/**
 * Safely copies source string into destination string.  The destination
 * will always be null terminated if this function is successful.
 *
 * @param dst Destination string.
 * @param src Source string.
 */
template<size_t dstSize> void copyString(char (&dst)[dstSize],
  const std::string &src) {
  copyString(dst, dstSize, src);
}

/**
 * Sets both the process name and the command-line to the specified value.
 *
 * The command-line is set by modifiying argv[0] and the process name is
 * modified using prctl(PR_SET_NAME, ...).  Please note that the length of
 * argv[0] cannot be changed and that prctl(PR_SET_NAME, ...) is limited to
 * 15 characters plus the null terminator.
 *
 * @param argv0 In/out paramater: A pointer to argv[0].
 * @param name The new name of the process.  If the name is too long for either
 * the command-line or the process name then it will truncated.  Any truncation
 * for argv[0] will be independent of any truncation for process name if argv[0]
 * is not 15 characters in length.
 */
void setProcessNameAndCmdLine(char *const argv0, const std::string &name);

/**
 * Sets the process name to the specified value.
 *
 * The process name is modified using prctl(PR_SET_NAME, ...).  Please note that
 * prctl(PR_SET_NAME, ...) is limited to 15 characters plus the null terminator.
 *
 * @param name The new name of the process.  If the new name is longer than 15
 * characters then it will be truncated.
 */
void setProcessName(const std::string &name);

/**
 * Sets both the process name and the command-line to the specified value.
 *
 * The command-line is set by modifiying argv[0] and the process name is
 * modified using prctl(PR_SET_NAME, ...).  Please note that the length of
 * argv[0] cannot be changed and that prctl(PR_SET_NAME, ...) is limited to
 * 15 characters plus the null terminator.
 *
 * @param argv0 In/out paramater: A pointer to argv[0].
 * @param cmdLine The new command-line.  If the new command-line is longer than
 * argv[0] then it will be truncated.
 */
void setCmdLine(char *const argv0, const std::string &cmdLine) throw();

} // namespace utils
} // namespace castor
