// ----------------------------------------------------------------------
// File: System/realWrapper.hh
// Author: Eric Cano - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include <sys/types.h>
#include <dirent.h>

#include <gmock/gmock.h>

namespace Tape {
namespace System {
  /**
   * Wrapper class the all system calls used, allowing writing of test harnesses
   * for unit testing.
   * The member function are purposedly non-virtual, allowing full
   * performance with inline member functions.
   */
  class realWrapper{
  public:
    DIR* opendir(const char *name) { return ::opendir(name); }
    struct dirent * readdir(DIR* dirp) { return ::readdir(dirp); }
    int closedir(DIR* dirp) { return ::closedir(dirp); }
  };
  
  /**
   * Mock class for system wrapper, used to develop tests.
   */
  class mockWrapper {
  public:
    mockWrapper(): m_DIR((DIR*)&m_DIRfake) {
      ON_CALL(*this, opendir(::testing::_))
              .WillByDefault(::testing::Return(m_DIR));
    }
    MOCK_METHOD1(opendir, DIR*(const char *name));
    MOCK_METHOD1(readdir, struct dirent*(DIR* dirp));
    MOCK_METHOD1(closedir, int(DIR* dirp));
    int m_DIRfake;
    DIR* m_DIR;
  };
} // namespace System
} // namespace Tape
