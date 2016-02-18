/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <list>
#include <string>

namespace systemTests {
class Subprocess {
public:
  Subprocess(const std::string & program, const std::list<std::string> &argv);
  ~Subprocess();
  void wait(void);
  std::string stdout();
  std::string stderr();
  void kill(int signal);
  int exitValue();
private:
  int m_stdoutFd;
  int m_stderrFd;
  pid_t m_child;
  bool m_childComplete;
  int m_childStatus;
  std::string m_stdout;
  std::string m_stderr;
};
}