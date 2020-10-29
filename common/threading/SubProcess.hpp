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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include <list>
#include <string>

namespace cta { namespace threading {
/**
 * A class spawning a new executable with a program path and arguments.
 * StdOut and StdErr can be recovered after execution (and calling wait()).
 */
class SubProcess {
public:
  SubProcess(const std::string & program, const std::list<std::string> &argv, const std::string & str = "");
  ~SubProcess();
  void wait(void);
  std::string stdout();
  std::string stderr();
  void kill(int signal);
  int exitValue();
  bool wasKilled();
  int killSignal();
private:
  int m_stdoutFd;
  int m_stderrFd;
  pid_t m_child;
  bool m_childComplete;
  int m_childStatus;
  std::string m_stdout;
  std::string m_stderr;
};
}}
