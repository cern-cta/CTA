/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <list>
#include <string>

namespace cta::threading {

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
} // namespace cta::threading
