/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
  SubProcess(const std::string& program, const std::list<std::string>& argv, const std::string& str = "");
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
  bool m_childComplete = false;
  int m_childStatus = 0;
  std::string m_stdout;
  std::string m_stderr;
};

} // namespace cta::threading
