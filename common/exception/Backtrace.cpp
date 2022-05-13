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

#include <execinfo.h>
#include <cxxabi.h>
#include <stdlib.h>
#include "Backtrace.hpp"

#ifdef COLLECTEXTRABACKTRACEINFOS
#include <bfd.h>
#include <sstream>
namespace cta {
  namespace exception {
    class bfdContext {
    public:
      bfdContext();
      ~bfdContext();
      std::string collectExtraInfos(const std::string &address);
    private:
      class mutex {
      public:
        mutex() { pthread_mutex_init(&m_mutex, nullptr); }
        void lock() { pthread_mutex_lock(&m_mutex); }
        void unlock() { pthread_mutex_unlock(&m_mutex); }
      private:
        pthread_mutex_t m_mutex;
      };
      mutex m_mutex;
      bfd* m_abfd;
      asymbol **m_syms;
      asection *m_text;
    };
  }
}

// code dedicated to extracting more information in the backtraces, typically
// resolving line numbers from adresses
// This code is compiled only in debug mode (where COLLECTEXTRABACKTRACEINFOS will be defined)
// as it's pretty heavy
cta::exception::bfdContext::bfdContext():
m_abfd(nullptr), m_syms(nullptr), m_text(nullptr)
{
  char ename[1024];
  int l = readlink("/proc/self/exe",ename,sizeof(ename));
  if (l != -1) {
    ename[l] = 0;
    bfd_init();
    m_abfd = bfd_openr(ename, 0);
    if (m_abfd) {
      /* oddly, this is required for it to work... */
      bfd_check_format(m_abfd,bfd_object);
      unsigned storage_needed = bfd_get_symtab_upper_bound(m_abfd);
      m_syms = (asymbol **) malloc(storage_needed);
      bfd_canonicalize_symtab(m_abfd, m_syms);
      m_text = bfd_get_section_by_name(m_abfd, ".text");
    }
  }
}

cta::exception::bfdContext::~bfdContext() {
  free (m_syms);
  /* According the bfd documentation, closing the bfd frees everything */
  m_text=nullptr;
  bfd_close(m_abfd);
}

std::string cta::exception::bfdContext::collectExtraInfos(const std::string& address) {
  std::ostringstream result;
  m_mutex.lock();
  if (m_abfd && m_text && m_syms) {
    std::stringstream ss;
    long offset;
    ss << std::hex << address;
    ss >> offset;
    offset -= m_text->vma;
    if (offset > 0) {
      const char *file;
      const char *func;
      unsigned line;
      if (bfd_find_nearest_line(m_abfd, m_text, m_syms, offset, &file, &func, &line)
          && file) {
        int status(-1);
        char * demangledFunc = abi::__cxa_demangle(func, nullptr, nullptr, &status);
        result << "at " << file << ":" << line << " (" << address << ")";
        free (demangledFunc);
      }
    }
  }
  m_mutex.unlock();
  return result.str();
}

namespace cta {
  namespace exception {
    bfdContext g_bfdContext;
  }
}
#endif // COLLECTEXTRABACKTRACEINFOS

cta::exception::Backtrace::Backtrace(bool fake): m_trace() {
  if (fake) return;
  void * array[200];
  g_lock.lock();
  size_t depth = ::backtrace(array, sizeof(array)/sizeof(void*));
  char ** strings = ::backtrace_symbols(array, depth);

  if (!strings)
    m_trace = "";
  else {
    for (size_t i=0; i<depth; i++) {
      std::string line(strings[i]);
      /* Demangle the c++, if possible. We expect the c++ function name's to live
       * between a '(' and a +
       * line format: /usr/lib/somelib.so.1(_Mangle2Mangle3Ev+0x123) [0x12345] */
      if ((std::string::npos != line.find("(")) && (std::string::npos != line.find("+"))) {
        std::string before, theFunc, after, addr;
        before = line.substr(0, line.find("(")+1);
        theFunc = line.substr(line.find("(")+1, line.find("+")-line.find("(")-1);
        after = line.substr(line.find("+"), line.find("[")-line.find("+")+1);
        addr = line.substr(line.find("[")+1, line.find("]")-line.find("[")-1);
        int status(-1);
        char * demangled = abi::__cxa_demangle(theFunc.c_str(), nullptr, nullptr, &status);
        if (0 == status) {
          m_trace += before;
          m_trace += demangled;
          m_trace += after;
#ifdef COLLECTEXTRABACKTRACEINFOS
          m_trace += g_bfdContext.collectExtraInfos(addr);
#else
          m_trace += addr;
#endif // COLLECTEXTRABACKTRACEINFOS
          m_trace += "]";
        } else {
          m_trace += strings[i];
        }
        m_trace += "\n";
        free(demangled);
      } else {
        m_trace += strings[i];
        m_trace += "\n";
      }
    }
    free (strings);
  }
  g_lock.unlock();
}

/* Implementation of the singleton lock */
cta::exception::Backtrace::mutex cta::exception::Backtrace::g_lock;
