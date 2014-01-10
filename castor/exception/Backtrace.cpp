/******************************************************************************
 *                      Backtrace.cpp
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
 * @author Eric Cano
 *****************************************************************************/

#include <execinfo.h>
#include <cxxabi.h>
#include <stdlib.h>
#include "Backtrace.hpp"

#ifdef COLLECTEXTRABACKTRACEINFOS
// Forward declaration for extracting more information in the backtraces 
namespace castor {
  namespace exception {
    std::string collectExtraInfos(const std::string &address);
  }
}
#endif // COLLECTEXTRABACKTRACEINFOS

castor::exception::Backtrace::Backtrace(): m_trace() {
  void * array[200];
  g_lock.lock();  
  size_t depth = ::backtrace(array, sizeof(array)/sizeof(void*));
  g_lock.unlock();
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
        char * demangled = abi::__cxa_demangle(theFunc.c_str(), NULL, NULL, &status);
        if (0 == status) {
          m_trace += before;
          m_trace += demangled;
          m_trace += after;
#ifdef COLLECTEXTRABACKTRACEINFOS
          m_trace += collectExtraInfos(addr);
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
}

/* Implementation of the singleton lock */
castor::exception::Backtrace::mutex castor::exception::Backtrace::g_lock;

#ifdef COLLECTEXTRABACKTRACEINFOS
/// code dedicated to extracting more information in the backtraces, typically
/// resolving line numbers from adresses
/// This code is compiled only in debug mode (where COLLECTEXTRABACKTRACEINFOS will be defined)
/// as it's pretty heavy

#include <bfd.h>
#include <sstream>
namespace castor {
  namespace exception {
    /// code dedicated to collecting extra infos on the backtrace (typically line numbers)
    /// compiled only in debug mode (where RESOLVELINENUMBERS will be defined)
    /// as it's pretty heavy
    static bfd* s_abfd = 0;
    static asymbol **s_syms = 0;
    static asection *s_text = 0;
    static pthread_mutex_t s_mutex;
    static bool s_mutex_is_initialized = false;

    std::string collectExtraInfos(const std::string &address) {
      std::ostringstream result;
      // if mutex not yet initilize, initialize it
      if (!s_mutex_is_initialized) {
        pthread_mutex_init(&s_mutex, NULL);
        s_mutex_is_initialized = true;
      }
      pthread_mutex_lock(&s_mutex);
      // if debug symbols were not yet analyzed, let's do it
      if (!s_abfd) {
        char ename[1024];
        int l = readlink("/proc/self/exe",ename,sizeof(ename));
        if (l != -1) {
          ename[l] = 0;
          bfd_init();
          s_abfd = bfd_openr(ename, 0);
          if (s_abfd) {
            /* oddly, this is required for it to work... */
            bfd_check_format(s_abfd,bfd_object);
            unsigned storage_needed = bfd_get_symtab_upper_bound(s_abfd);
            s_syms = (asymbol **) malloc(storage_needed);
            bfd_canonicalize_symtab(s_abfd, s_syms);
            s_text = bfd_get_section_by_name(s_abfd, ".text");
          }
        }
      }
      if (s_abfd && s_text && s_syms) {
        std::stringstream ss;
        long offset;
        ss << std::hex << address;
        ss >> offset;
        offset -= s_text->vma;
        if (offset > 0) {
          const char *file;
          const char *func;
          unsigned line;
          if (bfd_find_nearest_line(s_abfd, s_text, s_syms, offset, &file, &func, &line) && file) {
            int status(-1);
            char * demangledFunc = abi::__cxa_demangle(func, NULL, NULL, &status);
            result << "line " << line << " of file " << file
                   << " in " << (status?func:demangledFunc) << " (" << address << ")";
          }
        }
      }
      pthread_mutex_unlock(&s_mutex);
      return result.str();
    }
  }
}
#endif // COLLECTEXTRABACKTRACEINFOS
