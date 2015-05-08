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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include <string>
#include <pthread.h>

namespace cta {
  namespace exception {
    class Backtrace {
    public:
      Backtrace(bool fake=false);
      operator std::string() const { return m_trace; }
      Backtrace& operator= (const Backtrace& bt) { m_trace = bt.m_trace; return *this; }
    private:
      std::string m_trace;
      /**
       * Singleton lock around the apparently racy backtrace().
       * We write it with no error check as it's used only here.
       * We need a class in order to have a constructor for the global object.
       */
      class mutex {
      public:
        mutex() { pthread_mutex_init(&m_mutex, NULL); }
        void lock() { pthread_mutex_lock(&m_mutex); }
        void unlock() { pthread_mutex_unlock(&m_mutex); }
      private:
        pthread_mutex_t m_mutex;    
      };
      static mutex g_lock;
    };
  }
}

