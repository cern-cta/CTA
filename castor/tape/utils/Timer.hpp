/******************************************************************************
 *                      castor/tape/utils/Timer.hpp
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



#ifndef _CASTOR_TAPE_UTILS_TIMER_HPP_
#define _CASTOR_TAPE_UTILS_TIMER_HPP_

#include <sys/time.h>
#include "osdep.h"

namespace castor {
namespace tape {
namespace utils {

class Timer {
public:
  Timer();
  virtual ~Timer() {}
  signed64 getusecs();
  void reset();
private:
  timeval m_reference;
};

} // namespace utils
} // namespace tape
} // namespace castor

#endif /* _CASTOR_TAPE_UTILS_TIMER_HPP_ */
