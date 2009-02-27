/******************************************************************************
 *                      castor/tape/fsm/AbstractCallback.hpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_FSM_ABSTRACTCALLBACK
#define CASTOR_TAPE_FSM_ABSTRACTCALLBACK

namespace castor {
namespace tape   {
namespace fsm    {

  /**
   * Provides the specification for a callback functor to be used to invoke
   * the actions of a finite state machine.
   */
  class AbstractCallback {
  public:

    /**
     * The operator() function where the callback will be implemented by a
     * sub-class of AbstractCallback.
     *
     * Please note the callback should return an event in the case of an
     * automatic state transition.  If there is not be no automatic state
     * transition then the callback should return NULL.
     */
    virtual const char *operator()() const = 0;

    /**
     * Destructor.
     */
    virtual ~AbstractCallback() {
    }
  };

} // namespace fsm
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_FSM_ABSTRACTCALLBACK
