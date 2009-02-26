/******************************************************************************
 *                      castor/tape/fsm/Transition.hpp
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

#ifndef CASTOR_TAPE_FSM
#define CASTOR_TAPE_FSM

#include "castor/tape/fsm/AbstractCallback.hpp"


namespace castor {
namespace tape   {
namespace fsm    {

  /**
   * A state transition which specifies for a given "from state" and event the
   * "to state" to be transitioned to and the action to be performed whilst in
   * "to state".  The action to be performed is optional.  A value of NULL
   * specifies no action.
   *
   * @param fromState The "from state".
   * @param toState   The "to state".
   * @param event     The event.
   * @param action    A callback functor to be used to invoke the action which
   *                  is to be performed whilst in the "to state".
   */
  struct Transition {
    const char             *const fromState;
    const char             *const event;
    const char             *const toState;
    const AbstractCallback *const action;
  };

} // namespace fsm
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_FSM
