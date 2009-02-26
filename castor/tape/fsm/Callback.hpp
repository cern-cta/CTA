/******************************************************************************
 *                      castor/tape/fsm/Callback.hpp
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

#ifndef CASTOR_TAPE_FSM_CALLBACK
#define CASTOR_TAPE_FSM_CALLBACK

namespace castor {
namespace tape   {
namespace fsm    {

  /**
   * A concrete callback template functor to be used to invoke the actions of a
   * finite state machine.
   */
  template<class T> class Callback {
  public:

    typedef const char*(T::*PointerToMemberFunction)();

    /**
     * Constructor.
     */
    Callback(T &object, PointerToMemberFunction pointerToMemberFunction):
      m_object(object), m_pointerToMemberFunction(pointerToMemberFunction) {
    }

    /**
     * The operator() function where the callback is implemented.
     */
    const char *operator()() {
      return (m_object.*m_pointerToMemberFunction)();
    }

    /**
     * Destructor.
     */
    virtual ~Callback() {
    }


  private:

    /**
     * The object to be called back.
     */
    T &m_object;

    /**
     * Pointer to the function to be invoked on the object to be called back.
     */
    PointerToMemberFunction m_pointerToMemberFunction;
  };

} // namespace fsm
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_FSM_CALLBACK
