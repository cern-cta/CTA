/******************************************************************************
 *                      castor/tape/fsm/StateMachine.hpp
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

#ifndef CASTOR_TAPE_FSM_STATEMACHINE
#define CASTOR_TAPE_FSM_STATEMACHINE

#include "castor/exception/Exception.hpp"
#include "castor/tape/fsm/ChildToParentState.hpp"
#include "castor/tape/fsm/Transition.hpp"


namespace castor {
namespace tape   {
namespace fsm    {

  /**
   * A hierarchical finite state machine.
   */
  class StateMachine {
  public:

    /**
     * Constructor.
     */
    StateMachine() throw();

    /**
     * Sets the state of the state machine.
     *
     * Please note that this method must be called at least once before
     * dispatch() is called.
     */
    void setState(const char *const state);

    /**
     * Sets the transitions that define the dynamic behaviour of the state
     * machine.
     *
     * Please note the transitions are represented as an array of Transition,
     * where the length of the array is determined by the mandatory presence
     * of a terminating sentinal Transition element.  A terminating sentinal
     * Transition is a Transition that has a NULL fromState member.
     *
     * @param transitions The state machine transitions.
     */
    void setTransitions(const Transition *const transitions);

    /**
     * Sets the state heirarchy of the state machine.
     *
     * Please note the state hierarchy is represented as an array of
     * ChildToParentState, where the length of the array is determined by the
     * mandatory presence of a terminating sentinal ChildToParentState element.
     * A terminating sentinal ChildToParentState is a ChildToParentState that
     * has a NULL child member.
     *
     * @param stateHierarchy The state machine state hierarchy.
     */
    void setStateHierarchy(const ChildToParentState *const stateHierarchy);

    /**
     * Dispatches the specified event.
     *
     * @param event The event to be dispatched.
     */
    void dispatch(const char *event) throw(castor::exception::Exception);


  private:

    /**
     * The current state of the state machine.
     */
    const char *m_state;

    /**
     * The list of state transitions that define the dynamic behaviour of the
     * state machine.
     */
    const Transition *m_transitions;

    /**
     * The state hierarchy.
     */
    const ChildToParentState *m_stateHierarchy;

    /**
     * Searches the state transitions for the transition with specified
     * fromState and event.
     *
     * Please note that this function does not traverse the state hierarchy.
     *
     * @param state The state to be used when searching.
     * @param event The event to be used when searching.
     * @return A pointer to the transition if one is found, else NULL.
     */
    const Transition *findTransition(const char *const state,
      const char *const event) throw(castor::exception::Exception);

    /**
     * Searches the state hierarchy for the parent of the specified state.
     *
     * @param state The state whose parent is to be found.
     * @return A pointer to the parent state if one is found, else NULL.
     */
    const char *findParentState(const char *const state)
      throw(castor::exception::Exception);

    /**
     * Traverses the state heirarchy starting at the current state to find
     * the transition with the specified event.
     *
     * @param event The event triggering the transition.
     * @return A pointer to the transition if it is found, else NULL.
     */
    const Transition *findTransitionInHierarchy(const char *const event)
      throw(castor::exception::Exception);
  };

} // namespace fsm
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_FSM_STATEMACHINE
