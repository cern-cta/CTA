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
     *
     * @param state The state.
     */
    void setState(const char *const state);

    /**
     * Returns the state of the state machine or NULL if the state has not been
     * set.
     *
     * @return The state of the state machine.
     */
    const char *getState();

    /**
     * Sets the transitions that define the dynamic behaviour of the state
     * machine.
     *
     * Please note the transitions are represented as an array of Transition,
     * where the length of the array is determined by the mandatory presence
     * of a terminating sentinal Transition element.  A terminating sentinal
     * Transition is a Transition that has a NULL fromState member.
     *
     * Example code for setting the transitions of a state machine:
     * <code>
     * //---------------------------------------------
     * // Define and set the state machine transitions
     * //---------------------------------------------
     * 
     * typedef fsm::Callback<FsmTest> Callback;
     * Callback acceptRtcpdConn      (*this, &FsmTest::acceptRtcpdConn      );
     * Callback getReqFromRtcpd      (*this, &FsmTest::getReqFromRtcpd      );
     * Callback getVolFromRtcpd      (*this, &FsmTest::getVolFromRtcpd      );
     * Callback ping                 (*this, &FsmTest::ping                 );
     * Callback error                (*this, &FsmTest::error                );
     * Callback closeRtcpdConAndError(*this, &FsmTest::closeRtcpdConAndError);
     * 
     * fsm::Transition transitions[] = {
     * // from state       , to state        , event , action
     *   {"INIT"           , "WAIT_RTCPD_CON", "INIT", &acceptRtcpdConn      },
     *   {"WAIT_RTCPD_CON" , "FAILED"        , "ERR" , &error                },
     *   {"WAIT_RTCPD_CON" , "WAIT_RTCPD_REQ", "CON" , &getReqFromRtcpd      },
     *   {"WAIT_RTCPD_REQ" , "WAIT_TGATE_VOL", "REQ" , &getVolFromRtcpd      },
     *   {"WAIT_TGATE_VOL" , "END"           , "VOL" , NULL                  },
     *   {"RTCPD_CONNECTED", "FAILED"        , "ERR" , &closeRtcpdConAndError},
     *   {NULL             , NULL            , NULL  , NULL                  }};
     * 
     * m_fsm.setTransitions(transitions);
     * </code>
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
     * Example code for setting the state hierarchy of a state machine:
     * <code>
     * //------------------------
     * // Set the state hierarchy
     * //------------------------
     * 
     * fsm::ChildToParentState hierarchy[] = {
     * // child           , parent
     *   {"WAIT_RTCPD_REQ", "RTCPD_CONNECTED"},
     *   {"WAIT_TGATE_VOL", "RTCPD_CONNECTED"},
     *   {NULL            , NULL             }
     * };
     * 
     * m_fsm.setStateHierarchy(hierarchy);
     * </code>
     *
     * @param stateHierarchy The state machine state hierarchy.
     */
    void setStateHierarchy(const ChildToParentState *const stateHierarchy);

    /**
     * Dispatches the specified event and returns an internal event if one has
     * been generated.
     *
     * The dispatch method should normally be called in a loop feeding any
     * internally generated events back into the method until no internal
     * event is generated.  Running the dispatch method in a loop allows the
     * calling code to monitor the state machine's progress including automatic
     * state transitions which are the origin of internally generated events.
     *
     * Example code for calling dispatch:
     * <code>
     * //----------------------------------------------------------
     * // Execute the state machine with debugging print statements
     * //----------------------------------------------------------
     * 
     * const char *event = "INIT";
     * 
     * // While no more automatic state transitions
     * while(event != NULL) {
     * 
     *   std::cout << "From state        = " << m_fsm.getState() << std::endl;
     *   std::cout << "Dispatching event = " << event            << std::endl;
     * 
     *   event = m_fsm.dispatch(event);
     * 
     *   if(event != NULL) {
     *   std::cout << "Internally generated event for an automatic state "
     *     "transition" << std::endl;
     *   }
     * }
     * </code>
     *
     * @param event The event to be dispatched.
     * @return An internally generated event if one has been generated by an
     * automatic state change.
     */
    const char* dispatch(const char *event)
      throw(castor::exception::Exception);


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
