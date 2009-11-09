/******************************************************************************
 *                      StateMachine.cpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/fsm/StateMachine.hpp"
#include <string.h>

#include <errno.h>
#include <stdlib.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::fsm::StateMachine::StateMachine() throw() : m_state(NULL),
  m_transitions(NULL), m_stateHierarchy(NULL) {
}


//-----------------------------------------------------------------------------
// setState
//-----------------------------------------------------------------------------
void castor::tape::fsm::StateMachine::setState(const char *const state) {
  m_state = state;
}


//-----------------------------------------------------------------------------
// getState
//-----------------------------------------------------------------------------
const char *castor::tape::fsm::StateMachine::getState() {
  return(m_state);
}


//-----------------------------------------------------------------------------
// setTransitions
//-----------------------------------------------------------------------------
void castor::tape::fsm::StateMachine::setTransitions(
  const Transition *const transitions) {
  m_transitions = transitions;
}


//-----------------------------------------------------------------------------
// setStateHierarchy
//-----------------------------------------------------------------------------
void castor::tape::fsm::StateMachine::setStateHierarchy(
  const ChildToParentState *const stateHierarchy) {
  m_stateHierarchy = stateHierarchy;
}


//-----------------------------------------------------------------------------
// dispatch
//-----------------------------------------------------------------------------
const char *castor::tape::fsm::StateMachine::dispatch(const char *event)
  throw(castor::exception::Exception) {

  // Check the state has been set
  if(m_state == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": State not set when dispatch() called";
    throw ex;
  }

  const Transition *const transition = findTransitionInHierarchy(event);

  // Throw an exception if there is no transition for the event
  if(transition == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": No transition found "
         ": State = " << m_state
      << ": Event = " << event;
    throw ex;
  }

  // Move to the next state if not a self transition
  if(strcmp(transition->fromState, transition->toState) != 0) {
    m_state = transition->toState;
  }

  // Get ready to detect an internally generated event from an automatic
  // state transition if there is one
  event = NULL;

  // If there is an action
  if(transition->action != NULL) {

    // Invoke it which may generate an event
    event = (*transition->action)();
  }

  return event;
}


//-----------------------------------------------------------------------------
// findTransition
//-----------------------------------------------------------------------------
const castor::tape::fsm::Transition
  *castor::tape::fsm::StateMachine::findTransition(const char *const state,
  const char *const event) throw(castor::exception::Exception) {

  // Check the transitions have been set
  if(m_transitions == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Transitions not set when dispatch() called";
    throw ex;
  }

  // For each transition
  // Terminating sentinal Transition has a NULL fromState member
  for(int i=0; m_transitions[i].fromState != NULL; i++) {
    // If the transition has been found, then return a pointer to it
    if(strcmp(m_transitions[i].fromState, state) == 0  &&
       strcmp(m_transitions[i].event, event) == 0) {
       return &m_transitions[i];
    }
  }

  // Transition not found
  return NULL;
}


//-----------------------------------------------------------------------------
// findParentState
//-----------------------------------------------------------------------------
const char *castor::tape::fsm::StateMachine::findParentState(
  const char *const state) throw(castor::exception::Exception) {

  // Check the state heirarchy has been set
  if(m_stateHierarchy == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": State hierarchy not set when dispatch() called";
    throw ex;
  }

  // For each child/parent maplet
  // Terminating sentinal maplet has a NULL child member
  for(int i=0; m_stateHierarchy[i].child != NULL; i++) {

    // If the parent has been found, then return a pointer to it
    if(strcmp(m_stateHierarchy[i].child, state) == 0) {
      return m_stateHierarchy[i].parent;
    }
  }

  // Parent state not found
  return NULL;
}


//-----------------------------------------------------------------------------
// findTransitionInHierarchy
//-----------------------------------------------------------------------------
const castor::tape::fsm::Transition
  *castor::tape::fsm::StateMachine::findTransitionInHierarchy(
  const char *const event) throw(castor::exception::Exception) {

  const Transition *transition = NULL;

  // Start within the state hiearchy at the current state
  const char *state = m_state;

  // While a transition has not been found and the state is within the state
  // hierarchy
  while(transition == NULL && state != NULL) {

    transition = findTransition(state, event);

    if(transition != NULL) {
      return transition;
    }

    state = findParentState(state);
  }

  // Transition not found
  return NULL;
}
