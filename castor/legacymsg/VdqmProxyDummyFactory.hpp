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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/legacymsg/VdqmProxyFactory.hpp"

namespace castor {
namespace legacymsg {

/**
 * Concrete factory for creating objects of type VdqmProxyDummy.
 */
class VdqmProxyDummyFactory: public VdqmProxyFactory {
public:
  /**
   * Constructor.
   *
   * @param job The vdqm job to be passed to the constructor of the
   * VdqmProxyDummy objects created by this factory.
   */
  VdqmProxyDummyFactory(const RtcpJobRqstMsgBody &job) throw();

  /**
   * Destructor.
   */
  ~VdqmProxyDummyFactory() throw();

  /**
   * Creates an object of type VdqmProxyDummy on the heap and returns a pointer
   * to it.
   *
   * Please note that it is the responsibility of the caller to deallocate the
   * proxy object from the heap.
   *
   * @return A pointer to the newly created object.
   */
  VdqmProxy *create();

private:

  /**
   * The vdqm job to be passed to the constructor of the
   * VdqmProxyDummy objects created by this factory.
   */
  const RtcpJobRqstMsgBody &m_job;

}; // class VdqmProxyDummyFactory

} // namespace legacymsg
} // namespace castor

