/******************************************************************************
 *                      DevTools.h
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
 * @author Castor dev team
 *****************************************************************************/

#ifndef CASTOR_VDQM_CDEVTOOLS_H
#define CASTOR_VDQM_CDEVTOOLS_H 1


#include <stdint.h>

/**
 * Prints to standard out the specified IP address and port.
 *
 * @param ip the IP address in host byte order.
 * @param port the IP port in host byte order.
 */
void printIpAndPort(const uint32_t ip, const int port);

/**
 * Prints to standard out the magic number, request type, and IP information of
 * the specified message.
 *
 * @param messageWasSent should be set to true if the message was sent, or to
 * false the message was received.
 * @param messageInNetworkByteOrder should be set to true if the contents of
 * the message is in network byte order, or to false if the contents is in host
 * byte order.
 * @param socket the socket used to send or receive the message.
 * @param hdrbuf the header buffer.
 */
void printMessage(const int messageWasSent, const int messageInNetworkByteOrder,
  const int socket, void* hdrbuf);

#endif // CASTOR_VDQM_CDEVTOOLS_H
