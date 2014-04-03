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

#pragma once

#include <stdint.h>


/**
 * Prints to standard out the string form of the specified IP address and port.
 *
 * @param ip the IP address in host byte order.
 * @param port the IP port number in host byte order.
 */
void vdqmCDevToolsPrintIpAndPort(const uint32_t ip, const int port);

/**
 * Returns the string form of the specified CASTOR-message magic-number.
 *
 * @param magic the CASTOR-message magic-number.
 */
const char *castorMagicNb2Str(const uint32_t magic);

/**
 * Returns the string form of the specified RTCP-message request-type.
 *
 * @param type the RTCP-message request-type.
 */
const char *rtcpReqTypeToStr(const uint32_t type);

/**
 * Returns the string form of the specified VDQM-message request-type.
 *
 * @param type the VDQM-message request-type.
 */
const char *vdqmReqTypeToStr(const uint32_t type);

/**
 * Returns the string form of the specified CASTOR-message request-type.
 *
 * @param type the CASTOR-message request-type.
 */
const char *castorReqTypeToStr(const uint32_t magic, const uint32_t type);

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
void printCastorMessage(const int messageWasSent,
  const int messageInNetworkByteOrder, const int socket, void* hdrbuf);

