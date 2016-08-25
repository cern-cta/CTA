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
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/messages/Constants.hpp"
#include "castor/messages/Exception.pb.h"
#include "castor/messages/ForkCleaner.pb.h"
#include "castor/messages/ForkDataTransfer.pb.h"
#include "castor/messages/ForkLabel.pb.h"
#include "castor/messages/ForkSucceeded.pb.h"
#include "castor/messages/ProcessCrashed.pb.h"
#include "castor/messages/ProcessExited.pb.h"
#include "castor/messages/ReturnValue.pb.h"
#include "castor/messages/StopProcessForker.pb.h"
#include "castor/log/Logger.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerFrame.hpp"

#include <google/protobuf/message.h>
#include <stdint.h>
#include <string>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Class containing code common to the ProcessForker class and the
 * ProcessForker proxy-classes.
 */
class ProcessForkerUtils {
public:

  /**
   * Serializes the specified message into the specified frame.
   *
   * Please note that this method sets both the type and payload fields of the
   * frame.
   *
   * @param frame Output parameter: The frame.
   * @param msg The message.
   */
  static void serializePayload(ProcessForkerFrame &frame,
    const messages::Exception &msg);

  /**
   * Serializes the specified message into the specified frame.
   *
   * Please note that this method sets both the type and payload fields of the
   * frame.
   *
   * @param frame Output parameter: The frame.
   * @param msg The message.
   */
  static void serializePayload(ProcessForkerFrame &frame,
    const messages::ForkCleaner &msg);
  
  /**
   * Serializes the specified message into the specified frame.
   *
   * Please note that this method sets both the type and payload fields of the
   * frame.
   *
   * @param frame Output parameter: The frame.
   * @param msg The message.
   */
  static void serializePayload(ProcessForkerFrame &frame,
    const messages::ForkDataTransfer &msg);

  /**
   * Serializes the specified message into the specified frame.
   *
   * Please note that this method sets both the type and payload fields of the
   * frame.
   *
   * @param frame Output parameter: The frame.
   * @param msg The message.
   */
  static void serializePayload(ProcessForkerFrame &frame,
    const messages::ForkLabel &msg);

  /**
   * Serializes the specified message into the specified frame.
   *
   * Please note that this method sets both the type and payload fields of the
   * frame.
   *
   * @param frame Output parameter: The frame.
   * @param msg The message.
   */
  static void serializePayload(ProcessForkerFrame &frame,
    const messages::ForkSucceeded &msg);

  /**
   * Serializes the specified message into the specified frame. 
   *  
   * Please note that this method sets both the type and payload fields of the
   * frame.
   *
   * @param frame Output parameter: The frame.
   * @param msg The message.
   */
  static void serializePayload(ProcessForkerFrame &frame,
    const messages::ProcessCrashed &msg);

  /**
   * Serializes the specified message into the specified frame. 
   *  
   * Please note that this method sets both the type and payload fields of the
   * frame.
   *
   * @param frame Output parameter: The frame.
   * @param msg The message.
   */
  static void serializePayload(ProcessForkerFrame &frame,
    const messages::ProcessExited &msg);

  /**
   * Serializes the specified message into the specified frame. 
   *  
   * Please note that this method sets both the type and payload fields of the
   * frame.
   *
   * @param frame Output parameter: The frame.
   * @param msg The message.
   */
  static void serializePayload(ProcessForkerFrame &frame,
    const messages::ReturnValue &msg);

  /**
   * Serializes the specified message into the specified frame.
   *
   * Please note that this method sets both the type and payload fields of the
   * frame.
   *
   * @param frame Output parameter: The frame.
   * @param msg The message.
   */
  static void serializePayload(ProcessForkerFrame &frame,
    const messages::StopProcessForker &msg);

  /**
   * Writes a frame with the specified message as its payload to the specified
   * file descriptor.
   *
   * @param fd The file descriptor to be written to.
   * @param msg The message to sent as the payload of the frame.
   * @param log Optional parameter: Object representing the API of the CASTOR
   * logging system.
   */
  static void writeFrame(const int fd, const messages::Exception &msg,
    log::Logger *const log = NULL);

  /**
   * Writes a frame with the specified message as its payload to the specified
   * file descriptor.
   *
   * @param fd The file descriptor to be written to.
   * @param msg The message to sent as the payload of the frame.
   * @param log Optional parameter: Object representing the API of the CASTOR
   * logging system.
   */
  static void writeFrame(const int fd, const messages::ForkCleaner &msg,
    log::Logger *const log = NULL);
  
  /**
   * Writes a frame with the specified message as its payload to the specified
   * file descriptor.
   *
   * @param fd The file descriptor to be written to.
   * @param msg The message to sent as the payload of the frame.
   * @param log Optional parameter: Object representing the API of the CASTOR
   * logging system.
   */
  static void writeFrame(const int fd, const messages::ForkDataTransfer &msg,
    log::Logger *const log = NULL);

  /**
   * Writes a frame with the specified message as its payload to the specified
   * file descriptor.
   *
   * @param fd The file descriptor to be written to.
   * @param msg The message to sent as the payload of the frame.
   * @param log Optional parameter: Object representing the API of the CASTOR
   * logging system.
   */
  static void writeFrame(const int fd, const messages::ForkLabel &msg,
    log::Logger *const log = NULL);

  /**
   * Writes a frame with the specified message as its payload to the specified
   * file descriptor.
   *
   * @param fd The file descriptor to be written to.
   * @param msg The message to sent as the payload of the frame.
   * @param log Optional parameter: Object representing the API of the CASTOR
   * logging system.
   */
  static void writeFrame(const int fd, const messages::ProcessCrashed &msg,
    log::Logger *const log = NULL);

  /**
   * Writes a frame with the specified message as its payload to the specified
   * file descriptor.
   *
   * @param fd The file descriptor to be written to.
   * @param msg The message to sent as the payload of the frame.
   * @param log Optional parameter: Object representing the API of the CASTOR
   * logging system.
   */
  static void writeFrame(const int fd, const messages::ProcessExited &msg,
    log::Logger *const log = NULL);

  /**
   * Writes a frame with the specified message as its payload to the specified
   * file descriptor.
   *
   * @param fd The file descriptor to be written to.
   * @param msg The message to sent as the payload of the frame.
   * @param log Optional parameter: Object representing the API of the CASTOR
   * logging system.
   */
  static void writeFrame(const int fd, const messages::StopProcessForker &msg,
    log::Logger *const log = NULL);

  /**
   * Writes the specified frame to the specified file descriptor.
   *
   * @param fd The file descriptor to be written to.
   * @param frame The frame.
   * @param log Optional parameter: Object representing the API of the CASTOR
   * logging system.
   */
  static void writeFrame(const int fd, const ProcessForkerFrame &frame,
    log::Logger *const log = NULL);

  /**
   * Reads either a good-day reply-message or an exception message from the
   * specified File descriptor.
   *
   * If a good-day reply-message is read from the file descriptor then it is
   * parsed into the specified Google protocol-buffer.
   *
   * If an exception message is read from the file descriptor then it is
   * converted into a C++ exception and thrown.
   *
   * @param fd The file descriptor to be read from.
   * @param timeout Timeout in seconds.
   * @param msg Output parameter: The good-day reply-message in the form of a
   * Google protocol-buffer.
   */
  template<typename T> static void readReplyOrEx(const int fd,
    const int timeout, T &msg) {
    const ProcessForkerFrame frame = readFrame(fd, timeout);

    // Throw an exception if the ProcessForker replied with one
    if(messages::MSG_TYPE_EXCEPTION == frame.type) {
      messages::Exception exMsg;
      if(!exMsg.ParseFromString(frame.payload)) {
        cta::exception::Exception ex;
        ex.getMessage() << "Failed to parse Exception message"
          ": ParseFromString() returned false";
        throw ex;
      }
      cta::exception::Exception ex;
      ex.getMessage() << exMsg.message();
      throw ex;
    }

    parsePayload(frame, msg);
  }

  /**
   * Reads a frame from the specified file descriptor.
   *
   * @param fd The file descriptor to be read from.
   * @param timeout Timeout in seconds.
   * @return The frame.
   */
  static ProcessForkerFrame readFrame(const int fd, const int timeout);

  /**
   * Parses the payload of the specified frame.
   *
   * @param frame The frame.
   * @param msg Output parameter: The message contained within the payload of
   * the frame.
   */
  static void parsePayload(const ProcessForkerFrame &frame,
    messages::Exception &msg);

  /**
   * Parses the payload of the specified frame.
   *
   * @param frame The frame.
   * @param msg Output parameter: The message contained within the payload of
   * the frame.
   */
  static void parsePayload(const ProcessForkerFrame &frame,
    messages::ForkCleaner &msg);
  
  /**
   * Parses the payload of the specified frame.
   *
   * @param frame The frame.
   * @param msg Output parameter: The message contained within the payload of
   * the frame.
   */
  static void parsePayload(const ProcessForkerFrame &frame,
    messages::ForkDataTransfer &msg);

  /**
   * Parses the payload of the specified frame.
   *
   * @param frame The frame.
   * @param msg Output parameter: The message contained within the payload of
   * the frame.
   */
  static void parsePayload(const ProcessForkerFrame &frame,
    messages::ForkLabel &msg);

  /**
   * Parses the payload of the specified frame.
   *
   * @param frame The frame.
   * @param msg Output parameter: The message contained within the payload of
   * the frame.
   */
  static void parsePayload(const ProcessForkerFrame &frame,
    messages::ForkSucceeded &msg);

  /**
   * Parses the payload of the specified frame.
   *
   * @param frame The frame.
   * @param msg Output parameter: The message contained within the payload of
   * the frame.
   */
  static void parsePayload(const ProcessForkerFrame &frame,
    messages::ProcessCrashed &msg);

  /**
   * Parses the payload of the specified frame.
   *
   * @param frame The frame.
   * @param msg Output parameter: The message contained within the payload of
   * the frame.
   */
  static void parsePayload(const ProcessForkerFrame &frame,
    messages::ProcessExited &msg);

  /**
   * Parses the payload of the specified frame.
   *
   * @param frame The frame.
   * @param msg Output parameter: The message contained within the payload of
   * the frame.
   */
  static void parsePayload(const ProcessForkerFrame &frame,
    messages::ReturnValue &msg);

  /**
   * Parses the payload of the specified frame.
   *
   * @param frame The frame.
   * @param msg Output parameter: The message contained within the payload of
   * the frame.
   */
  static void parsePayload(const ProcessForkerFrame &frame,
    messages::StopProcessForker &msg);

private:

  /**
   * The maximum permitted size in bytes for the payload of a frame sent between
   * the ProcessForker and its proxy.
   */
  static const ssize_t s_maxPayloadLen = 1024;

  /**
   * Writes a frame containing the specified message to the specified file
   * descriptor.
   *
   * @param fd The file descriptor to be written to.
   * @param type The type of message.
   * @param msg The message.
   * @param log Optional parameter: Object representing the API of the CASTOR
   * logging system.
   */
  static void writeFrame(const int fd, const messages::MsgType type,
    const google::protobuf::Message &msg, log::Logger *const log = NULL);

  /**
   * Writes a frame header to the specfied file descriptor.
   *
   * @param fd The file descriptor to be written to.
   * @param msgType The type of the message being sent in the payload of the
   * frame.
   * @param payloadLen The length of the frame payload in bytes.
   * @param log Optional parameter: Object representing the API of the CASTOR
   * logging system.
   */
  static void writeFrameHeader(const int fd,
    const messages::MsgType msgType,
    const uint32_t payloadLen,
    log::Logger *const log = NULL);

  /**
   * Writes the specified unsigned 32-bit integer to the specified file
   * descriptor.
   *
   * @param fd The file descriptor to be written to.
   * @param value The value to be written.
   */
  static void writeUint32(const int fd, const uint32_t value);

  /**
   * Writes the specified message as the payload of a frame to the specified
   * file descriptor.
   *
   * @param fd The file descriptor to be written to.
   * @param msg The message.
   * @param log Optional parameter: Object representing the API of the CASTOR
   * logging system.
   */
  static void writeFramePayload(const int fd,
    const google::protobuf::Message &msg, log::Logger *const log = NULL);

  /**
   * Writes the specified message as the payload of a frame to the specified
   * file descriptor.
   *
   * @param fd The file descriptor to be written to.
   * @param msg The message.
   */
  static void writeFramePayload(const int fd, const std::string &msg,
    log::Logger *const log = NULL);

  /**
   * Writes the specified string to the specified file descriptor.
   *
   * @param fd The file descriptor to be written to.
   * @param str The string.
   * @param log Optional parameter: Object representing the API of the CASTOR
   * logging system.
   */
  static void writeString(const int fd, const std::string &str,
    log::Logger *const log = NULL);

  /**
   * Reads the payload-type field of a frame from the specified file descriptor.
   *
   * @param fd The file descriptor to be read from.
   * @param timeout The timeout in seconds.
   * @return The value of the payload-type field.
   */
  static messages::MsgType readPayloadType(const int fd, const int timeout);

  /**
   * Reads the payload-length field of a frame from the specified file
   * descriptor.
   *
   * @param fd The file descriptor to be read from.
   * @param timeout The timeout in seconds.
   * @return The value of the payload-length field.
   */
  static uint32_t readPayloadLen(const int fd, const int timeout);

  /**
   * Reads an unsigned 32-bit integer from the specified file descriptor.
   *
   * @param fd The file descriptor to be read from.
   * @param timeout The timeout in seconds.
   * @return The unsigned 32-bit integer.
   */
  static uint32_t readUint32(const int fd, const int timeout);

  /**
   * Reads the payload of a frame from the specified file descriptor.
   *
   * @param fd The file descriptor to be read from.
   * @param timeout The timeout in seconds.
   * @param payloadLen The length of the payload in bytes.
   * @return The payload as a string.
   */
  static std::string readPayload(const int fd, const int timeout,
    const ssize_t payloadLen);

}; // class ProcessForkerUtils

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
