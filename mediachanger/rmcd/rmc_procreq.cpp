/*
 * SPDX-FileCopyrightText: 2001 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rmc_procreq.hpp"

#include "mediachanger/librmc/marshall.hpp"
#include "mediachanger/librmc/serrno.hpp"
#include "mediachanger/librmc/smc_struct.hpp"
#include "rmc_constants.hpp"
#include "rmc_marshall_element.hpp"
#include "rmc_sendrep.hpp"
#include "rmc_smcsubr.hpp"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

// set in rmc_serv.c
extern struct extended_robot_info g_extended_robot_info;

int rmc_srv_export(cta::log::LogContext& lc, const struct rmc_srv_rqst_context* const rqst_context) {
  int c;
  gid_t gid;
  char* rbp;
  uid_t uid;
  char vid[CA_MAXVIDLEN + 1];

  rbp = rqst_context->req_data;
  const char* req_data_end = rqst_context->req_data + REQ_DATA_SIZE;
  unmarshall_LONG(rbp, uid);
  unmarshall_LONG(rbp, gid);
  // TODO: better parameter names?
  cta::log::ScopedParamContainer params(lc);
  params.add("request", "eject_cartridge");
  params.add("request_uid", uid);
  params.add("request_gid", gid);
  params.add("request_from", std::string(rqst_context->clienthost));
  // Unmarshall and ignore the loader field as it is no longer used
  {
    char smc_ldr[CA_MAXRBTNAMELEN + 1];
    if (unmarshall_STRINGN(&rbp, req_data_end, smc_ldr, CA_MAXRBTNAMELEN + 1)) {
      rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC06, "loader");
      params.add(cta::semconv::log::errorMessage, "Failed to unmarshall string for smc loader");
      lc.log(cta::log::ERR, "Eject cartridge failed");
      params.add("rc", ERMCUNREC);
      return ERMCUNREC;
    }
  }
  if (unmarshall_STRINGN(&rbp, req_data_end, vid, CA_MAXVIDLEN + 1)) {
    rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC06, "vid");
    params.add(cta::semconv::log::errorMessage, "Failed to unmarshall string for VID");
    lc.log(cta::log::ERR, "Eject cartridge failed");
    params.add("rc", ERMCUNREC);
    return ERMCUNREC;
  }
  params.add("vid", std::string(vid));
  lc.log(cta::log::DEBUG, "Attempting to eject cartridge");
  c = smc_export(rqst_context->rpfd,
                 lc,
                 g_extended_robot_info.smc_fd,
                 g_extended_robot_info.smc_ldr,
                 &g_extended_robot_info.robot_info,
                 vid);

  if (c) {
    c += ERMCRBTERR;
    params.add("rc", c);
    lc.log(cta::log::ERR, "Eject cartridge failed");
  }
  lc.log(cta::log::INFO, "Eject cartridge success");
  return 0;
}

int rmc_srv_findcart(cta::log::LogContext& lc, const struct rmc_srv_rqst_context* const rqst_context) {
  int c;
  struct smc_element_info* element_info;
  struct smc_element_info* elemp;
  gid_t gid;
  int i;
  const char* msgaddr;
  int nbelem;
  char* rbp;
  char* repbuf;
  char* sbp;
  struct smc_status smc_status;
  int startaddr;
  char fmt_template[40];
  int type;
  uid_t uid;

  rbp = rqst_context->req_data;
  const char* req_data_end = rqst_context->req_data + REQ_DATA_SIZE;
  unmarshall_LONG(rbp, uid);
  unmarshall_LONG(rbp, gid);

  // TODO: better parameter names?
  cta::log::ScopedParamContainer params(lc);
  params.add("request", "find_cartridge");
  params.add("request_uid", uid);
  params.add("request_gid", gid);
  params.add("request_from", std::string(rqst_context->clienthost));
  // Unmarshall and ignore the loader field as it is no longer used
  {
    char smc_ldr[CA_MAXRBTNAMELEN + 1];
    if (unmarshall_STRINGN(&rbp, req_data_end, smc_ldr, CA_MAXRBTNAMELEN + 1)) {
      rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC06, "loader");
      params.add(cta::semconv::log::errorMessage, "Failed to unmarshall string for smc loader");
      params.add("rc", ERMCUNREC);
      lc.log(cta::log::ERR, "Find cartridge failed");
      return ERMCUNREC;
    }
  }
  if (unmarshall_STRINGN(&rbp, req_data_end, fmt_template, 40)) {
    rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC06, "fmt_template");
    params.add(cta::semconv::log::errorMessage, "Failed to unmarshall string for fmt_template");
    params.add("rc", ERMCUNREC);
    lc.log(cta::log::ERR, "Find cartridge failed");
    return ERMCUNREC;
  }
  unmarshall_LONG(rbp, type);
  unmarshall_LONG(rbp, startaddr);
  unmarshall_LONG(rbp, nbelem);
  params.add("fmt_template", std::string(fmt_template));
  params.add("nb_elem", nbelem);
  lc.log(cta::log::DEBUG, "Attempting to find cartridge");

  if (nbelem < 1) {
    rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC06, "nbelem");
    params.add(cta::semconv::log::errorMessage, "nb_elem is negative");
    params.add("rc", ERMCUNREC);
    lc.log(cta::log::ERR, "Find cartridge failed");
    return ERMCUNREC;
  }
  if ((element_info = reinterpret_cast<smc_element_info*>(malloc(nbelem * sizeof(struct smc_element_info))))
      == nullptr) {
    rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC05);
    params.add(cta::semconv::log::errorMessage, "Cast failed");
    params.add("rc", ERMCUNREC);
    lc.log(cta::log::ERR, "Find cartridge failed");
    return ERMCUNREC;
  }
  c = smc_find_cartridge(g_extended_robot_info.smc_fd,
                         g_extended_robot_info.smc_ldr,
                         fmt_template,
                         type,
                         startaddr,
                         nbelem,
                         element_info,
                         &g_extended_robot_info.robot_info);
  if (c < 0) {
    c = smc_lasterror(lc, &smc_status, &msgaddr);
    free(element_info);
    rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC02, "smc_find_cartridge", msgaddr);
    c += ERMCRBTERR;
    params.add(cta::semconv::log::errorMessage, "nb_elem is negative");
    params.add("rc", c);
    lc.log(cta::log::ERR, "Find cartridge failed");
    return c;
  }
  if ((repbuf = reinterpret_cast<char*>(malloc(c * 18 + 4))) == nullptr) {
    rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC05);
    free(element_info);
    params.add("rc", ERMCUNREC);
    lc.log(cta::log::ERR, "Find cartridge failed");
    return ERMCUNREC;
  }
  sbp = repbuf;
  marshall_LONG(sbp, c);
  for (i = 0, elemp = element_info; i < c; i++, elemp++) {
    rmc_marshall_element(&sbp, elemp);
  }
  free(element_info);
  rmc_sendrep(lc, rqst_context->rpfd, MSG_DATA, sbp - repbuf, repbuf);
  free(repbuf);
  lc.log(cta::log::INFO, "Find cartridge success");
  return 0;
}

int rmc_srv_getgeom(cta::log::LogContext& lc, const struct rmc_srv_rqst_context* const rqst_context) {
  gid_t gid;
  char* rbp;
  char repbuf[64];
  char* sbp;
  uid_t uid;

  rbp = rqst_context->req_data;
  const char* req_data_end = rqst_context->req_data + REQ_DATA_SIZE;
  unmarshall_LONG(rbp, uid);
  unmarshall_LONG(rbp, gid);
  // TODO: better parameter names?
  cta::log::ScopedParamContainer params(lc);
  params.add("request", "get_library_robot_geometry");
  params.add("request_uid", uid);
  params.add("request_gid", gid);
  params.add("request_from", std::string(rqst_context->clienthost));
  // Unmarshall and ignore the loader field as it is no longer used
  {
    char smc_ldr[CA_MAXRBTNAMELEN + 1];
    if (unmarshall_STRINGN(&rbp, req_data_end, smc_ldr, CA_MAXRBTNAMELEN + 1)) {
      rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC06, "loader");
      params.add(cta::semconv::log::errorMessage, "Failed to unmarshall string for smc loader");
      params.add("rc", ERMCUNREC);
      lc.log(cta::log::ERR, "Get library robot geometry failed");
      return ERMCUNREC;
    }
  }
  lc.log(cta::log::DEBUG, "Attempting to get library robot geometry");

  sbp = repbuf;
  marshall_STRING(sbp, g_extended_robot_info.robot_info.inquiry);
  marshall_LONG(sbp, g_extended_robot_info.robot_info.transport_start);
  marshall_LONG(sbp, g_extended_robot_info.robot_info.transport_count);
  marshall_LONG(sbp, g_extended_robot_info.robot_info.slot_start);
  marshall_LONG(sbp, g_extended_robot_info.robot_info.slot_count);
  marshall_LONG(sbp, g_extended_robot_info.robot_info.port_start);
  marshall_LONG(sbp, g_extended_robot_info.robot_info.port_count);
  marshall_LONG(sbp, g_extended_robot_info.robot_info.device_start);
  marshall_LONG(sbp, g_extended_robot_info.robot_info.device_count);
  rmc_sendrep(lc, rqst_context->rpfd, MSG_DATA, sbp - repbuf, repbuf);

  params.add("inquiry", std::string(g_extended_robot_info.robot_info.inquiry));
  params.add("transport_start", g_extended_robot_info.robot_info.transport_start);
  params.add("transport_count", g_extended_robot_info.robot_info.transport_count);
  params.add("slot_start", g_extended_robot_info.robot_info.slot_start);
  params.add("slot_count", g_extended_robot_info.robot_info.slot_count);
  params.add("port_start", g_extended_robot_info.robot_info.port_start);
  params.add("port_count", g_extended_robot_info.robot_info.port_count);
  params.add("device_start", g_extended_robot_info.robot_info.device_start);
  params.add("device_count", g_extended_robot_info.robot_info.device_count);
  lc.log(cta::log::INFO, "Get library robot geometry success");
  return 0;
}

int rmc_srv_import(cta::log::LogContext& lc, const struct rmc_srv_rqst_context* const rqst_context) {
  int c;
  gid_t gid;
  char* rbp;
  uid_t uid;
  char vid[CA_MAXVIDLEN + 1];

  rbp = rqst_context->req_data;
  const char* req_data_end = rqst_context->req_data + REQ_DATA_SIZE;
  unmarshall_LONG(rbp, uid);
  unmarshall_LONG(rbp, gid);
  // TODO: better parameter names?
  cta::log::ScopedParamContainer params(lc);
  params.add("request", "inject_cartridge");
  params.add("request_uid", uid);
  params.add("request_gid", gid);
  params.add("request_from", std::string(rqst_context->clienthost));
  // Unmarshall and ignore the loader field as it is no longer used
  {
    char smc_ldr[CA_MAXRBTNAMELEN + 1];
    if (unmarshall_STRINGN(&rbp, req_data_end, smc_ldr, CA_MAXRBTNAMELEN + 1)) {
      rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC06, "loader");
      params.add(cta::semconv::log::errorMessage, "Failed to unmarshall string for smc loader");
      params.add("rc", ERMCUNREC);
      lc.log(cta::log::ERR, "Inject cartridge failed");
      return ERMCUNREC;
    }
  }
  if (unmarshall_STRINGN(&rbp, req_data_end, vid, CA_MAXVIDLEN + 1)) {
    rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC06, "vid");
    params.add(cta::semconv::log::errorMessage, "Failed to unmarshall string for VID");
    params.add("rc", ERMCUNREC);
    lc.log(cta::log::ERR, "Inject cartridge failed");
    return ERMCUNREC;
  }
  params.add("vid", std::string(vid));
  lc.log(cta::log::DEBUG, "Attempting to inject cartridge");

  c = smc_import(rqst_context->rpfd,
                 lc,
                 g_extended_robot_info.smc_fd,
                 g_extended_robot_info.smc_ldr,
                 &g_extended_robot_info.robot_info,
                 vid);
  if (c) {
    c += ERMCRBTERR;
    params.add("rc", c);
    lc.log(cta::log::ERR, "Inject cartridge failed");
    return c;
  }
  lc.log(cta::log::INFO, "Inject cartridge success");
  return 0;
}

int rmc_srv_mount(cta::log::LogContext& lc, const struct rmc_srv_rqst_context* const rqst_context) {
  int c;
  int drvord;
  gid_t gid;
  int invert;
  char* rbp;
  uid_t uid;
  char vid[CA_MAXVIDLEN + 1];

  rbp = rqst_context->req_data;
  const char* req_data_end = rqst_context->req_data + REQ_DATA_SIZE;
  unmarshall_LONG(rbp, uid);
  unmarshall_LONG(rbp, gid);
  // TODO: better parameter names?
  cta::log::ScopedParamContainer params(lc);
  params.add("request", "mount_cartridge");
  params.add("request_uid", uid);
  params.add("request_gid", gid);
  params.add("request_from", std::string(rqst_context->clienthost));
  // Unmarshall and ignore the loader field as it is no longer used
  {
    char smc_ldr[CA_MAXRBTNAMELEN + 1];
    if (unmarshall_STRINGN(&rbp, req_data_end, smc_ldr, CA_MAXRBTNAMELEN + 1)) {
      rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC06, "loader");
      params.add(cta::semconv::log::errorMessage, "Failed to unmarshall string for smc loader");
      params.add("rc", ERMCUNREC);
      lc.log(cta::log::ERR, "Mount cartridge failed");
      return ERMCUNREC;
    }
  }
  if (unmarshall_STRINGN(&rbp, req_data_end, vid, CA_MAXVIDLEN + 1)) {
    rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC06, "vid");
    params.add(cta::semconv::log::errorMessage, "Failed to unmarshall string for VID");
    params.add("rc", ERMCUNREC);
    lc.log(cta::log::ERR, "Mount cartridge failed");
    return ERMCUNREC;
  }
  unmarshall_SHORT(rbp, invert);
  unmarshall_SHORT(rbp, drvord);

  params.add("vid", std::string(vid));
  params.add("drive_ordinal", drvord);
  params.add("invert", invert);
  lc.log(cta::log::DEBUG, "Attempting to mount cartridge");

  c = smc_mount(rqst_context->rpfd,
                lc,
                g_extended_robot_info.smc_fd,
                g_extended_robot_info.smc_ldr,
                &g_extended_robot_info.robot_info,
                drvord,
                vid,
                invert);
  if (c) {
    c += ERMCRBTERR;
    params.add("rc", c);
    lc.log(cta::log::ERR, "Mount cartridge failed");
    return c;
  }
  lc.log(cta::log::INFO, "Mount cartridge success");
  return 0;
}

int rmc_srv_readelem(cta::log::LogContext& lc, const struct rmc_srv_rqst_context* const rqst_context) {
  int c;
  struct smc_element_info* element_info;
  struct smc_element_info* elemp;
  gid_t gid;
  int i;
  const char* msgaddr;
  int nbelem;
  char* rbp;
  char* repbuf;
  char* sbp;
  struct smc_status smc_status;
  int startaddr;
  int type;
  uid_t uid;

  rbp = rqst_context->req_data;
  const char* req_data_end = rqst_context->req_data + REQ_DATA_SIZE;
  unmarshall_LONG(rbp, uid);
  unmarshall_LONG(rbp, gid);
  // TODO: better parameter names?
  cta::log::ScopedParamContainer params(lc);
  params.add("request", "read_element");
  params.add("request_uid", uid);
  params.add("request_gid", gid);
  params.add("request_from", std::string(rqst_context->clienthost));
  // Unmarshall and ignore the loader field as it is no longer used
  {
    char smc_ldr[CA_MAXRBTNAMELEN + 1];
    if (unmarshall_STRINGN(&rbp, req_data_end, smc_ldr, CA_MAXRBTNAMELEN + 1)) {
      rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC06, "loader");
      params.add(cta::semconv::log::errorMessage, "Failed to unmarshall string for smc loader");
      params.add("rc", ERMCUNREC);
      lc.log(cta::log::ERR, "Read element failed");
      return ERMCUNREC;
    }
  }
  unmarshall_LONG(rbp, type);
  unmarshall_LONG(rbp, startaddr);
  unmarshall_LONG(rbp, nbelem);
  params.add("start_addr", startaddr);
  params.add("nb_elem", nbelem);
  params.add("type", type);
  lc.log(cta::log::DEBUG, "Attempting to read element");

  if (type < 0 || type > 4) {
    rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC06, "type");
    params.add(cta::semconv::log::errorMessage, "Invalid type");
    params.add("rc", ERMCUNREC);
    lc.log(cta::log::ERR, "Read element failed");
    return ERMCUNREC;
  }
  if (nbelem < 1) {
    rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC06, "nbelem");
    params.add(cta::semconv::log::errorMessage, "nb_elem is negative");
    params.add("rc", c);
    lc.log(cta::log::ERR, "Read element failed");
    return ERMCUNREC;
  }
  if ((element_info = reinterpret_cast<smc_element_info*>(malloc(nbelem * sizeof(struct smc_element_info))))
      == nullptr) {
    rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC05);
    // TODO error message
    params.add("rc", c);
    lc.log(cta::log::ERR, "Read element failed");
    return ERMCUNREC;
  }
  if ((c = smc_read_elem_status(g_extended_robot_info.smc_fd,
                                g_extended_robot_info.smc_ldr,
                                type,
                                startaddr,
                                nbelem,
                                element_info))
      < 0) {
    c = smc_lasterror(lc, &smc_status, &msgaddr);
    free(element_info);
    rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC02, "smc_read_elem_status", msgaddr);
    c += ERMCRBTERR;
    params.add("rc", c);
    lc.log(cta::log::ERR, "Read element failed");
    return c;
  }
  if ((repbuf = reinterpret_cast<char*>(malloc(c * 18 + 4))) == nullptr) {
    rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC05);
    free(element_info);
    params.add("rc", ERMCUNREC);
    lc.log(cta::log::ERR, "Read element failed");
    return ERMCUNREC;
  }
  sbp = repbuf;
  marshall_LONG(sbp, c);
  for (i = 0, elemp = element_info; i < c; i++, elemp++) {
    rmc_marshall_element(&sbp, elemp);
  }
  free(element_info);
  rmc_sendrep(lc, rqst_context->rpfd, MSG_DATA, sbp - repbuf, repbuf);
  free(repbuf);
  lc.log(cta::log::INFO, "Read element success");
  return 0;
}

int rmc_srv_unmount(cta::log::LogContext& lc, const struct rmc_srv_rqst_context* const rqst_context) {
  int c;
  int drvord;
  int force;
  gid_t gid;
  char* rbp;
  uid_t uid;
  char vid[CA_MAXVIDLEN + 1];

  rbp = rqst_context->req_data;
  const char* req_data_end = rqst_context->req_data + REQ_DATA_SIZE;
  unmarshall_LONG(rbp, uid);
  unmarshall_LONG(rbp, gid);
  cta::log::ScopedParamContainer params(lc);
  params.add("request", "unmount_cartridge");
  params.add("request_uid", uid);
  params.add("request_gid", gid);
  params.add("request_from", std::string(rqst_context->clienthost));
  // Unmarshall and ignore the loader field as it is no longer used
  {
    char smc_ldr[CA_MAXRBTNAMELEN + 1];
    if (unmarshall_STRINGN(&rbp, req_data_end, smc_ldr, CA_MAXRBTNAMELEN + 1)) {
      rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC06, "loader");
      params.add(cta::semconv::log::errorMessage, "Failed to unmarshall string for smc loader");
      params.add("rc", ERMCUNREC);
      lc.log(cta::log::ERR, "Unmount cartridge failed");
      return ERMCUNREC;
    }
  }
  if (unmarshall_STRINGN(&rbp, req_data_end, vid, CA_MAXVIDLEN + 1)) {
    rmc_sendrep(lc, rqst_context->rpfd, MSG_ERR, RMC06, "vid");
    params.add(cta::semconv::log::errorMessage, "Failed to unmarshall string for VID");
    params.add("rc", ERMCUNREC);
    lc.log(cta::log::ERR, "Unmount cartridge failed");
    return ERMCUNREC;
  }
  unmarshall_SHORT(rbp, drvord);
  unmarshall_SHORT(rbp, force);

  params.add("vid", std::string(vid));
  params.add("drive_ordinal", drvord);
  params.add("force", force);
  lc.log(cta::log::DEBUG, "Attempting to unmount cartridge");

  c = smc_dismount(rqst_context->rpfd,
                   lc,
                   g_extended_robot_info.smc_fd,
                   g_extended_robot_info.smc_ldr,
                   &g_extended_robot_info.robot_info,
                   drvord,
                   force == 0 ? vid : "");
  if (c) {
    c += ERMCRBTERR;
    params.add("rc", c);
    lc.log(cta::log::ERR, "Unmount cartridge failed");
    return c;
  }
  lc.log(cta::log::INFO, "Unmount cartridge success");
  return 0;
}
