// SPDX-FileCopyrightText: 2025 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

// Shamelessly copied from https://gitlab.cern.ch/eos/eos-charts/-/tree/master/utils/templates
{{/*
Privileged flag definition
  Used to run a container in priviledged mode.
  Defaults to "false", i.e., not privileged,
  but can be overridden by local or global values.
*/}}
{{- define "common.securityContext.privileged" }}
{{- $privilegedGlobal := "" -}}
{{- $privilegedLocal := "" -}}
{{- if .Values.global -}}
  {{ $privilegedGlobal = dig "securityContext" "privileged" "" .Values.global }}
{{- end }}
{{- if .Values.securityContext -}}
  {{ $privilegedLocal = dig "privileged" "" .Values.securityContext }}
{{- end }}
{{- if $privilegedGlobal }}
  {{- printf "%v" $privilegedGlobal }}
{{- else if $privilegedLocal }}
  {{- printf "%v" $privilegedLocal }}
{{- else }}
  {{- printf "false" }}
{{- end }}
{{- end }}


{{/*
Allow Privilege Escalation flag definition
  Used to allow for priviledge escalation by processes inside the container.
  Defaults to "false", i.e., escalation is not allowed,
  but can be overridden by local or global values.
*/}}
{{- define "common.securityContext.allowPrivilegeEscalation" -}}
{{- $apeGlobal := "" -}}
{{- $apeLocal := "" -}}
{{- if .Values.global -}}
  {{ $apeGlobal = dig "securityContext" "allowPrivilegeEscalation" "" .Values.global }}
{{- end }}
{{- if .Values.securityContext -}}
  {{ $apeLocal = dig "allowPrivilegeEscalation" "" .Values.securityContext }}
{{- end }}
{{- if $apeGlobal }}
  {{- printf "%v" $apeGlobal }}
{{- else if $apeLocal }}
  {{- printf "%v" $apeLocal }}
{{- else }}
  {{- printf "false" }}
{{- end }}
{{- end }}
