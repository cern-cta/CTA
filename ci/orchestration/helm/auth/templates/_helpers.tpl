{{- /*
SPDX-FileCopyrightText: 2025 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{- define "keycloak.fullname" -}}
  {{- printf "%s-%s" .Release.Name "keycloak" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kdc.fullname" -}}
  {{- printf "%s-%s" .Release.Name "kdc" | trunc 63 | trimSuffix "-" -}}
{{- end -}}
