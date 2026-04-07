{{- /*
SPDX-FileCopyrightText: 2025 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{- define "kdc.fullname" -}}
  {{- printf "%s-%s" .Release.Name "kdc" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Connection shared by objectstore initialization and the CTA daemons. */}}
{{- define "ci-setup.objectstoreUrl" -}}
{{- if eq .Values.objectstore.backend "ceph" -}}
rados://{{ .Values.objectstore.ceph.id }}@{{ .Values.objectstore.ceph.pool }}:{{ .Values.objectstore.ceph.namespace }}
{{- else if eq .Values.objectstore.backend "vfs" -}}
/objectstore
{{- else -}}
{{- fail "objectstore.backend must be vfs or ceph" -}}
{{- end -}}
{{- end -}}
