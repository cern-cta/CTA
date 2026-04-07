{{- /*
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{/* Keep backend metadata separate from the connection credentials in Secrets. */}}
{{- define "scheduler.config" -}}
{{- if .Values.global.objectstore.enabled }}
{{- if not (has .Values.global.objectstore.backend (list "vfs" "ceph")) }}
{{- fail "global.objectstore.backend must be vfs or ceph" }}
{{- end }}
backend: {{ .Values.global.objectstore.backend }}
cephConfig:
  id: {{ .Values.global.objectstore.ceph.id | quote }}
  pool: {{ .Values.global.objectstore.ceph.pool | quote }}
  namespace: {{ .Values.global.objectstore.ceph.namespace | quote }}
{{- else }}
backend: postgres
{{- end }}
{{- end }}
