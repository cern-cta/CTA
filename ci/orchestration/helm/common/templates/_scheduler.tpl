{{- /*
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: GPL-3.0-or-later
*/ -}}

{{/* Keep backend-specific mounts identical for services and administration jobs. */}}
{{- define "common.scheduler.mounts" -}}
- name: scheduler-config
  mountPath: /etc/cta/cta-scheduler.conf
  subPath: cta-scheduler.conf
  readOnly: true
{{- if eq .backend "ceph" }}
- name: scheduler-config
  mountPath: /etc/ceph
  readOnly: true
{{- else if eq .backend "vfs" }}
- name: vfs-storage-volume
  mountPath: /objectstore
{{- end }}
{{- end }}

{{- define "common.scheduler.volumes" -}}
{{- if not (has .backend (list "postgres" "ceph" "vfs")) }}
{{- fail "global.scheduler.backend must be postgres, ceph or vfs" }}
{{- end }}
- name: scheduler-config
  secret:
    secretName: {{ required "global.scheduler.existingSecret is required" .existingSecret | quote }}
{{- if eq .backend "vfs" }}
- name: vfs-storage-volume
  persistentVolumeClaim:
    claimName: {{ required "global.scheduler.existingClaim is required for vfs" .existingClaim | quote }}
{{- end }}
{{- end }}
