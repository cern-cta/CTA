{{ $filesHandler := .Files}}
{{ $namespace := .Release.Namespace}}
{{- range .Values.configs}}

apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .name }}
  namespace: {{ $namespace }}
  labels:
    {{- .labels | toYaml | nindent 4}}
data:
  {{- if (.file)}}
  {{- $fullDir := printf "%s/%s" "configmaps" .file}}
  {{ .file }} : |-
    {{- range ( $filesHandler.Lines $fullDir ) }}
    {{ .}}
    {{- end}}
  {{- else}}
    {{- .data | toYaml | nindent 2}}
  {{- end}}

---
{{- end}}

---

apiVersion: v1
kind: ConfigMap
metadata:
  labels:
    config: eoscta
  name: eos-grpc-keytab
  namespace: {{.Release.Namespace}}
data:
  eos.grpc.keytab: |-
    # instance  host:port       token
    ctaeos      ctaeos:50051    2168e517-f9b2-458d-aa7b-4dc1ec448986
