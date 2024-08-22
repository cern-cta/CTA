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
