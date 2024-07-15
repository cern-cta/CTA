{{ $filesHandler := .Files}}
{{ $namespace := .Release.Namespace}}
{{- range .Values.configs}}
# TODO add correct directory to read
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .name }}
  namespace: {{ $namespace }}
  labels:
    config: keypass-names
data:
  {{- if (.file)}}
    {{ .file }} : |-
      {{- range ( $filesHandler.Lines .file ) }}
      {{ . | indent 1}}
      {{- end}}
  {{- else}}
    {{- .data | toYaml | nindent 4}}
  {{- end}}
---
{{- end}}