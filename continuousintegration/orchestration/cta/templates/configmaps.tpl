{{ $filesHandler := .Files}}
{{ $namespace := .Release.Namespace}}
{{- range .Values.configs}}

apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .name }}
  namespace: {{ $namespace }}
  labels:
    config: keypass-names
data:
  {{- if (.file)}}
    {{$fullDir := printf "%s/%s" "configmaps" .file}}
    {{ .file }} : |-
      {{- range ( $filesHandler.Lines $fullDir ) }}
      {{ . | indent 1}}
      {{- end}}
  {{- else}}
    {{- .data | toYaml | nindent 4}}
  {{- end}}

---
{{- end}}