{{ $filesHandler := .Files}}
{{ $content := $filesHandler.Get "configmaps/kdc-krb5.yaml" }}
{{- range .Values.cta.configs}}

apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .name }} 
  labels:
    config: keypass-names
data:
  example.txt: |-
  # TODO: Find why File.get gives error when reading here
---
{{- end}}