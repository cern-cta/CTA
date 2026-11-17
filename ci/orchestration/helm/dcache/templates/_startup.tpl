{{/*
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: GPL-3.0-or-later

Auth supplies the shared test CA; dCache signs host certificates here because their DNS names depend on its release and pools.
This keeps dCache topology out of the auth chart.

The pinned upstream chart calls this helper in both door and pool pods, passing the certificate hostname.
Extend its init sequence because it has no extraInitContainers setting.
Keep init-host-certs last: the caller appends its certificate volume mount to the last container.
*/}}
{{- define "dcache.certs.init" }}
        - name: wait-for-postgres
          image: postgres:17
          command: [sh, -ec]
          args:
            - |
              export PGHOST="$(cat /database/PGHOST)"
              export PGUSER="$(cat /database/POSTGRES_USER)"
              export PGPASSWORD="$(cat /database/POSTGRES_PASSWORD)"
              export PGDATABASE="$(cat /database/POSTGRES_DB)" PGCONNECT_TIMEOUT=5
              until psql -c 'SELECT 1'; do
                echo "Waiting for the dCache database"
                sleep 2
              done

              # Replace the upstream test connection defaults with the provisioner's Secret.
              sed -E '/^[[:space:]]*(dcache.db.(host|user|password)|[a-z-]+.db.name)[[:space:]]*=/d' \
                /database-base/dcache.conf > /database-config/dcache.conf
              printf '\n' >> /database-config/dcache.conf
              cat /database/dcache.conf >> /database-config/dcache.conf
          volumeMounts:
            - name: database-credentials
              mountPath: /database
              readOnly: true
            - name: database-base
              mountPath: /database-base
              readOnly: true
            - name: dcache-config
              mountPath: /database-config
        - name: init-host-certs
          image: docker.io/dcache/ci-init-cert:1.2
          securityContext:
            runAsUser: 0
            runAsGroup: 0
          env:
            - name: HOST_NAME
              value: {{ . | quote }}
            - name: TEST_CA_CERT
              valueFrom:
                secretKeyRef:
                  name: ca-crt-pem
                  key: ca.crt.pem
            - name: TEST_CA_KEY
              valueFrom:
                secretKeyRef:
                  name: ca-key-pem
                  key: ca.key.pem
          command: [sh, -ec]
          args:
            - |
              # Use the namespace test CA without downloading RPMs or contacting an external CA.
              umask 077
              work_dir=$(mktemp -d)
              trap 'rm -rf "$work_dir"' EXIT
              printf '%s\n' "$TEST_CA_CERT" > "$work_dir/ca.pem"
              printf '%s\n' "$TEST_CA_KEY" > "$work_dir/ca.key"
              unset TEST_CA_CERT TEST_CA_KEY

              # Sign directly with the auth chart's CA and its fixed test-only passphrase.
              short_host=${HOST_NAME%%.*}
              openssl req -new -x509 -newkey rsa:2048 -nodes -days 365 \
                -keyout "$work_dir/hostkey.pem" -out "$work_dir/hostcert.pem" \
                -subj "/CN=$short_host" -set_serial "0x$(openssl rand -hex 16)" \
                -CA "$work_dir/ca.pem" -CAkey "$work_dir/ca.key" -passin pass:1234 \
                -addext "basicConstraints=critical,CA:FALSE" \
                -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
                -addext "extendedKeyUsage=serverAuth,clientAuth" \
                -addext "subjectAltName=DNS:$HOST_NAME,DNS:$short_host"

              # dCache needs hashed CA trust; XRootD also requires an empty VOMS trust directory.
              cert_dir=/etc/grid-security
              install -d -m 0755 "$cert_dir" "$cert_dir/certificates" "$cert_dir/vomsdir"
              ca_hash=$(openssl x509 -in "$work_dir/ca.pem" -noout -subject_hash)
              ca_subject=$(openssl x509 -in "$work_dir/ca.pem" -noout -subject -nameopt compat)
              install -m 0644 "$work_dir/ca.pem" "$cert_dir/certificates/$ca_hash.0"
              cat > "$work_dir/signing_policy" <<EOF
              access_id_CA X509 '${ca_subject#subject=}'
              pos_rights globus CA:sign
              cond_subjects globus '"*"'
              EOF
              install -m 0644 "$work_dir/signing_policy" "$cert_dir/certificates/$ca_hash.signing_policy"
              install -o 994 -g 1000 -m 0644 "$work_dir/hostcert.pem" "$cert_dir/hostcert.pem"
              install -o 994 -g 1000 -m 0600 "$work_dir/hostkey.pem" "$cert_dir/hostkey.pem"
{{- end }}

{{/* The upstream chart supplies no database Secret option; both door and pools use this helper. */}}
{{- define "dcache.conf.volume" }}
- name: dcache-config
  emptyDir: {}
- name: database-base
  configMap:
    name: {{ .Release.Name }}-configmap
    items:
      - key: dcache.conf
        path: dcache.conf
- name: database-credentials
  secret:
    secretName: {{ .Values.database.existingSecret }}
- name: dcache-layout
  configMap:
    name: {{ .Release.Name }}-configmap
    items:
      - key: dcache-k8s-door
        path: dcache-k8s.conf
{{- end }}
