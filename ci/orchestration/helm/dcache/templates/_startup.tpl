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
          envFrom:
            - configMapRef:
                name: postgres-secret
          command: [sh, -ec]
          args:
            - |
              export PGPASSWORD="$POSTGRES_PASSWORD" PGCONNECT_TIMEOUT=5
              until psql -h chimera-postgresql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c 'SELECT 1'; do
                echo "Waiting for the dCache database"
                sleep 2
              done
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
