# 1. generate a private key for my CA (root certificate)
openssl genpkey -algorithm RSA -out ca-key.pem -aes256 # 1234 as the phrase
# 1. create the root certificate and sign it with my private key
openssl req -key ca-key.pem -new -x509 -out ca-cert.pem -days 3650
# 2. generate a private key for the server. This will be used to establish the secure connection
openssl genpkey -algorithm RSA -out server-key.pem -aes256
# 3. Create a CSR certificate signing request for the server. Basically ask the CA to sign the server cert
openssl req -new -key server-key.pem -out server.csr # the common name should match the domain or IP the grpc server will be using
# 4. sign the server certificate with the root certificate
openssl x509 -req -in server.csr -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server-cert.pem -days 365
# 5. create the chain certificate (not needed if I am only using a single root certificate and server certificate)


############################# Another try, the example I found here; ###################################
## https://github.com/triton-inference-server/server/issues/5254

### This does work, it allows me to create the grpc server. But the health check service then it seems is not working

# Generate valid CA
openssl genrsa -passout pass:1234 -des3 -out ca.key 4096
openssl req -passin pass:1234 -new -x509 -days 365 -key ca.key -out ca.crt -subj  "/C=SP/ST=Spain/L=Valdepenias/O=Test/OU=Test/CN=Root CA"

# Generate valid Server Key/Cert
openssl genrsa -passout pass:1234 -des3 -out server.key 4096
openssl req -passin pass:1234 -new -key server.key -out server.csr -subj  "/C=SP/ST=Spain/L=Valdepenias/O=Test/OU=Server/CN=cta-frontend-grpc-0"
openssl x509 -req -passin pass:1234 -days 365 -in server.csr -CA ca.crt -CAkey ca.key -set_serial 01 -out server.crt

# Remove passphrase from the Server Key
openssl rsa -passin pass:1234 -in server.key -out server.key

# Generate valid Client Key/Cert
openssl genrsa -passout pass:1234 -des3 -out client.key 4096
openssl req -passin pass:1234 -new -key client.key -out client.csr -subj  "/C=SP/ST=Spain/L=Valdepenias/O=Test/OU=Client/CN=localhost"
openssl x509 -passin pass:1234 -req -days 365 -in client.csr -CA ca.crt -CAkey ca.key -set_serial 01 -out client.crt

# Remove passphrase from Client Key
openssl rsa -passin pass:1234 -in client.key -out client.key