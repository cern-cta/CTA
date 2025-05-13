# Auth directory Structure
This directory contains the logic related token authentication for the gRPC Frontend.

- `JWTAuthenticator.hpp`: This class inherits from `grpc::MetadataCredentialsPlugin`. It is client-side code.
It is expected to be used on per-call basis to attach credentials to each call.
It overrides the `GetMetadata` method which attaches the credentials to a metadata struct.
Then when making the call on the client side, we are expected to create call credentials as follows:
```c++
auto call_creds = grpc::MetadataCredentialsFromPlugin(
    std::unique_ptr<grpc::MetadataCredentialsPlugin>(
        new JWTAuthenticator("cta-grpc-jwt-auth-token")));
```
So even though it was written here, it should be moved to the EOS repository.

- `ServiceJWTAuthProcessor.hpp`: This class is the server-side interceptor that allows the server-side to verify the token by performing token validation.
The way it is expected to be used is the following:
First I need to create a shared pointer to grpc::ServerCredentials spServerCredentials.
Then call `spServerCredentials->SetAuthMetadataProcessor(spAuthProcessor);`
