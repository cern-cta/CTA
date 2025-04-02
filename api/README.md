# CTA REST API

This directory contains the source code for the CTA REST API. At its foundation, the API is built using FastAPI and SQLAlchemy. A few important notes to remember when working on this code:

- The API layer and the internals should be clearly separated. If we were to switch from FastAPI to another API framework, the internals should be completely reusable. So make sure not to leak any FastAPI-specific things into the internals.
- We deliberately do not use an ORM. Stick to plain SQL queries, because that will make possible migrations in the future much easier. It is also easier to identify bottlenecks for problematic queries this way.
- When adding new routes, make sure the documentation reflects these changes appropriately.
- Keep testability in mind when writing the internals. Make everything stateless wherever possible.

## Configuration

There is only minimal configuration to the API, as it is meant to be a stupid stateless service. However, it expects a few environment variables:

- `CTA_CATALOGUE_CONF`: should contain the Catalogue DB connection string. If not provided, it will fall back to the file `/etc/cta/cta-catalogue.conf` for the connection string. If neither are provided, the service will not start.
- `JWKS_ENDPOINT`: should contain the endpoint that hosts the `jwks.json` used to verify the JWTs. Without providing this, the service will not start.
- `JWKS_CACHE_EXPIRY` (optional): how often (in seconds) should the `jwks.json` file be refreshed from the JWKS endpoint. Defaults to 300 seconds if not provided.

## Authentication

Every route in the API requires a JWT. The CTA REST API expects a JWT to be present in the "authorization" request header field. The client uses the "Bearer" authentication scheme to transmit the token as per section 2.1 of [RFC 6750](https://www.rfc-editor.org/rfc/rfc6750). For example:

```txt
GET /resource HTTP/1.1
Host: server.example.com
Authorization: Bearer mF_9.B5f-4.1JqM
```

The JWT headers must include the `alg`, `typ`, and `kid`. E.g.

```json
{
  "alg": "RS256",
  "typ": "JWT",
  "kid": "abc123"
}
```

The JWT body must include an expiration date (unix timestamp):

```json
{
  "exp": 1700000000
}
```

For security reasons, the only supported signing algorithms are:

- `RS256`
- `ES256`

## Running the unit tests

Set up a virtual environment and install the required dependencies:

```sh
python3 -m venv venv
. venv/bin/activate
pip install -r requirements/dev.txt
```

Once this is done, simply execute the following. This commands adds the `-v` flag to ensure we nicely see some more detailed output per test.

```sh
pytest tests/ -v
```
