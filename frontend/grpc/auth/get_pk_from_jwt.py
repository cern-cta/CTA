from jwcrypto import jwk
import jwt
import requests
# on client pod
# dnf install python3-pip

## obtain the token from keycloak
def get_admin_token():
    url = "http://auth-keycloak:8080/realms/master/protocol/openid-connect/token"
    # Form data for the POST request
    data = {
        "grant_type": "password",
        "client_id": "admin-cli",
        "username": "admin",
        "password": "admin"
    }
    # Headers for the request
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    # Send the POST request to get the access token
    response = requests.post(url, data=data, headers=headers)

    if response.status_code == 200:
        # Extract the access_token from the JSON response
        response_json = response.json()
        admin_access_token = response_json.get("access_token")
        print("Access Token:", admin_access_token)
    else:
        print(f"Failed to retrieve access token. Status code: {response.status_code}")
    return admin_access_token


def update_token_lifetime(admin_access_token):
    # URL for the Keycloak realm endpoint
    url = "http://auth-keycloak:8080/admin/realms/master"

    # Headers with Authorization and Content-Type
    headers = {
        "Authorization": f"Bearer {admin_access_token}",
        "Content-Type": "application/json"
    }

    # Data to update the realm settings
    data = {
        "accessTokenLifespan": 29030400,
        "ssoSessionMaxLifespan": 29030400
    }

    # Send the PUT request
    response = requests.put(url, headers=headers, json=data)

    # Check if the response was successful
    if response.status_code == 204:
        print("Realm updated successfully.")
    else:
        print(f"Failed to update realm. Status code: {response.status_code}")
        print(response.text)


admin_token = get_admin_token()
update_token_lifetime(admin_token)
token = get_admin_token()
# with open('token', 'r') as file:
#     token = file.read().rstrip()

header = jwt.get_unverified_header(token)
print(header)

# Get the 'kid'
kid = header.get("kid")
alg = header.get("alg")

print("KID:", kid)
print("ALG:", alg)

# now for the specific KID, get the public key from the JWKS
# Fetch the JWKS from Keycloak
jwks_url = "http://auth-keycloak:8080/realms/master/protocol/openid-connect/certs"
response = requests.get(jwks_url)
jwks = response.json()
key_data = (jwks['keys']['kid' == kid])

print(key_data)

key = jwk.JWK(**key_data)
pubkey = (key.export_to_pem(private_key=False).decode())
print(pubkey)

## Validate the token using the public key..?
jwt.decode(token, pubkey, algorithms=["RS256"])

## this blogpost should be helpful in token validation: https://blog.ropardo.ro/2025/02/06/keycloak-access-token-verification-a-step-by-step-example/
