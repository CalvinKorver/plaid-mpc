"""
One-time setup script to get a Plaid access_token via the Link flow.

Usage:
    source .venv/bin/activate
    python3 setup_link.py
    # Open http://localhost:8080 in your browser
    # Click "Connect Bank" and complete the Plaid Link flow
    # Copy the printed PLAID_ACCESS_TOKEN into your .env file
    # Ctrl+C to stop
"""

from dotenv import load_dotenv
load_dotenv()

import os
from flask import Flask, request, jsonify
import plaid
from plaid.api import plaid_api
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.country_code import CountryCode
from plaid.model.products import Products

PLAID_CLIENT_ID = os.environ["PLAID_CLIENT_ID"]
PLAID_SECRET = os.environ["PLAID_SECRET"]
PLAID_ENV = os.environ.get("PLAID_ENV", "production").lower()

ENV_MAP = {
    "sandbox": plaid.Environment.Sandbox,
    "production": plaid.Environment.Production,
}

if PLAID_ENV not in ENV_MAP:
    raise ValueError(f"PLAID_ENV must be 'sandbox' or 'production', got: {PLAID_ENV!r}")

configuration = plaid.Configuration(
    host=ENV_MAP[PLAID_ENV],
    api_key={"clientId": PLAID_CLIENT_ID, "secret": PLAID_SECRET},
)
api_client = plaid.ApiClient(configuration)
client = plaid_api.PlaidApi(api_client)

app = Flask(__name__)

HTML_PAGE = """<!DOCTYPE html>
<html>
<head>
  <title>Plaid Link Setup</title>
  <style>
    body { font-family: sans-serif; max-width: 480px; margin: 80px auto; text-align: center; }
    button { padding: 12px 24px; font-size: 16px; background: #00c853; color: white;
             border: none; border-radius: 6px; cursor: pointer; }
    button:hover { background: #00b048; }
    #status { margin-top: 20px; color: #555; }
  </style>
</head>
<body>
  <h2>Connect Your Bank Account</h2>
  <p>Click the button below and follow the prompts to link your bank.</p>
  <button id="link-button">Connect Bank</button>
  <p id="status"></p>

  <script src="https://cdn.plaid.com/link/v2/stable/link-initialize.js"></script>
  <script>
    async function initLink() {
      const resp = await fetch('/create_link_token');
      const data = await resp.json();
      if (data.error) {
        document.getElementById('status').textContent = 'Error: ' + data.error;
        return;
      }

      const handler = Plaid.create({
        token: data.link_token,
        onSuccess: async (public_token, metadata) => {
          document.getElementById('status').textContent = 'Exchanging token — check your terminal...';
          const result = await fetch('/exchange_token', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({public_token})
          });
          const json = await result.json();
          if (json.success) {
            document.getElementById('status').textContent =
              'Done! Your access_token has been printed in the terminal. Add it to your .env file, then Ctrl+C this server.';
          } else {
            document.getElementById('status').textContent = 'Exchange failed: ' + (json.error || 'unknown error');
          }
        },
        onExit: (err, metadata) => {
          if (err) {
            console.error(err);
            document.getElementById('status').textContent = 'Link exited: ' + err.display_message;
          }
        }
      });

      document.getElementById('link-button').onclick = () => handler.open();
    }

    initLink();
  </script>
</body>
</html>
"""


@app.route("/")
def index():
    return HTML_PAGE


@app.route("/create_link_token")
def create_link_token():
    try:
        link_request = LinkTokenCreateRequest(
            products=[Products("transactions")],
            client_name="Plaid MPC Setup",
            country_codes=[CountryCode("US")],
            language="en",
            user=LinkTokenCreateRequestUser(client_user_id="local-setup-user"),
        )
        response = client.link_token_create(link_request)
        return jsonify({"link_token": response["link_token"]})
    except plaid.ApiException as e:
        return jsonify({"error": str(e)}), 500


@app.route("/exchange_token", methods=["POST"])
def exchange_token():
    try:
        public_token = request.json["public_token"]
        exchange_request = ItemPublicTokenExchangeRequest(public_token=public_token)
        response = client.item_public_token_exchange(exchange_request)
        access_token = response["access_token"]

        print("\n" + "=" * 60)
        print("SUCCESS! Your access_token is:")
        print(access_token)
        print()
        print("Add this line to your .env file:")
        print(f"PLAID_ACCESS_TOKEN={access_token}")
        print("=" * 60 + "\n")

        return jsonify({"success": True})
    except plaid.ApiException as e:
        return jsonify({"error": str(e), "success": False}), 500


if __name__ == "__main__":
    print(f"Starting Plaid Link setup server (env={PLAID_ENV})...")
    print("Open http://localhost:8080 in your browser to connect your bank.")
    app.run(port=8080, debug=False)
