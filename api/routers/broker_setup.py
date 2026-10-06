"""
routers/broker_setup.py
─────────────────────────
Route/data-compatible port of the old algo.trade's broker-setup catalog and
broker-credentials CRUD (api.py:5947-6666) — the "choose your broker" grid
(BrokerLogin.tsx) and the dynamic per-broker Setup page it links to.

BROKER_SETUP_SEED / BROKER_FIELD_SCHEMA / BROKER_ICON_OVERRIDES are copied
verbatim from the old system (same broker_key catalog, same per-broker field
schema and setup instructions, sourced from AlgoTest's own decompiled
broker-instruction data — see the old file's comments for provenance). Same
`broker_setup` / `broker_credentials` / `broker_configuration` Mongo
collections, so nothing here needs a data migration.

NOT ported: broker_gateway.reset_broker_cache() call in
save_broker_credentials for broker_key=="dhan" — that's old-system
process-wide tick/quote cache machinery algo-2_0 doesn't share; skipped
rather than importing a cache-invalidation hook with no corresponding cache
here.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from pymongo.errors import DuplicateKeyError

from shared.auth.dependency import get_current_user
from shared.db.mongo import get_mongo

router = APIRouter(prefix="/algo", tags=["broker-setup"])


def _f(key: str, label: str, placeholder: str = "") -> dict:
    return {
        "key": key,
        "label": label,
        "placeholder": placeholder or label,
        "masked": ("secret" in key) or ("totp" in key) or key == "pin",
    }


def _i(text: str, href: str | None = None, copy: str | None = None, label: str | None = None) -> dict:
    step: dict = {"text": text}
    if href:
        step["href"] = href
    if copy:
        step["copy"] = copy
    if label:
        step["label"] = label
    return step


BROKER_SETUP_SEED: list[dict] = [
    # -- brokers we actually integrate today --
    {"broker_key": "dhan",       "display_name": "Dhan Client",   "broker_icon": "dhan.svg",       "badges": ["NSE", "BSE", "MCX"], "pricing_note": "", "is_active": True, "sort_order": 10},
    {"broker_key": "zerodha",    "display_name": "Zerodha Kite",  "broker_icon": "kite-logo.svg",  "badges": ["NSE", "BSE"],        "pricing_note": "", "is_active": True, "sort_order": 20},
    {"broker_key": "flattrade",  "display_name": "FlatTrade",     "broker_icon": "flattrade.svg",  "badges": ["NSE", "BSE"],        "pricing_note": "", "is_active": True, "sort_order": 30},
    # -- catalog of brokers not yet wired up (shown as "Coming Soon") --
    {"broker_key": "upstox",             "display_name": "Upstox Client",          "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 100},
    {"broker_key": "5paisa_xstream",     "display_name": "5Paisa XStream (Client)","badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 110},
    {"broker_key": "5paisa_xts",         "display_name": "5Paisa XTS",             "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 120},
    {"broker_key": "ac_agrawal",         "display_name": "A.C. Agrawal Shares",    "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 130},
    {"broker_key": "aliceblue",          "display_name": "Alice Blue Client",      "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 140},
    {"broker_key": "angelone",           "display_name": "Angel One Limited",      "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 150},
    {"broker_key": "arham",              "display_name": "Arham Wealth",           "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 160},
    {"broker_key": "bigul",              "display_name": "Bigul",                  "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 170},
    {"broker_key": "coinswitch",         "display_name": "CoinSwitch",             "badges": ["COINSWITCH"], "pricing_note": "", "is_active": False, "sort_order": 180},
    {"broker_key": "db_international",   "display_name": "DB International",      "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 190},
    {"broker_key": "delta_exchange",     "display_name": "Delta Exchange",         "badges": ["DELTA"], "pricing_note": "", "is_active": True, "sort_order": 200},
    {"broker_key": "delta_exchange_api", "display_name": "Delta Exchange (API)",   "badges": ["DELTA"], "pricing_note": "", "is_active": False, "sort_order": 210},
    {"broker_key": "findoc",             "display_name": "Findoc",                 "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 220},
    {"broker_key": "findoc_xts",         "display_name": "Findoc XTS",             "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 230},
    {"broker_key": "firstock",           "display_name": "Firstock",               "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 240},
    {"broker_key": "fyers",              "display_name": "Fyers",                  "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 250},
    {"broker_key": "groww",              "display_name": "Groww",                  "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 260},
    {"broker_key": "hdfc_sky",           "display_name": "HDFC Sky",               "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 270},
    {"broker_key": "ibulls",             "display_name": "IBulls",                 "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 280},
    {"broker_key": "iifl",               "display_name": "IIFL",                   "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 290},
    {"broker_key": "iiflcapital",        "display_name": "IIFL Capital",           "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 300},
    {"broker_key": "indmoney",           "display_name": "IndMoney",               "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 310},
    {"broker_key": "jainam_xts",         "display_name": "Jainam XTS",             "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 320},
    {"broker_key": "kotak",              "display_name": "Kotak Securities",       "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 330},
    {"broker_key": "motilal",            "display_name": "Motilal Oswal",          "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 340},
    {"broker_key": "mstock",             "display_name": "mStock",                 "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 350},
    {"broker_key": "nubra",              "display_name": "Nubra",                  "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 360},
    {"broker_key": "paytm",              "display_name": "Paytm Money",            "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 370},
    {"broker_key": "pocketful",          "display_name": "Pocketful",              "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 380},
    {"broker_key": "rmoney",             "display_name": "RMoney",                 "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 390},
    {"broker_key": "samco",              "display_name": "Samco",                  "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 400},
    {"broker_key": "shoonya",            "display_name": "Shoonya",                "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 410},
    {"broker_key": "tradejini",          "display_name": "TradeJini",              "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 420},
    {"broker_key": "tradesmart",         "display_name": "TradeSmart",             "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 430},
    {"broker_key": "wisdom_xts",         "display_name": "Wisdom Capital (XTS)",   "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 440},
    {"broker_key": "zebu",               "display_name": "Zebu",                   "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 450},
    {"broker_key": "compositedge",       "display_name": "CompositEdge",           "badges": ["NSE", "BSE"], "pricing_note": "", "is_active": False, "sort_order": 460},
]

_XTS_FIELDS = [
    _f("api_key", "Interactive API Key"),
    _f("api_secret", "Interactive API Secret"),
    _f("market_data_api_key", "Market Data API Key"),
    _f("market_data_api_secret", "Market Data API Secret"),
]
_XTS_NOTE = ["XTS-family broker — the four API keys are used directly against the broker's XTS API; there's no login redirect."]

BROKER_FIELD_SCHEMA: dict[str, dict] = {
    "dhan":               {"auth_type": "totp_pin", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key"), _f("api_secret", "API Secret")],
                            "login_url_template": None,
                            "instructions": [
                                _i(text="Please go to web.dhan.co and login to your account", href="https://web.dhan.co/", label="web.dhan.co"),
                                _i(text='Click on "My Profile on Dhan" (Top right corner, Under your photo)'),
                                _i(text="Copy your Client ID from under Profile Details, and paste it below"),
                            ]},
    "zerodha":            {"auth_type": "oauth", "fields": [_f("api_key", "API Key"), _f("api_secret", "API Secret")],
                            "login_url_template": "https://kite.zerodha.com/connect/login?v=3&api_key={api_key}", "instructions": [
                            _i(text="Go to Kite Connect's Developer Portal and create a new app. Choose Type as Connect or Personal, enter any App Name you like, and enter your Broker Client ID."),
                            _i(text="Copy the API key and API secret and paste them in below and click Add."),
                        ]},
    "flattrade":          {"auth_type": "oauth", "fields": [_f("api_key", "API Key"), _f("api_secret", "API Secret")],
                            "login_url_template": "https://auth.flattrade.in/?app_key={api_key}", "instructions": [
                            _i(text='Please to go to https://wall.flattrade.in and login to your account', href='https://wall.flattrade.in/', label='https://wall.flattrade.in'),
                            _i(text='Navigate to “Pi” on the menu section of the Page.'),
                            _i(text='Leave the Postback as blank'),
                            _i(text='Copy the App Key and Secret Key and paste them below'),
                        ]},
    "upstox":             {"auth_type": "oauth", "fields": [_f("api_key", "API Key"), _f("api_secret", "API Secret")],
                            "login_url_template": "https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={api_key}&redirect_uri={redirect_uri}&state={api_key}", "instructions": [
                            _i(text='Please to go to login.upstox.com and login to your account.', href='https://login.upstox.com/', label='login.upstox.com'),
                            _i(text='Go to "My Account" section, choose the App tab, then click on New App'),
                            _i(text='Enter any App Name.'),
                            _i(text='Click on App Details. Copy your API Key and Secret, and paste them below.'),
                        ]},
    "5paisa_xstream":     {"auth_type": "oauth", "fields": [_f("client_id", "User ID"), _f("api_key", "Client Code"), _f("api_secret", "API Key"), _f("host_lookup_url", "Encryption Key")],
                            "login_url_template": "https://dev-openapi.5paisa.com/WebVendorLogin/VLogin/Index?VendorKey={vendor_key}&ResponseURL={redirect_uri}",
                            "website_url": "https://www.5paisa.com",
                            "instructions": ["Please go to this link and login to your account.", "Click on profile from top right, copy the client ID.", "Paste the client ID above."],
                            "note": "Requires our own 5paisa API vendor-key partnership to actually go live — the VendorKey AlgoTest uses is theirs, not ours."},
    "5paisa_xts":         {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": [
                            _i(text='If you do not have a 5Paisa account, you can open one here for free . Use this promo code to get ₹2000 worth vouchers from top brands', href='https://5paisa.page.link/pFrBGaxBvHJASyGW8', copy='RAGH4073'),
                            _i(text='If you already have API key & Secret pair, for both the Interactive as well as Market Data, then you can skip these steps, and directly copy/paste those in the respective fields below.'),
                            _i(text='API Activation: Send an email to shrikant.shirke@5paisa.com nisha.thakur@5paisa.com , for activating XTS API and terminal for your account with the following information:', href='mailto:shrikant.shirke@5paisa.com', label='shrikant.shirke@5paisa.com'),
                            _i(text='Keep the mail subject as', copy='XTS API Creation for {UICONFIG.brandName}'),
                            _i(text='Keep the Mail body as', copy='Please get my XTS API created subjected to below details:'),
                            _i(text='Client Name'),
                            _i(text='Client Code'),
                            _i(text='Once you receive the email with API Key details and your login ID and password. Copy the API key and secret pair, and paste them in the respective fields below.'),
                            _i(text='Make sure you do not use the same API key and secret pair to fire trades from anywhere else, otherwise your executions would run in unexpected errors.'),
                            _i(text='Once your XTS account has been made and XTS API been created, you will no longer be able to input any orders from the 5Paisa trading terminal/App. You can still view your live MTM & position detail from the 5Paisa trading terminal/App.'),
                            _i(text="Don't worry, you can still take manual trades from your 5Paisa XTS account, please follow this link to login to your XTS account(For manual XTS web trading)", href='https://xtsmum.5paisa.com/'),
                            _i(text='For any issue in any of the previous step, please contact Mr. Deepak Tripathi:', href='tel:+91-9699054837', label='Mr. Deepak Tripathi'),
                        ]},
    "ac_agrawal":         {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": [
                            _i(text='Please to go to https://www.acagarwal.com and select Trading API from Product and tools tab.', href='https://www.acagarwal.com', label='https://www.acagarwal.com'),
                            _i(text='Click on Request API link and enter Client id, confirm it by entering OTP'),
                            _i(text='Upon submission Agree to the terms and condition column and click submit'),
                            _i(text='You will receive a mail within 24 hours of submission and then can proceed for API key generation'),
                            _i(text='Click on API Link, first link under API Dashboard, as shared in mail and select Trading API from Product and tools tab.', href='https://www.acagarwal.com/', label='as shared in mail'),
                            _i(text="Create profile by entering details. Field marked with '*' are mandatory"),
                            _i(text='Verification mail will be sent on your email id. Click the link in the email to verify your email.'),
                            _i(text="Post successful registration & Email address verification. Click 'Continue to Login'"),
                            _i(text='Login Id & password will be shared over mail'),
                            _i(text='Note : Below steps to be initiated only after Activation of API registration request.'),
                            _i(text='Proceed for API Subscription by clicking API Catalogue'),
                            _i(text="Click on 'Create New Application'. It will ask to Validate your XTS login by putting your XTS Client Id and XTS login password shared with you on mail in (5)"),
                            _i(text='Post validation, subscribe to Interactive and Market Data API'),
                            _i(text="Under the subscription page, please mention below URL in the 'Redirect URL' Box. URL for Marketdata API & Interactive API are mentioned below. Note the URLs are different from the link mentioned in the shared PDF in step 4"),
                            _i(text='MarketData API :', copy='https://developers.symphonyfintech.in/doc/marketdata/'),
                            _i(text='Interactive API :', copy='https://developers.symphonyfintech.in/doc/interactive/'),
                            _i(text='Post subscription Login credentials along with secret key will be shared. User can login to Live Environment using the same.'),
                            _i(text='Copy and paste the API key and secret below, once the App becomes active.'),
                            _i(text='For any issue in any of the previous step, please contact Mr. Yogesh Sharma:', href='tel:+91-6378882400', label='Mr. Yogesh Sharma'),
                        ]},
    "aliceblue":          {"auth_type": "oauth", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key"), _f("api_secret", "API Secret")],
                            "login_url_template": "https://ant.aliceblueonline.com/?appcode={api_key}&state={broker_id}", "instructions": [
                            _i(text='If you do not have a Aliceblue account, you can open one here with following benefits', href='https://aliceblueonline.com/open-account-fill-kyc-request-call-back?C=SHYD1331'),
                            _i(text='BROKERAGE: ₹15 / order'),
                            _i(text='Please to go to https://ant.aliceblueonline.com/ and login to your account', href='https://ant.aliceblueonline.com/', label='https://ant.aliceblueonline.com/'),
                            _i(text='Enter the client id visible on the top right of the screen, below your name.'),
                        ]},
    "angelone":           {"auth_type": "oauth", "fields": [_f("api_key", "API Key")],
                            "login_url_template": "https://smartapi.angelbroking.com/publisher-login?api_key={api_key}&redirect_url={redirect_uri}", "instructions": [
                            _i(text='Please go to this link and login to your account.', href='https://bit.ly/4ktlAu4'),
                            _i(text='Create a new app, and fill out the following details'),
                            _i(text='App Name -'),
                            _i(text='Enter the API key of the generated app below.'),
                        ]},
    "arham":              {"auth_type": "other", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key")], "login_url_template": None, "instructions": [
                            _i(text='Enter your client id'),
                            _i(text='Enter your API Key'),
                        ], "fallback": True},
    "bigul":              {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": [
                            _i(text='Go to https://trading.bigul.co/dashboard#!/login', href='https://trading.bigul.co/dashboard#!/login', label='https://trading.bigul.co/dashboard#!/login'),
                            _i(text="Click 'Create Account' and enter the necessary details to sign up on the API dashboard portal. Keep the user ID the same as your client ID and the password the same as your trading account password for convenience."),
                            _i(text='Post Login create the API account and fill all the information as per the account opening done with Bigul. USER ID is your client code or your trading code need to enter as per the and fill the information as per the bigul trading account. Once you log in for the first time, you will have to validate your trading ID. When it prompts put in your client ID and trading account password to validate your trading account. Once the trading account is validated, you will be able to create a new API app'),
                            _i(text='You will have to create 2 API apps: Interactive API app (Order Management API) & Market data API app'),
                            _i(text='Refer to the link for further information. How to create API apps for Bigul?', href='https://bigul.co/en/wp-content/uploads/2024/02/How-to-activate-API-2.pdf', label='How to create API apps for Bigul'),
                        ]},
    "coinswitch":         {"auth_type": "other", "fields": [_f("api_key", "API Key"), _f("api_secret", "Secret Key")], "login_url_template": None, "instructions": [
                            _i(text='Go to coinswitch.co and log in to your CoinSwitch account.', href='https://coinswitch.co/', label='coinswitch.co'),
                            _i(text='Navigate to API Trading.'),
                            _i(text='Click Generate API Key & Secret.'),
                            _i(text='Copy the generated API Key and API Secret.'),
                            _i(text='Paste the API Key and API Secret in the fields provided here.'),
                        ]},
    "db_international":  {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": _XTS_NOTE},
    "delta_exchange":     {"auth_type": "other", "fields": [_f("api_key", "API Key"), _f("api_secret", "API Secret")], "login_url_template": None,
                            "instructions": [
                            _i(text='Use this link to create an account and avail 10% discount on transaction charges https://www.delta.exchange/?code='),
                            _i(text='Copy and paste your client ID'),
                        ]},
    "delta_exchange_api": {"auth_type": "other", "fields": [_f("api_key", "API Key"), _f("api_secret", "API Secret")], "login_url_template": None,
                            "instructions": ["Login is handled server-side in AlgoTest's source — the exact redirect URL wasn't recoverable from this backup."]},
    "findoc":             {"auth_type": "consent", "fields": [_f("api_key", "Client ID")],
                            "login_url_template": "https://connector-app.odinconnector.co.in/landing/redirect?sAppToken={app_token}&sTwoWayToken={two_way_token}&sPartnerId={partner_id}&oEchoBackObject={state}",
                            "instructions": ["Requires our own ODIN connector partner registration (sAppToken/sTwoWayToken/sPartnerId) — AlgoTest's are theirs, not usable by us."]},
    "findoc_xts":         {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": [
                            _i(text='Go to https://xts.myfindoc.com/dashboard#!/login', href='https://xts.myfindoc.com/dashboard#!/login', label='https://xts.myfindoc.com/dashboard#!/login'),
                            _i(text="Click 'Create Account' and enter the necessary details to sign up on the API dashboard portal. Keep the user ID the same as your client ID and the password the same as your trading account password for convenience."),
                            _i(text='Post Login create the API account and fill all the information as per the account opening done with Findoc. USER ID is your client code or your trading code need to enter as per the and fill the information as per the Findoc trading account. Once you log in for the first time, you will have to validate your trading ID. When it prompts put in your client ID and trading account password to validate your trading account. Once the trading account is validated, you will be able to create a new API app'),
                            _i(text='You will have to create 2 API apps: Interactive API app (Order Management API) & Market data API app'),
                        ]},
    "firstock":           {"auth_type": "other", "fields": [_f("client_id", "Vendor Code"), _f("api_key", "API Key")], "login_url_template": None, "instructions": [
                            _i(text='Set up your API key and Vendor code by logging into key generation and generate a vendor code and API key.', href='https://connect.thefirstock.com/login', label='key generation'),
                            _i(text='Paste the generated vendor code and API key below'),
                        ]},
    "fyers":              {"auth_type": "oauth", "fields": [_f("api_key", "App ID"), _f("api_secret", "Secret ID")],
                            "login_url_template": "https://api-t1.fyers.in/api/v3/generate-authcode?response_type=code&client_id={api_key}&redirect_uri={redirect_uri}&state={broker_id}", "instructions": [
                            _i(text='BROKERAGE: ₹20/order'),
                            _i(text='Go to https://fyers.in/web/api-dashboard/user-apps and create a new app. Fill out the following details', href='https://fyers.in/web/api-dashboard/user-apps', label='https://fyers.in/web/api-dashboard/user-apps'),
                            _i(text='App Icon'),
                            _i(text='App Name'),
                            _i(text='Give all available permissions for the app'),
                            _i(text='Post App Creation, Copy the App ID and the Secret ID of the generated App and paste them here'),
                        ]},
    "groww":              {"auth_type": "other", "fields": [_f("api_key", "Client ID"), _f("totp_token", "TOTP Token")], "login_url_template": None,
                            "instructions": [
                            _i(text='Go to https://groww.in/trade-api/api-keys and generate your API key', href='https://groww.in/trade-api/api-keys', label='https://groww.in/trade-api/api-keys'),
                            _i(text='Click on Generate API key > Generate TOTP token to set up TOTP-based authentication'),
                            _i(text='Copy your API Key TOTP Token and paste it below'),
                        ]},
    "hdfc_sky":           {"auth_type": "other", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key"), _f("api_secret", "API Secret")], "login_url_template": None, "instructions": []},
    "ibulls":             {"auth_type": "other", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key"), _f("api_secret", "API Secret")], "login_url_template": None, "instructions": [], "fallback": True},
    "iifl":               {"auth_type": "consent", "fields": [_f("client_id", "Client ID"), _f("api_secret", "API Secret"), _f("host_lookup_url", "App Key")], "login_url_template": None,
                            "instructions": [
                            _i(text='If you already have API key & Secret pair, for both the Interactive as well as Market Data, then you can skip steps 2 to 7.'),
                            _i(text='F&O Activation: Make sure F&O has been activated for your account. You can check the status here: IIFL F&O Status and activate it from the same link. You can also get in touch with your IIFL RM to activate your F&O', href='https://ttweb.indiainfoline.com/Trade/Dashboard.aspx'),
                            _i(text='Login and Go to Dashboard -> My Account -> My Details -> Trading Preference', href='https://ttweb.indiainfoline.com/Trade/Dashboard.aspx', label='Dashboard'),
                            _i(text='API Activation: Send an email to ttblazesupport@iifl.com for activating XTS API and Blaze terminal for your account with the following information:', href='mailto:ttblazesupport@iifl.com', label='ttblazesupport@iifl.com'),
                            _i(text='Client ID'),
                            _i(text='Registered Name'),
                            _i(text='Mobile Number'),
                            _i(text='PAN Number'),
                            _i(text='Date of Birth'),
                            _i(text='Location(City)'),
                            _i(text='Segments: CM, F&O'),
                            _i(text='Email'),
                            _i(text='Once you receive a welcome mail with Login ID and password from Blaze IIFL, create a new account on Blaze API using this link: Blaze terminal', href='https://ttblaze.iifl.com/dashboard#!/login'),
                            _i(text='After creating the account validate your Trading ID with the Login ID and Password received on the mail in the previous step.'),
                            _i(text='Under My App -> Create new app. You need to create an Interactive Data API as well as Market Data API.'),
                            _i(text='Once created you will get a mail with your API key and secret for both the APIs. The current status of the API will be De-activated.'),
                            _i(text='Once the status of both Interactive and Market Data app changes to Active you can copy the respective API key and secret pair, and paste them below.'),
                            _i(text='Make sure you do not use the same API key and secret pair to fire trades from anywhere else, otherwise your executions would run in unexpected errors.'),
                            _i(text='For any issue in any of the previous step, please contact Mr. Santosh Gupta:', href='tel:+91-8591403350', label='Mr. Santosh Gupta'),
                        ]},
    "iiflcapital":        {"auth_type": "other", "fields": [_f("client_id", "Client ID")], "login_url_template": None, "instructions": [], "fallback": True},
    "indmoney":           {"auth_type": "oauth", "fields": [_f("api_key", "Client ID")], "login_url_template": None,
                            "instructions": ["Uses PKCE OAuth (code_challenge generated server-side) — the exact URL wasn't fully recoverable from this backup."]},
    "jainam_xts":         {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": _XTS_NOTE},
    "kotak":              {"auth_type": "other", "fields": [_f("api_key", "Access Token")], "login_url_template": None,
                            "instructions": ["No login redirect — paste an access token generated from Kotak Neo's own dashboard."]},
    "motilal":            {"auth_type": "oauth", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key"), _f("api_secret", "API Secret")],
                            "login_url_template": "https://invest.motilaloswal.com/OpenAPI/Login.aspx?apikey={api_key}", "instructions": [
                            _i(text='You will need to provide consent to use open API. Go to onlinetrade.motilaloswal.com/emailers/TGS/2022/Mailer/Jun22/14Jun2022/API-consent-Mailer.html and follow the given instructions.', href='https://onlinetrade.motilaloswal.com/emailers/TGS/2022/Mailer/Jun22/14Jun2022/API-consent-Mailer.html'),
                            _i(text='You also need setup TOTP for your account. Learn more here'),
                            _i(text='Copy your API key and Client ID and paste them here'),
                        ]},
    "mstock":             {"auth_type": "other", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key")], "login_url_template": None, "instructions": [
                            _i(text='Login to trade.mstock.com', href='https://trade.mstock.com', label='trade.mstock.com'),
                            _i(text='Expand sidebar from top left near the broker logo, and click on "Trading APIs"'),
                            _i(text='Click on "Generate New API Key"'),
                            _i(text="Enter Application Name as your app's name."),
                            _i(text='Choose API Type as "B"'),
                            _i(text='Keep the validity as 1 year and click on Submit.'),
                            _i(text='Enter your API Key and Client ID in the fields below'),
                        ]},
    "nubra":              {"auth_type": "other", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key"), _f("api_secret", "API Secret")], "login_url_template": None, "instructions": [], "fallback": True},
    "paytm":              {"auth_type": "oauth", "fields": [_f("api_key", "API Key"), _f("api_secret", "API Secret")],
                            "login_url_template": "https://login.paytmmoney.com/merchant-login?apiKey={api_key}&state={api_key}", "instructions": [
                            _i(text='Please to go to developer.paytmmoney.com and click on SIGN IN', href='https://developer.paytmmoney.com', label='developer.paytmmoney.com'),
                            _i(text='Login with Paytm Money Registered Mobile Number/ Email.'),
                            _i(text='Click on Create New App. A single user can generate API keys and Secrets for up to five apps.'),
                            _i(text='Click proceed to create the app.'),
                            _i(text='Click proceed to get the API Key and secret. You can access the API Key and API secret for the app in the dashboard section.'),
                            _i(text='If you have any further questions, refer to the official documentation', href='https://www.paytmmoney.com/blog/how-to-create-api-key-and-secret/', label='official documentation'),
                        ]},
    "pocketful":          {"auth_type": "other", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key"), _f("api_secret", "API Secret")], "login_url_template": None, "instructions": [], "fallback": True},
    "rmoney":             {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": [
                            _i(text='You are required to send a request for XTS API activation to RMoney.'),
                            _i(text='Please connect with your broker to get the interactive API and market data API credentials.'),
                        ]},
    "samco":              {"auth_type": "other", "fields": [_f("client_id", "Client ID"), _f("api_key", "API Key"), _f("api_secret", "API Secret")], "login_url_template": None, "instructions": [], "fallback": True},
    "shoonya":            {"auth_type": "oauth", "fields": [_f("api_key", "Client Code"), _f("api_secret", "Secret Code")],
                            "login_url_template": "https://trade.shoonya.com/OAuthlogin/authorize/oauth?client_id={api_key}", "instructions": [
                            _i(text='Please enter your Shoonya Client ID for broker setup.'),
                        ]},
    "tradejini":          {"auth_type": "oauth", "fields": [_f("api_key", "API Key"), _f("api_secret", "API Secret")],
                            "login_url_template": "https://api.tradejini.com/v2/api-gw/oauth/authorize?client_id={api_key}&redirect_uri={redirect_uri}&response_type=code&scope=general&state={redirect_uri}", "instructions": [
                            _i(text="Login to Tradejini's API portal . If you don't have a account on Tradejini API Portal please create a new account by clicking on signup button.", href='https://api.tradejini.com/developer-portal/main', label="Tradejini's API portal"),
                            _i(text='Click on Create an App.'),
                            _i(text='Leave all other fields blank & Click on Save button.'),
                            _i(text='Copy the API Key and API Secret from there and paste it in below API Key and API Secret fields.'),
                        ]},
    "tradesmart":         {"auth_type": "other", "fields": [_f("api_key", "Client Code")], "login_url_template": None,
                            "instructions": [
                            _i(text='IMPORTANT - Please make sure Authenticator TOTP is setup. Go to TradeSmart Setting and make sure you have setup TOTP by generating QR Code and setting it up in Google Authenticator.', href='https://web.tradesmartonline.in/settings', label='TradeSmart Setting'),
                        ]},
    "wisdom_xts":         {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": [
                            _i(text='If you do not have a Wisdom Capital account, you can open one here', href='https://wisdomcapital.in/referral_chintan/'),
                            _i(text='BENEFITS: ₹999/month for unlimited trading.'),
                            _i(text='Please to go to trade.wisdomcapital.in/dashboard and login to your account', href='https://trade.wisdomcapital.in/dashboard#!/login', label='trade.wisdomcapital.in/dashboard'),
                            _i(text='For any issue in any of the previous step, please contact Trading API Helpline: 01206633215', href='tel:01206633215', label='01206633215'),
                            _i(text='Obtain API key & Secret pair, for both the Interactive as well as Market Data, and copy/paste those in the respective fields below.'),
                        ]},
    "zebu":               {"auth_type": "other", "fields": [_f("client_id", "Vendor Code"), _f("api_key", "API Key")], "login_url_template": None, "instructions": []},
    "compositedge":       {"auth_type": "xts", "fields": _XTS_FIELDS, "login_url_template": None, "instructions": _XTS_NOTE, "fallback": True},
}

BROKER_ICON_OVERRIDES: dict[str, str] = {
    "upstox": "upstox.svg", "5paisa_xstream": "5paisa_xstream.svg", "5paisa_xts": "5paisa_xts.svg",
    "ac_agrawal": "ac_agrawal.svg", "aliceblue": "aliceblue.svg", "angelone": "angelone.svg",
    "arham": "arham.svg", "bigul": "bigul.svg", "coinswitch": "coinswitch.svg",
    "db_international": "db_international.png", "delta_exchange": "delta_exchange.svg",
    "delta_exchange_api": "delta_exchange_api.svg", "findoc": "findoc.svg", "findoc_xts": "findoc_xts.svg",
    "firstock": "firstock.svg", "fyers": "fyers.svg", "groww": "groww.svg", "hdfc_sky": "hdfc_sky.svg",
    "iifl": "iifl.svg", "iiflcapital": "iiflcapital.svg", "indmoney": "indmoney.svg",
    "jainam_xts": "jainam_xts.svg", "kotak": "kotak.svg", "motilal": "motilal.svg", "mstock": "mstock.svg",
    "paytm": "paytm.svg", "rmoney": "rmoney.svg", "shoonya": "shoonya.svg", "tradejini": "tradejini.svg",
    "tradesmart": "tradesmart.svg", "wisdom_xts": "wisdom_xts.svg", "zebu": "zebu.svg",
}

for _entry in BROKER_SETUP_SEED:
    _schema = BROKER_FIELD_SCHEMA.get(_entry["broker_key"])
    if _schema:
        _entry.update(_schema)
    _icon = BROKER_ICON_OVERRIDES.get(_entry["broker_key"])
    if _icon:
        _entry["broker_icon"] = _icon


# ── broker-setup catalog ────────────────────────────────────────────────────

@router.get("/broker-setup")
def list_broker_setup(q: str = "", active_only: bool = False) -> dict:
    mongo = get_mongo()
    col = mongo.raw["broker_setup"]
    query: dict = {}
    if active_only:
        query["is_active"] = True
    if q.strip():
        query["display_name"] = {"$regex": re.escape(q.strip()), "$options": "i"}
    catalog = list(col.find(query, {"_id": 0}).sort("sort_order", 1))

    counts: dict[str, int] = {}
    for doc in mongo.raw["broker_configuration"].find({}, {"broker_name": 1}):
        key = str(doc.get("broker_name") or "").strip().lower()
        if key:
            counts[key] = counts.get(key, 0) + 1

    records = [
        {**item, "apis_setup": counts.get(str(item.get("broker_key") or "").strip().lower(), 0)}
        for item in catalog
    ]
    return {"success": True, "count": len(records), "records": records}


@router.get("/broker-setup/{broker_key}")
def get_broker_setup(broker_key: str) -> dict:
    mongo = get_mongo()
    doc = mongo.raw["broker_setup"].find_one({"broker_key": broker_key.strip().lower()}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail=f"Unknown broker_key: {broker_key}")
    return {"success": True, "record": doc}


_BROKER_SETUP_SAVE_FIELDS = {"display_name", "broker_icon", "badges", "pricing_note", "is_active", "sort_order"}


@router.post("/broker-setup/save")
def save_broker_setup(payload: dict) -> dict:
    broker_key = str(payload.get("broker_key") or "").strip().lower()
    if not broker_key:
        raise HTTPException(status_code=400, detail="broker_key is required")

    fields: dict = {k: v for k, v in payload.items() if k in _BROKER_SETUP_SAVE_FIELDS}
    fields["broker_key"] = broker_key
    fields["updated_at"] = datetime.now(timezone.utc).isoformat()

    mongo = get_mongo()
    col = mongo.raw["broker_setup"]
    col.create_index("broker_key", unique=True)
    existing = col.find_one({"broker_key": broker_key})
    if not existing:
        fields["created_at"] = fields["updated_at"]
    col.update_one({"broker_key": broker_key}, {"$set": fields}, upsert=True)

    return {"success": True, "action": "updated" if existing else "created", "broker_key": broker_key}


@router.delete("/broker-setup/{broker_key}")
def delete_broker_setup(broker_key: str) -> dict:
    mongo = get_mongo()
    result = mongo.raw["broker_setup"].delete_one({"broker_key": broker_key.strip().lower()})
    return {"success": True, "deleted": result.deleted_count}


@router.post("/broker-setup/seed")
def seed_broker_setup() -> dict:
    """Idempotent: upserts BROKER_SETUP_SEED by broker_key. Never overwrites
    `instructions` for a key that already has some (Mongo owns that content
    once set, same as the old route)."""
    mongo = get_mongo()
    col = mongo.raw["broker_setup"]
    col.create_index("broker_key", unique=True)
    now = datetime.now(timezone.utc).isoformat()
    upserted = 0
    for entry in BROKER_SETUP_SEED:
        fields = {**entry, "updated_at": now}
        existing = col.find_one({"broker_key": entry["broker_key"]}, {"instructions": 1})
        if existing and existing.get("instructions"):
            fields.pop("instructions", None)
        result = col.update_one(
            {"broker_key": entry["broker_key"]},
            {"$set": fields, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
        if result.upserted_id or result.modified_count:
            upserted += 1
    return {"success": True, "seeded": len(BROKER_SETUP_SEED), "upserted": upserted}


# ── broker-credentials (per-user, dynamic per-broker Setup page) ───────────

def _mirror_dhan_credentials(fields: dict, app_user_id: str) -> None:
    set_fields: dict = {}
    if fields.get("client_id"):
        set_fields["user_id"] = fields["client_id"]
    for key in ("api_key", "api_secret", "totp_token", "pin"):
        if fields.get(key):
            set_fields["totp" if key == "totp_token" else key] = fields[key]
    if not set_fields:
        return
    set_fields["broker"] = "dhan"
    set_fields["app_user_id"] = app_user_id
    get_mongo().raw["kite_market_config"].update_one({"broker": "dhan"}, {"$set": set_fields}, upsert=True)


@router.get("/broker-credentials")
def list_broker_credentials(broker_key: str = "", current_user: dict = Depends(get_current_user)) -> dict:
    mongo = get_mongo()
    query: dict = {"app_user_id": str(current_user["_id"])}
    normalized_broker = str(broker_key or "").strip().lower()
    if normalized_broker:
        query["broker_key"] = normalized_broker

    schema_by_broker = {e["broker_key"]: e for e in BROKER_SETUP_SEED}
    records = []
    for doc in mongo.raw["broker_credentials"].find(query):
        broker_key_val = str(doc.get("broker_key") or "").strip().lower()
        schema = schema_by_broker.get(broker_key_val, {})
        masked_keys = {f["key"] for f in schema.get("fields", []) if f.get("masked")}
        stored_fields = doc.get("fields") or {}
        out_fields: dict[str, Any] = {}
        for k, v in stored_fields.items():
            if k in masked_keys:
                out_fields[f"has_{k}"] = bool(str(v or "").strip())
            else:
                out_fields[k] = v
        records.append({
            "_id":         str(doc.get("_id") or ""),
            "broker_key":  broker_key_val,
            "alias":       str(doc.get("alias") or "").strip(),
            "fields":      out_fields,
            "enabled":     bool(doc.get("enabled") or False),
            "app_user_id": str(doc.get("app_user_id") or "").strip(),
            "updated_at":  str(doc.get("updated_at") or "").strip(),
        })
    return {"success": True, "count": len(records), "records": records}


@router.post("/broker-credentials/save")
def save_broker_credentials(payload: dict, current_user: dict = Depends(get_current_user)) -> dict:
    user_id = str(current_user["_id"])
    doc_id = str(payload.get("_id") or "").strip()
    broker_key = str(payload.get("broker_key") or "").strip().lower()
    raw_fields = payload.get("fields") or {}
    if not isinstance(raw_fields, dict):
        raise HTTPException(status_code=400, detail="fields must be an object")
    if not doc_id and not broker_key:
        raise HTTPException(status_code=400, detail="broker_key is required when creating new credentials")

    mongo = get_mongo()
    col = mongo.raw["broker_credentials"]
    col.create_index([("app_user_id", 1), ("broker_key", 1)], unique=True)

    if doc_id:
        query: dict = {"_id": ObjectId(doc_id), "app_user_id": user_id}
        existing = col.find_one(query)
        if not existing:
            raise HTTPException(status_code=404, detail="Broker credential not found")
        if not broker_key:
            broker_key = str(existing.get("broker_key") or "")
    else:
        query = {"broker_key": broker_key, "app_user_id": user_id}
        existing = col.find_one(query)

    schema = mongo.raw["broker_setup"].find_one({"broker_key": broker_key}) or \
        next((e for e in BROKER_SETUP_SEED if e["broker_key"] == broker_key), None)
    if not schema:
        raise HTTPException(status_code=404, detail=f"Unknown broker_key: {broker_key}")
    allowed_keys = {f["key"] for f in (schema.get("fields") or [])}

    clean_fields = {k: str(v or "").strip() for k, v in raw_fields.items() if k in allowed_keys}
    merged_fields = {**(existing or {}).get("fields", {}), **{k: v for k, v in clean_fields.items() if v}}

    set_fields: dict = {
        "broker_key":  broker_key,
        "fields":      merged_fields,
        "app_user_id": user_id,
        "updated_at":  datetime.now(timezone.utc).isoformat(),
    }
    if "alias" in payload:
        set_fields["alias"] = str(payload.get("alias") or "").strip()
    if "enabled" in payload:
        set_fields["enabled"] = bool(payload.get("enabled"))
    if not existing:
        set_fields["created_at"] = set_fields["updated_at"]

    try:
        col.update_one(query, {"$set": set_fields}, upsert=True)
    except DuplicateKeyError:
        existing = col.find_one(query)
    saved_doc = col.find_one(query) or {}

    if broker_key == "dhan":
        _mirror_dhan_credentials(merged_fields, user_id)

    return {"success": True, "action": "updated" if existing else "created", "_id": str(saved_doc.get("_id") or "")}


@router.delete("/broker-credentials/{doc_id}")
def delete_broker_credentials(doc_id: str, current_user: dict = Depends(get_current_user)) -> dict:
    mongo = get_mongo()
    result = mongo.raw["broker_credentials"].delete_one({"_id": ObjectId(doc_id), "app_user_id": str(current_user["_id"])})
    return {"success": True, "deleted": result.deleted_count}
