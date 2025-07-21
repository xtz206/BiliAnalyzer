import json
import os

from typing import TypedDict

from bilibili_api import Credential, sync


class Cookies(TypedDict):
    sessdata: str
    bili_jct: str


def check() -> str:
    # TODO: implement a better way to indicate authentication status
    # Check if the credential file exists
    if not os.path.exists("credential.json"):
        return "Authentication File Not Found. Please Login First."

    try:
        credential = load_credential()
    except ValueError as e:
        return str(e)

    if not sync(credential.check_valid()):
        return "Authentication Expired. Please Login Again."

    return "BiliAnalyzer Authenticated Successfully"


def login_from_cookies(sessdata: str, bili_jct: str) -> Credential:
    credential = Credential(sessdata=sessdata, bili_jct=bili_jct)
    if not sync(credential.check_valid()):
        raise ValueError("Invalid Credential: Please Check Your Cookies")

    save_credential(credential)

    return credential


def login_from_file(filepath) -> Credential:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Credential File {filepath} Not Found")

    with open(filepath, "r") as f:
        cookies = json.load(f)
    if "sessdata" not in cookies:
        raise ValueError("Credential File Invalid: Missing 'sessdata' Cookie")
    if "bili_jct" not in cookies:
        raise ValueError("Credential File Invalid: Missing 'bili_jct' Cookie")

    cookies: Cookies = {
        "sessdata": cookies["sessdata"],
        "bili_jct": cookies["bili_jct"],
    }
    return login_from_cookies(**cookies)


def load_credential() -> Credential:
    if not os.path.exists("credential.json"):
        return Credential()

    with open("credential.json", "r") as f:
        cookies = json.load(f)
    if "sessdata" not in cookies:
        raise ValueError("Credential File Invalid: Missing 'sessdata' Cookie")
    if "bili_jct" not in cookies:
        raise ValueError("Credential File Invalid: Missing 'bili_jct' Cookie")

    cookies: Cookies = {
        "sessdata": cookies["sessdata"],
        "bili_jct": cookies["bili_jct"],
    }
    return login_from_cookies(**cookies)


def save_credential(credential: Credential) -> None:
    cookies = {"sessdata": credential.sessdata, "bili_jct": credential.bili_jct}
    with open("credential.json", "w") as f:
        json.dump(cookies, f)


def remove_credential() -> None:
    if os.path.exists("credential.json"):
        os.remove("credential.json")
