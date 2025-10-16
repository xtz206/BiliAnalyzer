import click
from ..auth import check, login_from_cookies, remove_credential


@click.group()
def auth():
    """Authenticate BiliAnalyzer with Bilibili Cookies"""


@auth.command()
def status():
    """Check Authentication Status of BiliAnalyzer"""
    click.echo(check())


@auth.command()
def login():
    """Login and Store Cookies for BiliAnalyzer"""
    sessdata: str = click.prompt("Please enter your sessdata cookie for bilibili")
    bili_jct: str = click.prompt("Please enter your bili_jct cookie for bilibili")
    try:
        credential = login_from_cookies(sessdata=sessdata, bili_jct=bili_jct)
    except ValueError as error:
        click.echo(f"Login Failed: {error}")
        return
    click.echo("BiliAnalyzer Logged In Successfully")


@auth.command()
def logout():
    """Logout BiliAnalyzer and Remove Stored Cookies"""
    remove_credential()
    click.echo("BiliAnalyzer Logged Out Successfully")
