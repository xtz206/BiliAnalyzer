import click
from bilibili_api import Credential, sync
from ..auth import load_credential
from ..fetch.comments import Fetcher
from ..database import Database
from ..parse import MemberParser


@click.argument("bvid", type=str)
@click.option(
    "-n",
    "--limit",
    type=int,
    default=10,
    help="Limit the maximum number of pages to fetch (default: 10)",
)
@click.option(
    "--no-auth",
    is_flag=True,
    help="Skip authentication and fetch comments without credentials",
)
@click.command(help="Fetch comments for a video with given BVID")
def fetch(bvid, limit, no_auth):
    """Fetch comments for a video with given BVID"""

    credential: Credential = Credential()
    if not no_auth:
        try:
            credential = load_credential()
        except ValueError as error:
            print(f"Authentication Failed: {error}")
            return
    fetcher = Fetcher(bvid, credential)
    database = Database("bilianalyzer.db")
    replies = sync(fetcher.fetch_replies(limit=limit))
    members = list(MemberParser.unroll_members(replies))
    database.save_replies(replies)
    database.save_members(members)
