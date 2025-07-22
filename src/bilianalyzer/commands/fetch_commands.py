import click
from bilibili_api import Credential, sync
from ..auth import load_credential
from ..fetch.comments import Fetcher
from ..database import RawDatabase, ReplyDatabase, MemberDatabase
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
    "-r",
    "--raw",
    is_flag=True,
    help="Store raw replies in the database additionally",
)
@click.option(
    "--no-auth",
    is_flag=True,
    help="Skip authentication and fetch comments without credentials",
)
@click.command(help="Fetch comments for a video with given BVID")
def fetch(bvid, limit, raw, no_auth):
    """Fetch comments for a video with given BVID"""

    credential: Credential = Credential()
    if not no_auth:
        try:
            credential = load_credential()
        except ValueError as error:
            print(f"Authentication Failed: {error}")
            return
    fetcher = Fetcher(bvid, credential)
    raw_db = RawDatabase("bilianalyzer.db")
    member_db = MemberDatabase("bilianalyzer.db")
    reply_db = ReplyDatabase("bilianalyzer.db", member_db)

    raw_replies = sync(fetcher.fetch_raw_replies(limit=limit))
    if raw:
        raw_db.save_raw_replies(raw_replies)
    replies = fetcher.parse_raw_replies(raw_replies)
    members = list(MemberParser.unroll_members(replies))
    reply_db.save_replies(replies)
    member_db.save_members(members)
