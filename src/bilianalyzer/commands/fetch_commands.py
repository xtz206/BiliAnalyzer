import click
from bilibili_api import Credential, sync
from ..auth import load_credential
from ..fetch.comments import ReplyFetcher
from ..fetch.videos import VideoFetcher
from ..database import ReplyDatabase, MemberDatabase, VideoDatabase, RawDatabase
from ..parse import MemberParser, ReplyParser, VideoParser


# TODO: add type hint for command
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
    help="Only fetch and store raw data without parsing",
)
@click.option(
    "--no-raw",
    is_flag=True,
    help="Parse and store data, but do not store raw data",
)
@click.option(
    "--no-auth",
    is_flag=True,
    help="Skip authentication and fetch comments without credentials",
)
@click.command(help="Fetch comments for a video with given BVID")
def fetch(bvid, limit, raw, no_raw, no_auth):
    """Fetch comments for a video with given BVID"""

    if raw and no_raw:
        raise click.UsageError("Options '--raw' and '--no-raw' are mutually exclusive.")

    credential: Credential = Credential()
    if not no_auth:
        try:
            credential = load_credential()
        except ValueError as error:
            print(f"Authentication Failed: {error}")
            return

    # parsers
    video_parser = VideoParser()
    member_parser = MemberParser()
    reply_parser = ReplyParser(member_parser)

    # databases
    raw_db = RawDatabase("bilianalyzer.db")
    video_db = VideoDatabase("bilianalyzer.db", video_parser)
    member_db = MemberDatabase("bilianalyzer.db")
    reply_db = ReplyDatabase("bilianalyzer.db", member_db)

    # fetchers
    if raw:
        video_fetcher = VideoFetcher(bvid, credential, video_parser, raw_db=raw_db)
        reply_fetcher = ReplyFetcher(bvid, credential, reply_parser, raw_db=raw_db)
    elif no_raw:
        video_fetcher = VideoFetcher(bvid, credential, video_parser, video_db=video_db)
        reply_fetcher = ReplyFetcher(bvid, credential, reply_parser, reply_db=reply_db)
    else:
        video_fetcher = VideoFetcher(bvid, credential, video_parser, video_db, raw_db)
        reply_fetcher = ReplyFetcher(bvid, credential, reply_parser, reply_db, raw_db)

    # fetch and (if needed) store
    sync(video_fetcher.fetch_video())
    sync(reply_fetcher.fetch_replies(limit=limit))
    