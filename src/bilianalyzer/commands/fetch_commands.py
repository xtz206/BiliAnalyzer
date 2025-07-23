import click
from bilibili_api import Credential, sync
from ..auth import load_credential
from ..fetch.comments import ReplyFetcher
from ..fetch.videos import VideoFetcher
from ..database import RawDatabase, ReplyDatabase, MemberDatabase, VideoDatabase
from ..parse import MemberParser, ReplyParser, VideoParser


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

    # parsers
    video_parser = VideoParser()
    member_parser = MemberParser()
    reply_parser = ReplyParser(member_parser)

    # fetchers
    video_fetcher = VideoFetcher(bvid, credential, video_parser)
    reply_fetcher = ReplyFetcher(bvid, credential, reply_parser)

    # databases
    raw_db = RawDatabase("bilianalyzer.db")
    video_db = VideoDatabase("bilianalyzer.db", video_parser)
    member_db = MemberDatabase("bilianalyzer.db")
    reply_db = ReplyDatabase("bilianalyzer.db", member_db)

    raw_video = sync(video_fetcher.fetch_raw_video())
    raw_replies = sync(reply_fetcher.fetch_raw_replies(limit=limit))

    if raw:
        raw_db.save_raw_replies(raw_replies)
        raw_db.save_raw_video(raw_video)

    video = video_parser.parse_from_api(raw_video)
    replies = reply_parser.batch_parse_from_api(raw_replies)
    members = list(member_parser.unroll_members(replies))

    video_db.save_video(video)
    reply_db.save_replies(replies)
    member_db.save_members(members)
