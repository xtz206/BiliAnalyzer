import click
from bilibili_api import bvid2aid
from bilibili_api.comment import CommentResourceType
from ..database import ReplyDatabase, MemberDatabase, RawDatabase
from ..parse import ReplyParser


@click.argument("bvid", type=str)
@click.command(help="Parse comments from video with given BVID")
def parse(bvid):
    """Parse comments from video with given BVID"""

    raw_db = RawDatabase("bilianalyzer.db")
    member_db = MemberDatabase("bilianalyzer.db")
    reply_db = ReplyDatabase("bilianalyzer.db", member_db)

    raw_replies = raw_db.load_raw_reply_by_resource(
        bvid2aid(bvid), CommentResourceType.VIDEO
    )

    if not raw_replies:
        print(f"No raw replies found for BVID {bvid}.")
        print(f"Please run 'uv run -m bilianalyzer fetch {bvid} -r' first")
        return

    replies = [ReplyParser.parse_from_api(raw_reply) for raw_reply in raw_replies]
    reply_db.save_replies(replies)

    print(f"Successfully parsed {len(replies)} replies from stored raw data.")
    print("Replies have been saved to the database.")
