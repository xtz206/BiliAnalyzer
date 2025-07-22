import click
from bilibili_api import bvid2aid
from bilibili_api.comment import CommentResourceType
from ..database import ReplyDatabase, MemberDatabase, RawDatabase
from ..parse import ReplyParser, MemberParser


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

    member_parser = MemberParser()
    reply_parser = ReplyParser(member_parser)
    replies = list(reply_parser.batch_parse_from_api(raw_replies))
    members = list(member_parser.unroll_members(replies))
    reply_db.save_replies(replies)
    member_db.save_members(members)

    print(f"Successfully parsed {len(replies)} raw replies from stored raw data.")
    print("Replies have been saved to the database.")
