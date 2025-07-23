import click
from bilibili_api import bvid2aid
from bilibili_api.comment import CommentResourceType
from ..database import ReplyDatabase, MemberDatabase, VideoDatabase, RawDatabase
from ..parse import ReplyParser, MemberParser, VideoParser


@click.argument("bvid", type=str)
@click.command(help="Parse comments from video with given BVID")
def parse(bvid):
    """Parse comments from video with given BVID"""

    raw_db = RawDatabase("bilianalyzer.db")
    video_db = VideoDatabase("bilianalyzer.db")
    member_db = MemberDatabase("bilianalyzer.db")
    reply_db = ReplyDatabase("bilianalyzer.db", member_db)

    raw_video = raw_db.load_raw_video_by_bvid(bvid)
    raw_replies = raw_db.load_raw_reply_by_resource(
        bvid2aid(bvid), CommentResourceType.VIDEO
    )

    if not raw_video and not raw_replies:
        print(f"No raw video or replies found for BVID {bvid}.")
        print(f"Please run 'uv run -m bilianalyzer fetch {bvid} -r' first")
        return

    video_parser = VideoParser()
    member_parser = MemberParser()
    reply_parser = ReplyParser(member_parser)

    if raw_video is not None:
        video = video_parser.parse_from_api(raw_video)
        video_db.save_video(video)
        print(f"Successfully parsed video {bvid} from stored raw data.")

    if len(raw_replies) != 0:
        replies = list(reply_parser.batch_parse_from_api(raw_replies))
        members = list(member_parser.unroll_members(replies))

        reply_db.save_replies(replies)
        member_db.save_members(members)

        print(f"Successfully parsed {len(replies)} raw replies from stored raw data.")

    print("Replies have been saved to the database.")
