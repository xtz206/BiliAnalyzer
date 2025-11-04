import click
from bilibili_api import bvid2aid
from bilibili_api.comment import CommentResourceType
from ..analyze.comments import CommentAnalyzer
from ..database import ReplyDatabase, MemberDatabase, VideoDatabase
from ..parse import ReplyParser, MemberParser, VideoParser


# TODO: add type hint for command
@click.argument("bvid", type=str)
@click.option(
    "-o",
    "--output",
    type=str,
    default=None,
    help="Output filepath for Analysis",
)
@click.command(help="Analyze comments from video with given BVID")
def analyze(bvid, output):
    """Analyze comments from video with given BVID"""

    video_parser = VideoParser()
    member_parser = MemberParser()
    reply_parser = ReplyParser(member_parser)

    video_db = VideoDatabase("bilianalyzer.db", video_parser)
    member_db = MemberDatabase("bilianalyzer.db", member_parser)
    reply_db = ReplyDatabase("bilianalyzer.db", member_db, reply_parser)

    video = video_db.load_video_by_bvid(bvid)
    if video is None:
        print(f"No video found for BVID {bvid}.")
        print(f"Please run 'uv run -m bilianalyzer fetch {bvid}' first")
        return

    replies = reply_db.load_replies_by_resource(bvid2aid(bvid), CommentResourceType.VIDEO)
    members = list(member_parser.unroll_members(replies))
    analyzer = CommentAnalyzer(video, members, replies)
    analysis = analyzer.get_analysis()

    print("=" * 40)
    print("BiliAnalyzer 评论分析报告")
    print("=" * 40)
    print()

    def print_dist(title, dist, unit="个", top=5):
        print(f"{title}:")
        if isinstance(dist, dict):
            items = list(dist.items())
        elif hasattr(dist, "most_common"):
            items = dist.most_common(top)
        else:
            items = []
        if len(items) == 0:
            print("无数据")
        else:
            for k, v in items[:top]:
                print(f"  {k}: {v} {unit}")
            if len(items) > top:
                print("  ...")
        print()

    print(f"视频BVID:", analysis["bvid"])
    print(f"视频标题:", analysis["title"])
    print(f"视频描述:")
    print(analysis["description"] or "无描述")
    print(f"视频发布时间:", analysis["publish_time"])
    print(f"视频上传时间:", analysis["upload_time"])
    print(f"评论总数:", analysis["reply_count"])
    print(f"参与评论的用户数:", analysis["member_count"])
    print()

    print_dist("用户UID位数分布", analysis["uid_lengths"], "次")
    print_dist("用户等级分布", analysis["levels"], "个")
    print_dist("用户大会员分布", analysis["vips"], "个")
    print_dist("用户性别分布", analysis["sexes"], "个")
    print()

    # TODO: refactor pendants and cardbags
    # TODO: readd fans medal
    print_dist("用户头像框分布", analysis["pendants"], "次")
    print_dist("用户数字周边分布", analysis["cardbags"], "次")

    print_dist("评论IP属地分布", analysis["locations"], "次")
    print_dist("评论发布时间分布", analysis["comment_intervals"], "次")

    print("=" * 40)

    if output is not None:
        analyzer.save_analysis(output)
        print(f"分析结果已保存至{output}")
