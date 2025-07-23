from typing import Optional
from collections import Counter
from collections.abc import Collection

from .. import Member, Reply, Video


class MemberAnalyzer:
    def __init__(self, members: Collection[Member]):
        self.members: Collection[Member] = members

    def analyze_uid_lengths(self) -> Counter[int]:
        return Counter(len(str(member.uid)) for member in self.members)

    def analyze_levels(self) -> Counter[int]:
        levels: Counter[int] = Counter()
        for member in self.members:
            if member.level is None:
                continue
            levels[member.level] += 1
        return levels

    def analyze_vips(self) -> Counter[str]:
        vips: Counter[str] = Counter()
        for member in self.members:
            if member.vip is None:
                continue
            vips[member.vip] += 1
        return vips

    def analyze_sexes(self) -> Counter[str]:
        sexes: Counter[str] = Counter()
        for member in self.members:
            if member.sex is None:
                sexes["保密"] += 1
            else:
                sexes[member.sex] += 1
        return sexes

    # TODO: refactor pendants and cardbags
    def analyze_pendants(self) -> Counter[str]:
        # NOTE: pendant 表示头像框，叠加在头像上
        pendants: Counter[str] = Counter()
        for member in self.members:
            if member.pendant is None:
                continue
            else:
                pendants[member.pendant] += 1
        return pendants

    # TODO: refactor pendants and cardbags
    def analyze_cardbags(self) -> Counter[str]:
        # NOTE: cardbag 表示数字周边，出现在评论右侧
        cardbags: Counter[str] = Counter()
        for member in self.members:
            if member.cardbag is None:
                continue
            else:
                cardbags[member.cardbag] += 1
        return cardbags

    # TODO: readd fans medal
    """
    def analyze_fans(self) -> tuple[str, int, Counter[int]]:
        # TODO: 用更好的方式返回值
        fans_name: str = "未知粉丝团"
        fans_count: int = 0
        fans_levels: Counter[int] = Counter()

        for member in self.members:
            if member.get("fans_detail") is None:
                continue
            if fans_name == "未知粉丝团":
                fans_name = member["fans_detail"].get("medal_name", "未知粉丝团").strip()
            fans_level: int = member["fans_detail"].get("level", 0)
            fans_levels[fans_level] += 1
            fans_count += 1
        return fans_name, fans_count, fans_levels
    """


class ReplyAnalyzer:
    def __init__(self, replies: Collection[Reply]):
        self.replies: Collection[Reply] = replies

    def analyze_locations(self) -> Counter[str]:
        locations: Counter[str] = Counter()
        for reply in self.replies:
            location = reply.location
            if location is not None:
                locations[location] += 1
        return locations


class CommentAnalyzer(MemberAnalyzer, ReplyAnalyzer):
    def __init__(
        self,
        video: Video,
        members: Collection[Member],
        replies: Collection[Reply],
    ):
        self.video: Video = video
        MemberAnalyzer.__init__(self, members)
        ReplyAnalyzer.__init__(self, replies)

    @staticmethod
    def _calc_interval_name(start_time: int, end_time: int) -> str:

        interval_hours: float = (end_time - start_time) / 3600.0
        INTERVAL_POINTS: list[float] = [
            0.0,
            0.5,
            1.0,
            2.0,
            3.0,
            6.0,
            12.0,
            24.0,
            48.0,
            72.0,
        ]
        INTERVAL_NAMES: list[str] = [
            # NOTE: "超时空评论" is just for fun and in case of special cases
            "超时空评论",
            "半小时内",
            "0.5-1小时内",
            "1-2小时内",
            "2-3小时内",
            "3-6小时内",
            "6-12小时内",
            "12-24小时内（1天内）",
            "24-48小时内（2天内）",
            "48-72小时内（3天内）",
            "3天以上",
        ]
        for interval_stop, interval_name in zip(INTERVAL_POINTS, INTERVAL_NAMES):
            if interval_hours < interval_stop:
                return interval_name
        return INTERVAL_NAMES[-1]

    def analyze_comment_intervals(self) -> Counter[str]:

        publish_time: int = self.video.publish_time
        comment_intervals: Counter[str] = Counter()

        for reply in self.replies:
            comment_time: Optional[int] = reply.ctime
            if comment_time is None:
                continue
            comment_intervals[self._calc_interval_name(publish_time, comment_time)] += 1

        return comment_intervals

    def generate_analysis(self):
        video: Video = self.video
        reply_count: int = len(self.replies)
        member_count: int = len(self.members)
        uid_lengths: Counter[int] = self.analyze_uid_lengths()
        levels: Counter[int] = self.analyze_levels()
        vips: Counter[str] = self.analyze_vips()
        sexes: Counter[str] = self.analyze_sexes()
        # TODO: refactor pendants and cardbags
        # TODO: readd fans medal
        pendants: Counter[str] = self.analyze_pendants()
        cardbags: Counter[str] = self.analyze_cardbags()
        locations: Counter[str] = self.analyze_locations()
        comment_intervals: Counter[str] = self.analyze_comment_intervals()
        analysis = {
            "bvid": video.bvid,
            "title": video.title,
            "description": video.description,
            "publish_time": video.publish_time,
            "upload_time": video.upload_time,
            "reply_count": reply_count,
            "member_count": member_count,
            "uid_lengths": uid_lengths,
            "levels": levels,
            "vips": vips,
            "sexes": sexes,
            "pendants": pendants,
            "cardbags": cardbags,
            # TODO: refactor pendants and cardbags
            # TODO: readd fans medal
            "locations": locations,
            "comment_intervals": comment_intervals,
        }
        return analysis


# DEBUG:
"""
def save_analysis(results: Analysis, filepath: FilePath) -> None:
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
"""
