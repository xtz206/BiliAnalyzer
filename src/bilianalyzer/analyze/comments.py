import json
import os
from collections import Counter
from collections.abc import Collection

from .. import Member, Reply


class MemberAnalyzer:
    def __init__(self, members: Collection[Member]):
        self.members: Collection[Member] = members

    def analyze_uid_lengths(self) -> Counter[int]:
        return Counter(len(member["mid"]) for member in self.members)

    def analyze_levels(self) -> Counter[int]:
        levels: Counter[int] = Counter()
        for member in self.members:
            if member.get("is_senior_member"):
                levels[7] += 1
            else:
                level: int = member["level_info"].get("current_level", 0)
                levels[level] += 1
        return levels

    def analyze_vips(self) -> Counter[str]:
        vips: Counter[str] = Counter()

        for member in self.members:
            if member["vip"]["vipStatus"] == 0:
                vips["非大会员"] += 1
            else:
                vips[member["vip"]["label"]["text"]] += 1

        return vips

    def analyze_sexes(self) -> Counter[str]:
        return Counter(member.get("sex", "保密") for member in self.members)

    def analyze_pendants(self) -> Counter[str]:
        # NOTE: pendant 表示头像框，叠加在头像上
        pendants: Counter[str] = Counter()
        for member in self.members:
            if member.get("pendant") is None:
                continue
            pendant: str = member["pendant"].get("name", "").strip()
            if pendant:
                pendants[pendant] += 1
        return pendants

    def analyze_cardbags(self) -> Counter[str]:
        # NOTE: cardbg 表示数字周边，出现在评论右侧
        cardbags: Counter[str] = Counter()
        for member in self.members:
            if member.get("user_sailing") is None:
                continue
            if member["user_sailing"].get("cardbg") is None:
                continue
            cardbag: str = member["user_sailing"]["cardbg"].get("name", "").strip()
            if cardbag:
                cardbags[cardbag] += 1
        return cardbags

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


class ReplyAnalyzer:
    def __init__(self, replies: Collection[Reply]):
        self.replies: Collection[Reply] = replies

    def analyze_locations(self) -> Counter[str]:
        prefix: str = "IP属地："
        locations: Counter[str] = Counter()
        for reply in self.replies:
            if "reply_control" not in reply:
                continue
            if "location" not in reply["reply_control"]:
                continue
            location: str = reply["reply_control"]["location"]
            if location.startswith(prefix):
                locations[location[len(prefix) :]] += 1
        return locations


class CommentAnalyzer(MemberAnalyzer, ReplyAnalyzer):
    def __init__(
        self,
        video_info: VideoInfo,
        replies: Collection[Reply],
    ):
        members = self._get_members(replies)
        MemberAnalyzer.__init__(self, members)
        ReplyAnalyzer.__init__(self, replies)
        self.video_info: VideoInfo = video_info

    @staticmethod
    def _get_members(replies: Collection[Reply]) -> Collection[Member]:
        members: list[Member] = []
        uids: dict[str, Member] = {}
        for reply in replies:
            member: Member = reply.get("member", {})
            uid: str = member.get("mid", "")
            if uid is None:
                continue
            if uid not in uids:
                uids[uid] = member
                members.append(member)
            else:
                # Update existing member with new information
                existing_member = uids[uid]
                for key, value in member.items():
                    if key not in existing_member or not existing_member[key]:
                        existing_member[key] = value
                uids[uid] = existing_member
        return members

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
            "超时空评论",  # NOTE: just kidding
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

        publish_time: int = self.video_info.get("pubdate", 0)
        comment_intervals: Counter[str] = Counter()

        for reply in self.replies:
            comment_time: int | None = reply.get("ctime")
            if comment_time is None:
                continue
            comment_intervals[self._calc_interval_name(publish_time, comment_time)] += 1

        return comment_intervals

    def generate_analysis(self) -> Analysis:
        video_info: VideoInfo = self.video_info
        reply_count: int = len(self.replies)
        member_count: int = len(self.members)
        uid_lengths: Counter[int] = self.analyze_uid_lengths()
        levels: Counter[int] = self.analyze_levels()
        vips: Counter[str] = self.analyze_vips()
        sexes: Counter[str] = self.analyze_sexes()
        pendants: Counter[str] = self.analyze_pendants()
        cardbags: Counter[str] = self.analyze_cardbags()
        fans_name, fans_count, fans_levels = self.analyze_fans()
        locations: Counter[str] = self.analyze_locations()
        comment_intervals: Counter[str] = self.analyze_comment_intervals()
        analysis: Analysis = {
            "video_info": video_info,
            "reply_count": reply_count,
            "member_count": member_count,
            "uid_lengths": uid_lengths,
            "levels": levels,
            "vips": vips,
            "sexes": sexes,
            "pendants": pendants,
            "cardbags": cardbags,
            "fans_name": fans_name,
            "fans_count": fans_count,
            "fans_levels": fans_levels,
            "locations": locations,
            "comment_intervals": comment_intervals,
        }
        return analysis


def save_analysis(results: Analysis, filepath: FilePath) -> None:
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
