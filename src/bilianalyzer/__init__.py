from dataclasses import dataclass
from typing import Optional, Any

from bilibili_api.comment import CommentResourceType


@dataclass
class Member:

    uid: int
    name: str
    sex: str
    sign: str
    level: Optional[int] = None
    # pendant_name: str
    # nameplate_name: str
    # cardbg_name: str
    # fans_medal_name: Optional[str] = None
    # fans_medal_level: Optional[int] = None
    # location: Optional[str] = None


@dataclass
class Reply:
    rpid: int
    oid: int
    otype: CommentResourceType
    message: str
    ctime: int
    replies: Optional[list["Reply"]] = None
    location: Optional[str] = None
    member: Optional[Member] = None


@dataclass
class VideoInfo:

    bvid: str
    title: str
    publish_time: int
