from dataclasses import dataclass
from typing import Optional

from bilibili_api.comment import CommentResourceType


@dataclass
class Member:

    uid: int
    name: str
    sex: Optional[str] = None
    sign: Optional[str] = None
    level: Optional[int] = None
    vip: Optional[str] = None
    pendant: Optional[str] = None
    cardbag: Optional[str] = None
    # TODO: fans medal

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
