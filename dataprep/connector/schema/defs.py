from copy import deepcopy
from typing import Dict, Optional, Union

from .base import DefBase, field


class PaginationDef(DefBase):
    type: str = field(allows={"offset", "seek"})
    max_count: int = field()
    offset_key: Optional[str] = field()
    limit_key: str = field()
    seek_id: Optional[str] = field()
    seek_key: Optional[str] = field()


class FullFieldDef(DefBase):
    required: bool = field()
    from_key: Optional[str] = field()
    to_key: Optional[str] = field()
    template: Optional[str] = field()
    remove_if_empty: bool = field()


FieldDef = Union[str, bool, FullFieldDef]


class OAuth2AuthorizationDef(DefBase):
    type: str = field(allows={"OAuth2"})
    grant_type: str = field()
    token_server_url: str = field()


class QueryParamAuthorizationDef(DefBase):
    type: str = field(allows={"QueryParam"})
    key_param: str = field()


AuthorizationDef = Union[OAuth2AuthorizationDef, QueryParamAuthorizationDef, str]


class BodyDef(DefBase):
    ctype: str = field()
    content: Dict[str, FieldDef] = field()


class RequestDef(DefBase):

    url: str = field()
    method: str = field()
    authorization: Optional[AuthorizationDef] = field()
    headers: Optional[Dict[str, FieldDef]] = field()
    params: Dict[str, FieldDef] = field()
    pagination: Optional[PaginationDef] = field()
    body: Optional[BodyDef] = field()
    cookies: Optional[Dict[str, FieldDef]] = field()


class SchemaFieldDef(DefBase):

    target: str = field()
    type: str = field()
    description: Optional[str] = field()

    def merge(self, rhs: "SchemaFieldDef") -> "SchemaFieldDef":
        if type(self) != type(rhs):
            raise ValueError(f"Cannot merge {type(self)} with {type(rhs)}")

        if self.target != rhs.target:
            raise ValueError("Cannot merge SchemaFieldDef with different target.")

        merged_type = merge_type(self.type, rhs.type)

        cur = deepcopy(self)
        cur.type = merged_type
        cur.description = rhs.description

        return cur


TYPE_TREE = {
    "object": None,
    "string": None,
    "float": "string",
    "int": "float",
    "bool": "string",
}


def merge_type(a: str, b: str) -> str:
    if a == b:
        return a

    aset = {a}
    bset = {b}

    while True:
        aparent = TYPE_TREE[a]
        if aparent is not None:
            if aparent in bset:
                return aparent
            else:
                aset.add(aparent)
        bparent = TYPE_TREE[b]
        if bparent is not None:
            if bparent in aset:
                return bparent
            else:
                bset.add(bparent)

        if aparent is None and bparent is None:
            raise RuntimeError("Unreachable")


class ResponseDef(DefBase):
    ctype: str = field()
    table_path: str = field()
    schema: Dict[str, SchemaFieldDef] = field()
    orient: str = field()


class ConfigDef(DefBase):
    version: int = field(allows={1})
    request: RequestDef = field()
    response: ResponseDef = field()
