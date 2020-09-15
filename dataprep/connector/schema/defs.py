"""Strong typed schema definition."""

from base64 import b64encode
from copy import deepcopy
from time import time
from typing import Any, Dict, Optional, Union

import requests

from .base import CannotParseError, DefBase, field

# pylint: disable=missing-class-docstring,missing-function-docstring
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
    storage: Dict[str, str]

    def __post_init__(self) -> None:
        self.storage = {}

    def build(self, req_data: Dict[str, Any], params: Dict[str, Any]) -> None:
        if self.grant_type == "ClientCredentials":
            if (
                "access_token" not in self.storage
                or self.storage.get("expires_at", 0) < time()
            ):
                # Not yet authorized
                ckey = params["client_id"]
                csecret = params["client_secret"]
                b64cred = b64encode(f"{ckey}:{csecret}".encode("ascii")).decode()
                resp: Dict[str, Any] = requests.post(
                    self.token_server_url,
                    headers={"Authorization": f"Basic {b64cred}"},
                    data={"grant_type": "client_credentials"},
                ).json()

                if resp["token_type"].lower() == "bearer":
                    raise RuntimeError("token_type is not bearer")

                access_token = resp["access_token"]
                self.storage["access_token"] = access_token
                if "expires_in" in resp:
                    self.storage["expires_at"] = (
                        time() + resp["expires_in"] - 60
                    )  # 60 seconds grace period to avoid clock lag

            req_data["headers"][
                "Authorization"
            ] = f"Bearer {self.storage['access_token']}"

            # TODO: handle auto refresh
        elif self.grant_type == "AuthorizationCode":
            raise NotImplementedError


class QueryParamAuthorizationDef(DefBase):
    type: str = field(allows={"QueryParam"})
    key_param: str = field()

    def build(self, req_data: Dict[str, Any], params: Dict[str, Any]) -> None:
        """Populate some required fields to the request data.
        Complex logic may also happens in this function (e.g. start a server to do OAuth).
        """
        req_data["params"][self.key_param] = params["access_token"]


class BearerAuthorizationDef(DefBase):
    def __init__(self, val: Any) -> None:  # pylint: disable=super-init-not-called
        if val == "Bearer":
            return
        else:
            raise CannotParseError

    def to_value(self) -> str:  # pylint: disable=no-self-use
        return "Bearer"

    @staticmethod
    def build(req_data: Dict[str, Any], params: Dict[str, Any]) -> None:
        """Populate some required fields to the request data.
        Complex logic may also happens in this function (e.g. start a server to do OAuth).
        """
        req_data["headers"]["Authorization"] = f"Bearer {params['access_token']}"


AuthorizationDef = Union[
    OAuth2AuthorizationDef, QueryParamAuthorizationDef, BearerAuthorizationDef
]


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

    def merge(self, rhs: Any) -> "SchemaFieldDef":
        if not isinstance(rhs, SchemaFieldDef):
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


def merge_type(a: str, b: str) -> str:  # pylint: disable=invalid-name
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
    ctype: str = field(allows={"application/xml", "application/json"})
    table_path: str = field()
    schema: Dict[str, SchemaFieldDef] = field()
    orient: str = field(allows={"records", "split"})


class ConfigDef(DefBase):
    version: int = field(allows={1})
    request: RequestDef = field()
    response: ResponseDef = field()
