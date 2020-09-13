from typing import Any, Dict, NamedTuple, Union

from stringcase import camelcase, snakecase


class DefBase:
    def to_value(self) -> Any:
        attrs = [
            a
            for a in dir(self)
            if not a.startswith("__") and not callable(getattr(self, a))
        ]
        return {camelcase(attr): to_value(getattr(self, attr)) for attr in attrs}


def to_value(v: Any) -> Any:
    if isinstance(v, (bool, int, float, str)):
        return v
    elif isinstance(v, dict):
        return {camelcase(key): to_value(value) for key, value in v.items()}
    elif isinstance(v, list):
        return [to_value(value) for value in v]
    elif isinstance(v, DefBase):
        return v.to_value()
    else:
        raise ValueError(f"{type(v)} not supported.")


class CannotParseError(Exception):
    pass


class PaginationDef(DefBase):
    type: str
    max_count: int
    offset_key: str
    limit_key: str
    seek_id: str
    seek_key: str

    def __init__(
        self,
        type: str,
        max_count: int,
        offset_key: str,
        limit_key: str,
        seek_id: str,
        seek_key: str,
    ) -> None:
        super().__init__()

        self.type = type
        self.max_count = max_count
        self.offset_key = offset_key
        self.limit_key = limit_key
        self.seek_id = seek_id
        self.seek_key = seek_key

    @classmethod
    def from_value(cls, d: Any) -> "PaginationDef":
        if isinstance(d, dict):
            return cls(
                type=d["type"],
                max_count=d["maxCount"],
                offset_key=d["offsetKey"],
                limit_key=d["limitKey"],
                seek_id=d["seekId"],
                seek_key=d["seekKey"],
            )
        else:
            raise CannotParseError


FieldDef = Union[str, bool, "FullFieldDef"]


class FullFieldDef(DefBase):
    required: bool
    from_key: str
    to_key: str
    template: str
    remove_if_empty: bool

    def __init__(
        self,
        required: bool,
        from_key: str,
        to_key: str,
        template: str,
        remove_if_empty: bool,
    ) -> None:
        super().__init__()

        self.required = required
        self.from_key = from_key
        self.to_key = to_key
        self.template = template
        self.remove_if_empty = remove_if_empty

    @classmethod
    def from_value(cls, d: Any) -> "FullFieldDef":
        if isinstance(d, dict):
            return cls(
                required=d["required"],
                from_key=d["fromKey"],
                to_key=d["toKey"],
                template=d["template"],
                remove_if_empty=d["removeIfEmpty"],
            )
        else:
            raise CannotParseError


def parse_field(d: Any) -> FieldDef:
    if isinstance(d, str):
        return d
    elif isinstance(d, bool):
        return d
    elif isinstance(d, dict):
        return FullFieldDef.from_value(d)
    else:
        raise CannotParseError


AuthorizationDef = Union[
    "OAuth2AuthorizationDef", "QueryParamAuthorizationDef", "BearerAuthorizationDef"
]


class OAuth2AuthorizationDef(DefBase):
    grant_type: str
    token_server_url: str

    def __init__(self, grant_type: str, token_server_url: str) -> None:
        super().__init__()

        self.grant_type = grant_type
        self.token_server_url = token_server_url

    @classmethod
    def from_value(cls, d: Any) -> "OAuth2AuthorizationDef":
        if isinstance(d, dict) and d["type"] == "OAuth2":
            return cls(grant_type=d["grantType"], token_server_url=d["tokenServerUrl"])
        else:
            raise CannotParseError

    def to_value(self) -> Dict[str, Any]:
        return {"type": "OAuth2", **super().to_value()}


class QueryParamAuthorizationDef(DefBase):
    key_param: str

    def __init__(self, key_param: str):
        self.key_param = key_param

    @classmethod
    def from_value(cls, d: Any) -> "QueryParamAuthorizationDef":
        if isinstance(d, dict) and d["type"] == "QueryParam":
            return cls(key_param=d["keyParam"])
        else:
            raise CannotParseError

    def to_value(self) -> Dict[str, Any]:
        return {"type": "QueryParam", **super().to_value()}


class BearerAuthorizationDef(DefBase):
    @classmethod
    def from_value(cls, d: Any) -> "BearerAuthorizationDef":
        if d == "Bearer":
            return cls()
        else:
            raise CannotParseError

    def to_value(self) -> str:
        return "Bearer"


def parse_authorization(d: Any) -> AuthorizationDef:
    if d["type"] == "Bearer":
        return BearerAuthorizationDef.from_value(d)
    elif d["type"] == "OAuth2":
        return OAuth2AuthorizationDef.from_value(d)
    elif d["type"] == "QueryParam":
        return QueryParamAuthorizationDef.from_value(d)
    else:
        raise ValueError(f"Unknown authorization type {d['type']}")


class BodyDef(DefBase):
    ctype: str
    content: Dict[str, FieldDef]

    def __init__(self, ctype: str, content: Dict[str, FieldDef]) -> None:
        self.ctype = ctype
        self.content = content

    @classmethod
    def from_value(cls, d: Any) -> "BodyDef":
        if isinstance(d, dict):
            return cls(
                ctype=d["ctype"],
                content={
                    key: parse_field(value) for key, value in d["content"].items()
                },
            )
        else:
            raise CannotParseError


class RequestDef(DefBase):
    url: str
    method: str
    authorization: AuthorizationDef
    headers: Dict[str, FieldDef]
    params: Dict[str, FieldDef]
    pagination: PaginationDef
    body: BodyDef
    cookies: Dict[str, FieldDef]

    def __init__(
        self,
        url: str,
        method: str,
        authorization: AuthorizationDef,
        headers: Dict[str, FieldDef],
        params: Dict[str, FieldDef],
        pagination: PaginationDef,
        body: BodyDef,
        cookies: Dict[str, FieldDef],
    ) -> None:
        self.url = url
        self.method = method
        self.authorization = authorization
        self.headers = headers
        self.params = params
        self.pagination = pagination
        self.body = body
        self.cookies = cookies

    @classmethod
    def from_value(cls, d: Any) -> "RequestDef":
        if isinstance(d, dict):
            return cls(
                url=d["url"],
                method=d["method"],
                authorization=parse_authorization(d["authorization"]),
                headers={
                    key: parse_field(value) for key, value in d["headers"].items()
                },
                params={key: parse_field(value) for key, value in d["params"].items()},
                pagination=PaginationDef.from_dict(d["pagination"]),
                body=BodyDef.from_dict(d["body"]),
                cookies={
                    key: parse_field(value) for key, value in d["cookies"].items()
                },
            )
        else:
            raise CannotParseError


class SchemaDef(DefBase):
    target: str
    type: str
    description: str

    def __init__(self, target: str, type: str, description: str,) -> None:
        self.target = target
        self.type = type
        self.description = description

    @classmethod
    def from_value(cls, d: Any) -> "SchemaDef":
        if isinstance(d, dict):
            return cls(**d)
        else:
            raise CannotParseError


class ResponseDef(DefBase):
    ctype: str
    table_path: str
    schema: SchemaDef
    orient: str

    def __init__(
        self, ctype: str, table_path: str, schema: SchemaDef, orient: str
    ) -> None:
        self.ctype = ctype
        self.table_path = table_path
        self.schema = schema
        self.orient = orient

    @classmethod
    def from_value(cls, d: Any) -> "ResponseDef":
        if isinstance(d, dict):
            return cls(
                ctype=d["ctype"],
                table_path=d["table_path"],
                schema=SchemaDef.from_dict(d["schema"]),
                orient=d["orient"],
            )
        else:
            raise CannotParseError


class ConfigDef(DefBase):
    version: int
    request: RequestDef
    response: ResponseDef

    def __init__(
        self, version: int, request: RequestDef, response: ResponseDef
    ) -> None:
        self.version = version
        self.request = request
        self.response = response

    @classmethod
    def from_dict(cls, d: Any) -> "ConfigDef":
        if isinstance(d, dict):
            return cls(
                version=d["version"],
                request=RequestDef.from_dict(d["request"]),
                response=ResponseDef.from_dict(d["response"]),
            )
        else:
            raise CannotParseError
