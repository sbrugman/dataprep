from copy import deepcopy
from typing import (
    Any,
    Dict,
    Union,
    Optional,
    TypeVar,
    Type,
    Iterator,
    Tuple,
    Generic,
    cast,
    Set,
)

from stringcase import camelcase, snakecase

T = TypeVar("T")


class Field(Generic[T]):
    override_none: bool
    merge: Optional[str]  # merge policy, values: same, override, keep or None
    case: str  # attribute case in the schema, camel or snake
    default: Optional[T]
    allows: Optional[Set[T]] = None

    def __init__(
        self,
        *,
        default: Optional[T] = None,
        merge: Optional[str] = None,
        case: str = "camel",
        override_none: bool = True,
        allows: Optional[Set[T]] = None,
    ) -> None:
        if merge is not None and merge not in {"same", "override", "keep"}:
            raise ValueError(
                f"merge must be 'same' or 'override', or 'keep' or None, got {merge}"
            )
        self.merge = merge

        if case not in {"camel", "snake"}:
            raise ValueError(f"case must be 'camel' or 'snake', got {case}")
        self.case = case

        self.default = default
        self.override_none = override_none
        self.allows = allows


def field(
    *,
    default: Optional[T] = None,
    merge: Optional[str] = None,
    case: str = "camel",
    allows: Optional[Set[T]] = None,
) -> T:
    return cast(T, Field(default=default, merge=merge, case=case, allows=allows))


def coalesce(a: Optional[T], b: Optional[T]) -> Optional[T]:
    if a is None:
        return b
    else:
        return a


class DefBase:
    def __new__(cls: Type["DefBase"], *args: Any, **kwargs: Any) -> "DefBase":
        for attr, ty, _ in cls._field_defs():
            if hasattr(ty, "__origin__") and ty.__origin__ in {dict, Dict}:
                key_t, _ = ty.__args__
                if key_t != str:
                    raise RuntimeError(f"{attr} must have str as the Dict key.")

        return super().__new__(cls)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__()
        if args:
            raise ValueError(f"args not supported.")

        for attr, ty, policy in self._field_defs():
            if policy.case == "snake":
                target_attr = snakecase(attr)
            elif policy.case == "camel":
                target_attr = camelcase(attr)
            else:
                raise RuntimeError(f"Unknown case {policy.case}.")

            val = kwargs.get(target_attr, policy.default)
            if policy.allows is not None and val not in policy.allows:
                raise CannotParseError(
                    f"{target_attr} has value {val}, which is not in {policy.allows}"
                )

            setattr(self, attr, instantiate_from_value(attr, ty, val))

    def __str__(self) -> str:
        return str(self.to_value())

    def __repr__(self) -> str:
        return str(self)

    @classmethod
    def _field_defs(cls) -> Iterator[Tuple[str, T, Field[T]]]:
        for attr, ty in cls.__dict__.get("__annotations__", {}).items():
            policy = cls.__dict__.get(attr)
            if policy is None:
                raise RuntimeError(f"{attr} is not attached with a Policy.")
            yield attr, ty, policy

    def to_value(self) -> Any:
        ret = {}

        for attr, _, policy in self._field_defs():
            if policy.case == "snake":
                target_attr = snakecase(attr)
            elif policy.case == "camel":
                target_attr = camelcase(attr)
            else:
                raise RuntimeError(f"Unknown case {policy.case}.")

            val = getattr(self, attr)
            if val is not None:
                ret[target_attr] = to_value(getattr(self, attr))

        return ret

    def merge(self, rhs: "DefBase") -> "DefBase":
        if type(self) != type(rhs):
            raise ValueError(f"Cannot merge {type(self)} with {type(rhs)}")

        cur = deepcopy(self)

        for attr, _, policy in self._field_defs():
            cur_value, rhs_value = getattr(cur, attr), getattr(rhs, attr)

            if cur_value is None and rhs_value is None:
                pass
            elif (cur_value is None) != (rhs_value is None):
                if policy.override_none:
                    # TODO check type compability when value incoming
                    setattr(cur, attr, coalesce(cur_value, rhs_value))
                else:
                    raise ValueError(f"None {attr} cannot be overriden.")
            else:

                merged = merge_values(cur_value, rhs_value, attr, policy)
                if attr == "schema":
                    print(merged)
                setattr(cur, attr, merged)

        return cur


def merge_values(lhs: T, rhs: T, attr: str, policy: Field[T]) -> T:
    """merge two not none values."""

    if type(lhs) != type(rhs):
        raise ValueError(
            f"Cannot merge {type(lhs)} with {type(rhs)} for {type(lhs).__name__}.{attr}"
        )

    if isinstance(lhs, DefBase):
        return lhs.merge(rhs)
    elif isinstance(rhs, dict):
        for key in rhs.keys():
            if key in lhs:
                lhs[key] = merge_values(lhs[key], rhs[key], attr, policy)
            else:
                lhs[key] = deepcopy(rhs[key])
        return lhs
    elif isinstance(lhs, (int, float, str, bool)):
        if policy.merge is None or policy.merge == "same":
            if lhs != rhs:
                raise ValueError(
                    f"Cannot merge with different {type(lhs).__name__}.{attr}: {lhs} != {rhs}."
                )
            return lhs
        elif policy.merge == "override":
            return rhs
        elif policy.merge == "keep":
            return lhs
        else:
            raise RuntimeError(f"Unknown merge policy {policy.merge}.")
    else:
        raise RuntimeError(f"Unknown type {type(lhs)}.")


def instantiate_from_value(attr: str, ty: Type[T], val: Any) -> T:
    if hasattr(ty, "__origin__"):  # typing types
        if ty.__origin__ == Union:
            for subty in ty.__args__:
                try:
                    return instantiate_from_value(attr, subty, val)
                except CannotParseError:
                    continue
            else:
                raise ValueError(f"No compatable type for {val}. Tried: {ty.__args__}")
        elif ty.__origin__ == dict:  # Dict
            if not isinstance(val, dict):
                raise CannotParseError(f"{ty} expects a dict but val is {type(val)}")
            _, ty = ty.__args__

            instantiated = {
                key: instantiate_from_value(f"{attr}.{key}", ty, val)
                for key, val in val.items()
            }
            return instantiated
    elif issubclass(ty, DefBase):
        if not isinstance(val, dict):
            raise CannotParseError(f"{ty} expects a dict but val is {type(val)}")

        return ty(**val)

    elif ty == type(val):
        return cast(T, val)
    elif ty is None:
        if val is None:
            return val
        else:
            raise CannotParseError(f"{ty} expects None but val is {type(val)}")
    else:
        raise CannotParseError(f"{ty} expected but got {type(val)}.")


def to_value(v: Any) -> Any:
    if isinstance(v, (bool, int, float, str)):
        return v
    elif isinstance(v, dict):
        return {key: to_value(value) for key, value in v.items()}
    elif isinstance(v, list):
        return [value for value in v]
    elif isinstance(v, DefBase):
        return v.to_value()
    else:
        raise ValueError(f"{type(v)} not supported.")


class CannotParseError(Exception):
    pass
