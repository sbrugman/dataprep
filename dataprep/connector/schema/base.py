# pylint: disable=missing-module-docstring, missing-class-docstring,missing-function-docstring,no-else-raise
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

T = TypeVar("T")  # pylint: disable=invalid-name


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


def coalesce(  # pylint: disable=invalid-name
    a: Optional[T], b: Optional[T]
) -> Optional[T]:
    if a is None:
        return b
    else:
        return a


class DefBase:
    def __new__(  # pylint: disable=unused-argument
        cls: Type["DefBase"], *, val: Any
    ) -> "DefBase":
        for attr, typ, _ in cls._field_defs():
            if hasattr(typ, "__origin__") and typ.__origin__ in {dict, Dict}:
                key_t, _ = typ.__args__
                if key_t != str:
                    raise RuntimeError(f"{attr} must have str as the Dict key.")

        return super().__new__(cls)

    def __init__(self, *, val: Any) -> None:
        super().__init__()
        if not isinstance(val, dict):
            raise CannotParseError(f"{_fty(type(self))} expects a dict but got {val}")
        val = cast(Dict[str, Any], val)

        for attr, typ, policy in self._field_defs():
            if policy.case == "snake":
                target_attr: str = snakecase(attr)
            elif policy.case == "camel":
                target_attr = camelcase(attr)
            else:
                raise RuntimeError(f"Unknown case {policy.case}.")

            val_ = val.get(target_attr, policy.default)
            if policy.allows is not None and val_ not in policy.allows:
                raise CannotParseError(
                    f"{target_attr} has value {val_}, which is not in {policy.allows}"
                )

            setattr(self, attr, instantiate_from_value(attr, typ, val_))

        self.__post_init__()

    def __post_init__(self) -> None:
        pass

    def __str__(self) -> str:
        return str(self.to_value())

    def __repr__(self) -> str:
        return str(self)

    @classmethod
    def _field_defs(cls) -> Iterator[Tuple[str, Any, Field[Any]]]:
        for attr, typ in cls.__dict__.get("__annotations__", {}).items():
            policy = cls.__dict__.get(attr)
            if policy is None:
                continue
            yield attr, typ, policy

    def to_value(self) -> Any:
        ret = {}

        for attr, _, policy in self._field_defs():
            if policy.case == "snake":
                target_attr: str = snakecase(attr)
            elif policy.case == "camel":
                target_attr = camelcase(attr)
            else:
                raise RuntimeError(f"Unknown case {policy.case}.")

            val = getattr(self, attr)
            if val is not None:
                ret[target_attr] = to_value(getattr(self, attr))

        return ret

    def merge(self, rhs: Any) -> "DefBase":
        if not isinstance(rhs, type(self)):
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


def merge_values(  # pylint: disable=too-many-branches
    lhs: T, rhs: T, attr: str, policy: Field[T]
) -> T:
    """merge two not none values."""

    if not isinstance(rhs, type(lhs)):
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
        raise RuntimeError(f"Unknown type {type(lhs).__name__}.")


def instantiate_from_value(  # pylint: disable=too-many-branches
    attr: str, typ: Type[T], val: Any
) -> T:
    if hasattr(typ, "__origin__"):  # typing types
        if typ.__origin__ == Union:
            for subty in typ.__args__:
                try:
                    return instantiate_from_value(attr, subty, val)
                except CannotParseError:
                    continue
            raise ValueError(
                f"No compatable type for {attr}: {_fty(typ)} with value {val}."
            )

        if typ.__origin__ == dict:  # Dict
            if not isinstance(val, dict):
                raise CannotParseError(f"{attr}: {_fty(typ)} expected but val is {val}")

            _, typ = typ.__args__

            instantiated: T = {
                key: instantiate_from_value(f"{attr}.{key}", typ, val_)
                for key, val_ in val.items()
            }
            return instantiated

        raise NotImplementedError(typ)
    elif issubclass(typ, DefBase):
        return typ(val=val)
    elif typ == type(val):
        return cast(T, val)
    elif typ is None:
        if val is not None:
            raise CannotParseError(f"{attr}:{_fty(typ)} expects None but val is {val}")
        return val
    else:
        raise CannotParseError(f"{attr}: {_fty(typ)} expected but got {val}.")


def to_value(val: Any) -> Any:
    if isinstance(val, (bool, int, float, str)):
        return val
    elif isinstance(val, dict):
        return {key: to_value(value) for key, value in val.items()}
    elif isinstance(val, list):
        return val
    elif isinstance(val, DefBase):
        return val.to_value()
    else:
        raise ValueError(f"{type(val)} not supported.")


def _fty(typ: Type[T]) -> str:
    if hasattr(typ, "__origin__"):
        return str(typ)
    elif typ is None:
        return str(None)
    else:
        return typ.__name__


class CannotParseError(Exception):
    """Cannot parse the def. Raise this for Union
    will try the parser the next type."""
