from typing import Optional

from pymongo.client_session import ClientSession

from mongoengine.connection import DEFAULT_CONNECTION_NAME

_sessions = {}


def set_local_session(db_alias: str, session: ClientSession):
    _sessions[key_local_session(db_alias)] = session


def get_local_session(
    db_alias: str = DEFAULT_CONNECTION_NAME,
) -> Optional[ClientSession]:
    return _sessions.get(key_local_session(db_alias))


def clear_local_session(db_alias: str = DEFAULT_CONNECTION_NAME):
    _sessions.pop(key_local_session(db_alias), None)


def clear_all():
    global _sessions
    _sessions = {}


def key_local_session(db_alias):
    return f"mongoengine_session_{db_alias}"
