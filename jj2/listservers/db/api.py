import contextlib
import contextvars
import os
import traceback

from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session


NAME = os.getenv("LISTSERVER_DATABASE_NAME", "servers.db")

URL = f"sqlite:///{NAME}"

engine = create_engine(URL)

SessionLocal = sessionmaker(bind=engine)

Base = declarative_base()

current_session = contextvars.ContextVar('current_session')


@contextlib.contextmanager
def get_session(session_factory=None, autocommit=False, **session_args) -> Session:
    if session_factory is None:
        session_factory = SessionLocal

    try:
        wrapped_session = current_session
        if wrapped_session:
            current_session.set(None)
    except LookupError:
        wrapped_session = None

    if wrapped_session is None:
        wrapped_session = session_factory(**session_args)
        current_session.set(wrapped_session)

    try:
        yield wrapped_session
        if autocommit:
            wrapped_session.commit()
    except SQLAlchemyError:
        wrapped_session.rollback()
        logger.exception("Exception occured")
        traceback.print_exc()
    finally:
        wrapped_session.close()
