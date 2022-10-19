import contextlib
import os
import traceback

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session


NAME = os.getenv("LISTSERVER_DATABASE_NAME", "servers.db")

URL = f"sqlite:///{NAME}"

engine = create_engine(URL)

SessionLocal = sessionmaker(bind=engine)

Base = declarative_base()


@contextlib.contextmanager
def get_session(session_factory=None) -> Session:
    if session_factory is None:
        session_factory = SessionLocal

    wrapped_session = session_factory()

    try:
        yield wrapped_session
    except SQLAlchemyError:
        traceback.print_exc()

    else:
        try:
            wrapped_session.commit()
        except SQLAlchemyError:
            wrapped_session.rollback()
            traceback.print_exc()

    finally:
        wrapped_session.close()
