import contextlib
import os
import traceback
from typing import ContextManager

from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, scoped_session


NAME = os.getenv('LISTSERVER_DATABASE_NAME', 'servers.db')

URL = f'sqlite:///{NAME}'

engine = create_engine(URL)

SessionLocal = sessionmaker(bind=engine)

Base = declarative_base()

current_session = scoped_session(SessionLocal)


@contextlib.contextmanager
def get_session() -> ContextManager[Session]:
    try:
        yield current_session
    except SQLAlchemyError:
        current_session.rollback()
        logger.exception('Exception occured')
        traceback.print_exc()
    finally:
        current_session.close()
