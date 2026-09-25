from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from app.config import settings


connect_args = {"check_same_thread": False}
if settings.DATABASE_URL.startswith("sqlite"):
    # SQLite 下多连接并发写需要等待锁，而非立刻报 database is locked
    connect_args["timeout"] = 30

engine = create_engine(
    settings.DATABASE_URL,
    connect_args=connect_args,
    echo=False
)


if settings.DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, connection_record):
        # 由 SQLAlchemy 显式控制事务（配合下方 BEGIN IMMEDIATE）
        dbapi_connection.isolation_level = None
        cursor = dbapi_connection.cursor()
        # WAL 允许读写并发；busy_timeout 让写锁竞争排队等待而不是立即失败
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    @event.listens_for(engine, "begin")
    def _begin_immediate(dbapi_connection):
        # 每个事务一开始即获取写锁，并发请求在 busy_timeout 内排队，
        # 后进入的事务能读到先提交事务的结果，从而避免重复订阅/重复发布。
        dbapi_connection.exec_driver_sql("BEGIN IMMEDIATE")


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
