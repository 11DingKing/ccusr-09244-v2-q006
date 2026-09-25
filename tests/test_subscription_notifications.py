import concurrent.futures
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base, get_db
from app.models import (
    DatasetNotification,
    DatasetReview,
    DatasetSubscription,
    DatasetVersion,
    OperationData,
    RobotModel,
    Scene,
    Skill,
)
from main import app

API = "/api/v1"


@pytest.fixture()
def env(tmp_path):
    """每个用例一个独立的 SQLite 文件库，通过依赖覆盖注入应用。"""
    db_file = tmp_path / "test.db"
    engine = create_engine(
        f"sqlite:///{db_file}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)

    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    try:
        yield SimpleNamespace(
            engine=engine,
            session_factory=TestingSessionLocal,
            db_file=db_file,
            client=TestClient(app),
        )
    finally:
        app.dependency_overrides.clear()
        engine.dispose()


def _seed_operation_data(session_factory):
    db = session_factory()
    try:
        rm = RobotModel(name="RM-X", manufacturer="acme")
        sc = Scene(name="SC-X", category="test")
        sk = Skill(name="SK-X", category="test")
        db.add_all([rm, sc, sk])
        db.flush()
        op = OperationData(
            robot_model_id=rm.id,
            scene_id=sc.id,
            skill_id=sk.id,
            motion_trajectory={"points": []},
            perception_records={"frames": []},
            timestamp_start=datetime.now(timezone.utc),
            timestamp_end=datetime.now(timezone.utc),
        )
        db.add(op)
        db.commit()
        return rm.id, sc.id, op.id
    finally:
        db.close()


def _create_dataset(client, robot_model_id, scene_id, op_id, name="DS-1"):
    r = client.post(f"{API}/datasets", json={
        "name": name,
        "robot_model_id": robot_model_id,
        "scene_id": scene_id,
        "owner_team": "owner-team",
        "operation_data_ids": [op_id],
    })
    assert r.status_code == 200, r.text
    return r.json()["id"]


def _subscribe(client, dataset_id, team):
    return client.post(f"{API}/datasets/{dataset_id}/subscriptions", json={"subscriber_team": team})


def _publish_version(client, dataset_id, reviewer="ops"):
    """提交审核并通过，发布当前版本并生成通知。"""
    r = client.post(f"{API}/datasets/{dataset_id}/review", json={"action": "submit"})
    assert r.status_code == 200, r.text
    r = client.post(f"{API}/datasets/{dataset_id}/review", json={"action": "approve", "reviewer": reviewer})
    assert r.status_code == 200, r.text
    return r.json()


def _start_new_version_cycle(client, dataset_id):
    r = client.post(f"{API}/datasets/{dataset_id}/versions", json={"change_description": "下一版本"})
    assert r.status_code == 200, r.text
    return r.json()


def _unread(client, team):
    r = client.get(f"{API}/notifications/unread", params={"subscriber_team": team})
    assert r.status_code == 200, r.text
    return r.json()


def _notification_rows(env, dataset_id):
    db = env.session_factory()
    try:
        return db.query(DatasetNotification).filter_by(dataset_id=dataset_id).all()
    finally:
        db.close()


def test_subscribe_distinguishes_created_duplicate_and_restored(env):
    rm_id, sc_id, op_id = _seed_operation_data(env.session_factory)
    ds = _create_dataset(env.client, rm_id, sc_id, op_id)

    # 首次订阅
    r1 = _subscribe(env.client, ds, "teamA")
    assert r1.status_code == 201
    body1 = r1.json()
    assert body1["result"] == "created"
    assert body1["subscription"]["status"] == "active"
    assert body1["subscription"]["epoch"] == 1
    sub_id = body1["subscription"]["id"]

    # 重复请求：幂等返回同一条订阅，不产生新记录
    r2 = _subscribe(env.client, ds, "teamA")
    assert r2.status_code == 200
    assert r2.json()["result"] == "duplicate"
    assert r2.json()["subscription"]["id"] == sub_id
    assert r2.json()["subscription"]["epoch"] == 1

    # 取消后重新订阅：恢复同一条记录，周期递增
    r = env.client.delete(f"{API}/datasets/{ds}/subscriptions/{sub_id}")
    assert r.status_code == 200
    assert r.json()["status"] == "cancelled"

    r3 = _subscribe(env.client, ds, "teamA")
    assert r3.status_code == 200
    assert r3.json()["result"] == "restored"
    assert r3.json()["subscription"]["id"] == sub_id
    assert r3.json()["subscription"]["status"] == "active"
    assert r3.json()["subscription"]["epoch"] == 2

    # 任一时刻同一接收方只有一条订阅记录
    db = env.session_factory()
    try:
        subs = db.query(DatasetSubscription).filter_by(dataset_id=ds, subscriber_team="teamA").all()
        assert len(subs) == 1
        assert subs[0].status == "active"
        assert subs[0].epoch == 2
    finally:
        db.close()


def test_parallel_subscribe_creates_single_active_subscription(env):
    rm_id, sc_id, op_id = _seed_operation_data(env.session_factory)
    ds = _create_dataset(env.client, rm_id, sc_id, op_id)

    def do_subscribe(_):
        client = TestClient(app)
        return client.post(f"{API}/datasets/{ds}/subscriptions", json={"subscriber_team": "teamA"})

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(do_subscribe, range(8)))

    assert all(r.status_code in (200, 201) for r in responses)
    results = [r.json()["result"] for r in responses]
    assert results.count("created") == 1
    assert results.count("duplicate") == len(responses) - 1
    assert len({r.json()["subscription"]["id"] for r in responses}) == 1

    db = env.session_factory()
    try:
        subs = db.query(DatasetSubscription).filter_by(dataset_id=ds, subscriber_team="teamA").all()
        assert len(subs) == 1
        assert subs[0].status == "active"
    finally:
        db.close()

    # 并发订阅只产生一个有效订阅，发布后该接收方只收到一条通知
    _publish_version(env.client, ds)
    unread = _unread(env.client, "teamA")
    assert len(unread) == 1
    assert unread[0]["subscriber_team"] == "teamA"


def test_concurrent_publish_produces_single_version_and_notifications(env):
    rm_id, sc_id, op_id = _seed_operation_data(env.session_factory)
    ds = _create_dataset(env.client, rm_id, sc_id, op_id)
    _subscribe(env.client, ds, "teamA")
    _subscribe(env.client, ds, "teamB")

    r = env.client.post(f"{API}/datasets/{ds}/review", json={"action": "submit"})
    assert r.status_code == 200, r.text

    def do_approve(_):
        client = TestClient(app)
        return client.post(f"{API}/datasets/{ds}/review", json={"action": "approve", "reviewer": "ops"})

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        responses = list(pool.map(do_approve, range(6)))

    # 并发发布只有一个可发送结果：一次成功，其余被状态机或冲突检测拒绝
    succeeded = [r for r in responses if r.status_code == 200]
    assert len(succeeded) == 1
    assert all(r.status_code in (400, 409) for r in responses if r.status_code != 200)

    db = env.session_factory()
    try:
        versions = db.query(DatasetVersion).filter_by(dataset_id=ds).all()
        numbers = [v.version_number for v in versions]
        assert len(numbers) == len(set(numbers))
        # 初始版本 + 一次发布快照，并发没有产生额外版本
        assert len(versions) == 2

        notifications = db.query(DatasetNotification).filter_by(dataset_id=ds).all()
        assert len(notifications) == 2
        assert {n.subscriber_team for n in notifications} == {"teamA", "teamB"}
        assert all(n.status == "unread" for n in notifications)

        approve_reviews = db.query(DatasetReview).filter_by(dataset_id=ds, action="approve").all()
        assert len(approve_reviews) == 1
    finally:
        db.close()


def test_repeated_publish_request_does_not_duplicate_notifications(env):
    rm_id, sc_id, op_id = _seed_operation_data(env.session_factory)
    ds = _create_dataset(env.client, rm_id, sc_id, op_id)
    _subscribe(env.client, ds, "teamA")

    _publish_version(env.client, ds)
    assert len(_unread(env.client, "teamA")) == 1

    # 串行重试同一个发布请求：状态机拒绝，通知不重复
    r = env.client.post(f"{API}/datasets/{ds}/review", json={"action": "approve", "reviewer": "ops"})
    assert r.status_code == 400
    assert len(_notification_rows(env, ds)) == 1
    assert len(_unread(env.client, "teamA")) == 1


def test_publish_after_cancel_notifies_only_active_subscribers(env):
    rm_id, sc_id, op_id = _seed_operation_data(env.session_factory)
    ds = _create_dataset(env.client, rm_id, sc_id, op_id)
    sub_a = _subscribe(env.client, ds, "teamA").json()["subscription"]
    _subscribe(env.client, ds, "teamB")

    r = env.client.delete(f"{API}/datasets/{ds}/subscriptions/{sub_a['id']}")
    assert r.status_code == 200
    assert r.json()["status"] == "cancelled"

    _publish_version(env.client, ds)

    # 已取消的接收方不产生通知，有效订阅正常收到
    assert _unread(env.client, "teamA") == []
    unread_b = _unread(env.client, "teamB")
    assert len(unread_b) == 1
    assert unread_b[0]["version_label"] == "1.0"

    notifications = _notification_rows(env, ds)
    assert len(notifications) == 1
    assert notifications[0].subscriber_team == "teamB"

    # 取消的订阅保留在历史记录中
    r = env.client.get(f"{API}/datasets/{ds}/subscriptions", params={"status": "cancelled"})
    assert r.status_code == 200
    assert [s["subscriber_team"] for s in r.json()] == ["teamA"]


def test_restore_does_not_resurrect_old_notifications(env):
    rm_id, sc_id, op_id = _seed_operation_data(env.session_factory)
    ds = _create_dataset(env.client, rm_id, sc_id, op_id)
    _subscribe(env.client, ds, "teamA")

    # 第一次发布：产生周期 1 的未读通知
    _publish_version(env.client, ds)
    unread = _unread(env.client, "teamA")
    assert len(unread) == 1
    first = unread[0]
    assert first["subscription_epoch"] == 1
    assert first["version_label"] == "1.0"
    sub_id = first["subscription_id"]

    # 取消订阅：未读不再可见，历史保留
    env.client.delete(f"{API}/datasets/{ds}/subscriptions/{sub_id}")
    assert _unread(env.client, "teamA") == []

    # 恢复订阅：旧通知不复活
    r = _subscribe(env.client, ds, "teamA")
    assert r.json()["result"] == "restored"
    assert _unread(env.client, "teamA") == []

    # 恢复后发布新版本：只出现新周期的一条未读
    _start_new_version_cycle(env.client, ds)
    _publish_version(env.client, ds)
    unread = _unread(env.client, "teamA")
    assert len(unread) == 1
    assert unread[0]["subscription_epoch"] == 2
    assert unread[0]["version_label"] == "1.1"

    # 历史通知仍可追溯，且能区分来自哪一次有效订阅
    r = env.client.get(f"{API}/datasets/{ds}/notifications")
    assert r.status_code == 200
    history = r.json()
    assert len(history) == 2
    assert sorted(n["subscription_epoch"] for n in history) == [1, 2]
    assert all(n["subscription_id"] == sub_id for n in history)


def test_unread_notifications_survive_restart(env):
    rm_id, sc_id, op_id = _seed_operation_data(env.session_factory)
    ds = _create_dataset(env.client, rm_id, sc_id, op_id)
    _subscribe(env.client, ds, "teamA")
    _publish_version(env.client, ds)
    assert len(_unread(env.client, "teamA")) == 1

    # 模拟服务重启：关闭旧引擎，基于同一数据库文件创建新引擎与会话
    env.engine.dispose()
    restarted_engine = create_engine(
        f"sqlite:///{env.db_file}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    RestartedSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=restarted_engine)

    def restarted_get_db():
        db = RestartedSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = restarted_get_db
    try:
        client = TestClient(app)
        r = client.get(f"{API}/notifications/unread", params={"subscriber_team": "teamA"})
        assert r.status_code == 200
        unread = r.json()
        assert len(unread) == 1
        assert unread[0]["subscriber_team"] == "teamA"
        assert unread[0]["status"] == "unread"
        assert unread[0]["version_label"] == "1.0"
    finally:
        restarted_engine.dispose()


def test_mark_notification_read(env):
    rm_id, sc_id, op_id = _seed_operation_data(env.session_factory)
    ds = _create_dataset(env.client, rm_id, sc_id, op_id)
    _subscribe(env.client, ds, "teamA")
    _publish_version(env.client, ds)

    unread = _unread(env.client, "teamA")
    assert len(unread) == 1
    notification_id = unread[0]["id"]

    r = env.client.post(f"{API}/notifications/{notification_id}/read")
    assert r.status_code == 200
    assert r.json()["status"] == "read"
    assert r.json()["read_at"] is not None
    assert _unread(env.client, "teamA") == []

    # 重复标记幂等
    r = env.client.post(f"{API}/notifications/{notification_id}/read")
    assert r.status_code == 200
    assert r.json()["status"] == "read"

    # 已读通知仍保留在数据集的通知历史中
    r = env.client.get(f"{API}/datasets/{ds}/notifications", params={"status": "read"})
    assert r.status_code == 200
    assert len(r.json()) == 1

    r = env.client.post(f"{API}/notifications/999999/read")
    assert r.status_code == 404


def test_version_cycle_assigns_unique_sequential_numbers(env):
    rm_id, sc_id, op_id = _seed_operation_data(env.session_factory)
    ds = _create_dataset(env.client, rm_id, sc_id, op_id)

    _publish_version(env.client, ds)
    snapshot = _start_new_version_cycle(env.client, ds)
    # 新版本周期的快照记录的是上一周期（1.0）的最终状态
    assert snapshot["version_label"] == "1.0"
    _publish_version(env.client, ds)

    db = env.session_factory()
    try:
        numbers = [
            n for (n,) in db.query(DatasetVersion.version_number)
            .filter_by(dataset_id=ds)
            .order_by(DatasetVersion.version_number)
            .all()
        ]
        assert numbers == [1, 2, 3, 4]
    finally:
        db.close()
