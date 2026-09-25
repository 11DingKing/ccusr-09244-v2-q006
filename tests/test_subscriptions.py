"""订阅唯一性与通知生成流程测试。

覆盖：
- 首次订阅 / 重复请求 / 恢复订阅三种结果的区分与历史保留
- 并行订阅只能产生一个有效订阅
- 重复/并发版本发布只能产生一个可发送通知
- 取消后发布不产生通知，恢复后新版本正常通知且不复活旧通知
- 版本发布与通知落库的原子性（共同成功或共同失败）
- 通知持久化，重启后未读通知仍可查询
"""

import json
import socket
import threading
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import uvicorn

from app.database import SessionLocal
from app.models import (
    Dataset,
    DatasetItem,
    DatasetNotification,
    DatasetReview,
    DatasetSubscription,
    DatasetVersion,
    OperationData,
    RobotModel,
    Scene,
    Skill,
)

API = "/api/v1"
NOW = datetime.now(timezone.utc)


def _free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _http(method, url, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"}, method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


@pytest.fixture(scope="session")
def api_base():
    from main import app

    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            status, _ = _http("GET", f"{base}/health")
            if status == 200:
                break
        except Exception:
            pass
        threading.Event().wait(0.1)
    yield base
    server.should_exit = True
    thread.join(timeout=5)


def _seed_dataset(name_suffix="1"):
    """直接落库一个含 1 条作业数据的草稿数据集，返回 dataset_id。"""
    db = SessionLocal()
    try:
        model = RobotModel(name=f"机型-{name_suffix}", manufacturer="测试厂")
        scene = Scene(name=f"场景-{name_suffix}", category="测试")
        skill = Skill(name=f"技能-{name_suffix}", category="测试")
        db.add_all([model, scene, skill])
        db.flush()

        operation = OperationData(
            robot_model_id=model.id,
            scene_id=scene.id,
            skill_id=skill.id,
            motion_trajectory={"points": []},
            perception_records={"records": []},
            timestamp_start=NOW,
            timestamp_end=NOW + timedelta(seconds=10),
        )
        db.add(operation)
        db.flush()

        dataset = Dataset(
            name=f"数据集-{name_suffix}",
            robot_model_id=model.id,
            scene_id=scene.id,
            skill_id=skill.id,
            owner_team="数据生产组",
            total_items=1,
        )
        db.add(dataset)
        db.flush()
        db.add(DatasetItem(dataset_id=dataset.id, operation_data_id=operation.id))
        db.commit()
        return dataset.id
    finally:
        db.close()


def _approve_release(client, dataset_id, reviewer="审核员"):
    """走完整发布流程：submit → approve，返回 approve 响应。"""
    status, submitted = client.post(
        f"{API}/datasets/{dataset_id}/review",
        json={"action": "submit", "reviewer": reviewer},
    )
    assert status == 200, submitted
    status, body = client.post(
        f"{API}/datasets/{dataset_id}/review",
        json={"action": "approve", "reviewer": reviewer, "review_notes": "通过"},
    )
    assert status == 200, body
    return body


def _subscription_rows(dataset_id, team):
    db = SessionLocal()
    try:
        return db.query(DatasetSubscription).filter(
            DatasetSubscription.dataset_id == dataset_id,
            DatasetSubscription.subscriber_team == team,
        ).all()
    finally:
        db.close()


def _notification_count(version_id=None, team=None, unread_only=False):
    db = SessionLocal()
    try:
        query = db.query(DatasetNotification)
        if version_id is not None:
            query = query.filter(DatasetNotification.dataset_version_id == version_id)
        if team is not None:
            query = query.filter(DatasetNotification.subscriber_team == team)
        if unread_only:
            query = query.filter(DatasetNotification.is_read.is_(False))
        return query.count()
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 1. 首次订阅 / 重复请求 / 恢复订阅
# ---------------------------------------------------------------------------

def test_subscribe_distinguishes_created_duplicate_and_resumed(client):
    dataset_id = _seed_dataset("first")

    status, first = client.post(
        f"{API}/datasets/{dataset_id}/subscriptions",
        json={"subscriber_team": "算法A组", "contact_person": "张三"},
    )
    assert status == 200
    assert first["result"] == "created"
    assert first["status"] == "active"
    subscription_id = first["id"]

    # 重复请求：幂等返回同一条订阅，不新建
    status, duplicate = client.post(
        f"{API}/datasets/{dataset_id}/subscriptions",
        json={"subscriber_team": "算法A组", "contact_person": "张三"},
    )
    assert status == 200
    assert duplicate["result"] == "already_active"
    assert duplicate["id"] == subscription_id

    rows = _subscription_rows(dataset_id, "算法A组")
    assert len(rows) == 1

    # 取消订阅是软取消，历史行保留
    status, body = client.delete(
        f"{API}/datasets/{dataset_id}/subscriptions/{subscription_id}"
    )
    assert status == 200
    assert body["subscription_id"] == subscription_id

    rows = _subscription_rows(dataset_id, "算法A组")
    assert len(rows) == 1
    assert rows[0].status == "cancelled"
    assert rows[0].cancelled_at is not None

    status, active_list = client.get(
        f"{API}/datasets/{dataset_id}/subscriptions?active_only=true"
    )
    assert status == 200
    assert active_list == []

    # 重复取消应报错，而不是静默成功
    status, _ = client.delete(
        f"{API}/datasets/{dataset_id}/subscriptions/{subscription_id}"
    )
    assert status == 400

    # 恢复订阅复用原行
    status, resumed = client.post(
        f"{API}/datasets/{dataset_id}/subscriptions",
        json={"subscriber_team": "算法A组", "contact_person": "张三"},
    )
    assert status == 200
    assert resumed["result"] == "resumed"
    assert resumed["id"] == subscription_id
    assert resumed["status"] == "active"
    assert resumed["cancelled_at"] is None

    rows = _subscription_rows(dataset_id, "算法A组")
    assert len(rows) == 1
    assert rows[0].status == "active"

    # 生命周期事件完整保留
    status, events = client.get(
        f"{API}/datasets/{dataset_id}/subscriptions/{subscription_id}/events"
    )
    assert status == 200
    assert [e["action"] for e in events] == ["subscribed", "cancelled", "resumed"]


# ---------------------------------------------------------------------------
# 2. 并行订阅：并发请求只能产生一个有效订阅
# ---------------------------------------------------------------------------

def test_parallel_subscribe_creates_single_active_subscription(api_base):
    dataset_id = _seed_dataset("parallel")
    url = f"{api_base}{API}/datasets/{dataset_id}/subscriptions"
    payload = {"subscriber_team": "并发组", "contact_person": "王五"}

    results = []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        results.append(_http("POST", url, payload))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 8
    assert all(status == 200 for status, _ in results)
    created = [body for _, body in results if body["result"] == "created"]
    duplicates = [body for _, body in results if body["result"] == "already_active"]
    assert len(created) == 1
    assert len(duplicates) == 7
    assert len({body["id"] for body in duplicates + created}) == 1

    rows = _subscription_rows(dataset_id, "并发组")
    assert len(rows) == 1
    assert rows[0].status == "active"


# ---------------------------------------------------------------------------
# 3. 重复发布 / 并发发布：只能产生一个可发送通知
# ---------------------------------------------------------------------------

def test_duplicate_publish_generates_single_notification(client):
    dataset_id = _seed_dataset("dup-publish")
    client.post(
        f"{API}/datasets/{dataset_id}/subscriptions",
        json={"subscriber_team": "订阅一组"},
    )
    client.post(
        f"{API}/datasets/{dataset_id}/subscriptions",
        json={"subscriber_team": "订阅二组"},
    )

    release = _approve_release(client, dataset_id)

    # 同一版本每个接收方恰好一条通知
    assert _notification_count(version_id=release["dataset_version_id"]) == 2
    assert _notification_count(version_id=release["dataset_version_id"], team="订阅一组") == 1

    # 顺序重复 approve 被状态机拒绝，不产生新通知
    status, body = client.post(
        f"{API}/datasets/{dataset_id}/review",
        json={"action": "approve", "reviewer": "审核员"},
    )
    assert status == 400
    assert _notification_count() == 2


def test_concurrent_publish_generates_single_release_and_notification(api_base):
    dataset_id = _seed_dataset("concurrent-publish")
    subscribe_url = f"{api_base}{API}/datasets/{dataset_id}/subscriptions"
    review_url = f"{api_base}{API}/datasets/{dataset_id}/review"

    assert _http("POST", subscribe_url, {"subscriber_team": "并发发布组"})[0] == 200
    assert _http("POST", review_url, {"action": "submit"})[0] == 200

    results = []
    barrier = threading.Barrier(6)

    def worker():
        barrier.wait()
        results.append(_http(
            "POST", review_url,
            {"action": "approve", "reviewer": "审核员", "review_notes": "通过"},
        ))

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    success = [body for status, body in results if status == 200]
    rejected = [status for status, _ in results if status == 400]
    assert len(success) == 1
    assert len(rejected) == 5

    db = SessionLocal()
    try:
        # 只发布了一个新版本，只有一条可发送通知
        versions = db.query(DatasetVersion).filter(
            DatasetVersion.dataset_id == dataset_id
        ).order_by(DatasetVersion.version_number.asc()).all()
        # 只有一次发布成功，只产生一个新版本（current_version 由 1 推进到 2）
        assert [v.version_number for v in versions] == [2]
        notifications = db.query(DatasetNotification).filter(
            DatasetNotification.dataset_id == dataset_id
        ).all()
        assert len(notifications) == 1
        assert notifications[0].dataset_version_id == success[0]["dataset_version_id"]
        assert notifications[0].subscriber_team == "并发发布组"
        assert notifications[0].is_read is False
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 4. 取消后发布无通知；恢复后新版本正常通知且不复活旧通知
# ---------------------------------------------------------------------------

def test_cancelled_subscription_gets_no_notification_and_resume_does_not_revive(client):
    dataset_id = _seed_dataset("cancel-resume")

    status, sub = client.post(
        f"{API}/datasets/{dataset_id}/subscriptions",
        json={"subscriber_team": "摇摆组"},
    )
    assert status == 200

    # 取消后发布：不产生通知
    assert client.delete(
        f"{API}/datasets/{dataset_id}/subscriptions/{sub['id']}"
    )[0] == 200

    first_release = _approve_release(client, dataset_id)
    assert _notification_count(version_id=first_release["dataset_version_id"]) == 0

    # 恢复订阅：旧版本不补发通知
    status, resumed = client.post(
        f"{API}/datasets/{dataset_id}/subscriptions",
        json={"subscriber_team": "摇摆组"},
    )
    assert status == 200
    assert resumed["result"] == "resumed"
    assert _notification_count() == 0

    # 下线后重新走一轮发布：新版本产生且仅产生一条通知
    assert client.post(
        f"{API}/datasets/{dataset_id}/unpublish"
    )[0] == 200
    second_release = _approve_release(client, dataset_id)
    assert second_release["dataset_version_id"] != first_release["dataset_version_id"]

    assert _notification_count() == 1
    assert _notification_count(
        version_id=second_release["dataset_version_id"], team="摇摆组"
    ) == 1
    assert _notification_count(
        version_id=first_release["dataset_version_id"], team="摇摆组"
    ) == 0

    # 通知可追溯到恢复后的同一条订阅，运营能判断通知来自哪次有效订阅
    db = SessionLocal()
    try:
        note = db.query(DatasetNotification).filter(
            DatasetNotification.dataset_version_id == second_release["dataset_version_id"]
        ).one()
        assert note.subscription_id == resumed["id"]
        assert note.subscriber_team == "摇摆组"
        assert note.version_label == "1.2"
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 5. 版本发布与通知落库的原子性
# ---------------------------------------------------------------------------

def test_release_and_notifications_roll_back_together(client, monkeypatch):
    dataset_id = _seed_dataset("atomic")
    client.post(
        f"{API}/datasets/{dataset_id}/subscriptions",
        json={"subscriber_team": "原子组"},
    )
    assert client.post(
        f"{API}/datasets/{dataset_id}/review", json={"action": "submit"}
    )[0] == 200

    from app.services import subscription as subscription_service

    def boom(db, dataset, version):
        raise RuntimeError("通知落库失败")

    monkeypatch.setattr(subscription_service, "generate_notifications", boom)

    # 通知落库失败时，接口整体失败，不得留下半截发布结果
    with pytest.raises(RuntimeError):
        client.post(
            f"{API}/datasets/{dataset_id}/review",
            json={"action": "approve", "reviewer": "审核员"},
        )

    db = SessionLocal()
    try:
        dataset = db.query(Dataset).get(dataset_id)
        assert dataset.review_status == "pending_review"
        assert dataset.is_published is False
        assert dataset.published_at is None
        assert dataset.current_version == 1
        # 版本快照未新增（种子数据不产生版本行）
        assert db.query(DatasetVersion).filter(
            DatasetVersion.dataset_id == dataset_id
        ).count() == 0
        # 通知未落库
        assert db.query(DatasetNotification).filter(
            DatasetNotification.dataset_id == dataset_id
        ).count() == 0
        # approve 审核记录也随事务回滚
        assert db.query(DatasetReview).filter(
            DatasetReview.dataset_id == dataset_id,
            DatasetReview.action == "approve",
        ).count() == 0
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 6. 未读通知查询与重启后持久化
# ---------------------------------------------------------------------------

def test_unread_notifications_persist_across_restart(client, db_path):
    dataset_id = _seed_dataset("restart")
    client.post(
        f"{API}/datasets/{dataset_id}/subscriptions",
        json={"subscriber_team": "持久化组"},
    )
    release = _approve_release(client, dataset_id)

    status, unread = client.get(
        f"{API}/notifications",
        params={"subscriber_team": "持久化组", "unread_only": "true"},
    )
    assert status == 200
    assert len(unread) == 1
    notification_id = unread[0]["id"]
    assert unread[0]["version_label"] == "1.1"
    assert unread[0]["is_read"] is False
    assert unread[0]["dataset_version_id"] == release["dataset_version_id"]

    # 标记已读
    status, read_back = client.post(f"{API}/notifications/{notification_id}/read")
    assert status == 200
    assert read_back["is_read"] is True
    assert read_back["read_at"] is not None

    # 重复标记已读保持幂等
    status, again = client.post(f"{API}/notifications/{notification_id}/read")
    assert status == 200
    assert again["read_at"] == read_back["read_at"]

    # 模拟进程重启：用全新引擎/连接池打开同一个数据库文件
    fresh_engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
    )
    FreshSession = sessionmaker(bind=fresh_engine)
    try:
        db = FreshSession()
        try:
            assert db.query(DatasetNotification).filter(
                DatasetNotification.id == notification_id,
                DatasetNotification.is_read.is_(False),
            ).count() == 0
            assert db.query(DatasetNotification).filter(
                DatasetNotification.subscriber_team == "持久化组"
            ).count() == 1
            # 未读计数在重启后仍准确
            assert db.query(DatasetNotification).filter(
                DatasetNotification.is_read.is_(False)
            ).count() == 0
        finally:
            db.close()
    finally:
        fresh_engine.dispose()

    # “重启后”的新进程视角下，未读列表为空；已读记录仍可按团队查到
    status, unread_after = client.get(
        f"{API}/notifications",
        params={"subscriber_team": "持久化组", "unread_only": "true"},
    )
    assert unread_after == []
    status, all_notes = client.get(
        f"{API}/notifications", params={"subscriber_team": "持久化组"}
    )
    assert len(all_notes) == 1
    assert all_notes[0]["is_read"] is True
