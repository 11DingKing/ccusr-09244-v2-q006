"""数据集订阅与版本发布通知服务。

约束：
- 接收方（subscriber_team）对同一数据集任一时刻至多一条有效订阅（唯一索引 + 状态机）。
- 取消订阅保留历史（软取消 + 生命周期事件），重新订阅复用原行，不复活旧通知。
- 版本发布与通知落库在同一事务内完成，共同成功或共同失败。
- 每个 (版本, 订阅) 至多一条通知，重复/并发发布只能产生一个可发送结果。
"""

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.models import (
    Dataset,
    DatasetNotification,
    DatasetSubscription,
    DatasetSubscriptionEvent,
    DatasetVersion,
)


# 订阅接口返回的三种结果
CREATED = "created"        # 首次订阅
ALREADY_ACTIVE = "already_active"  # 重复请求：已有生效订阅
RESUMED = "resumed"        # 恢复订阅：复用曾取消的订阅行


def get_active_subscription(db, dataset_id: int, subscriber_team: str):
    """按唯一键取订阅行（含已取消的）。并发场景下对该行加写锁。"""
    stmt = (
        select(DatasetSubscription)
        .where(
            DatasetSubscription.dataset_id == dataset_id,
            DatasetSubscription.subscriber_team == subscriber_team,
        )
    )
    return db.execute(stmt.with_for_update()).scalar_one_or_none()


def subscribe(db, dataset: Dataset, subscriber_team: str,
              contact_person=None, notify_on_new_version: bool = True,
              actor: str = None):
    """幂等订阅。返回 (subscription, result)；result 区分首次/重复/恢复。

    并发下依赖 (dataset_id, subscriber_team) 唯一索引兜底：竞争失败的事务
    回滚并由调用方重试，最终只有一条订阅行、一个有效状态。
    """
    for attempt in range(2):
        subscription = get_active_subscription(db, dataset.id, subscriber_team)
        if subscription is None:
            subscription = DatasetSubscription(
                dataset_id=dataset.id,
                subscriber_team=subscriber_team,
                contact_person=contact_person,
                notify_on_new_version=notify_on_new_version,
                status="active",
            )
            db.add(subscription)
            db.flush()
            db.add(DatasetSubscriptionEvent(
                subscription_id=subscription.id, action="subscribed", actor=actor
            ))
            try:
                db.commit()
            except IntegrityError:
                # 并发请求抢先插入，回滚后重读，走重复/恢复分支
                db.rollback()
                if attempt == 0:
                    continue
                raise
            db.refresh(subscription)
            return subscription, CREATED

        if subscription.status == "active":
            # 重复请求：幂等返回现有订阅，不重复落任何事件/通知
            return subscription, ALREADY_ACTIVE

        # 历史订阅处于取消状态：恢复原行（保留历史，不产生新行、不复活旧通知）
        subscription.status = "active"
        subscription.cancelled_at = None
        subscription.contact_person = contact_person
        subscription.notify_on_new_version = notify_on_new_version
        db.add(DatasetSubscriptionEvent(
            subscription_id=subscription.id, action="resumed", actor=actor
        ))
        db.commit()
        db.refresh(subscription)
        return subscription, RESUMED


def unsubscribe(db, dataset_id: int, subscriber_team: str, actor: str = None):
    """软取消订阅：置状态并记录事件，保留行与历史。返回取消的订阅，不存在或已取消返回 None。"""
    subscription = get_active_subscription(db, dataset_id, subscriber_team)
    if subscription is None or subscription.status == "cancelled":
        return None

    subscription.status = "cancelled"
    subscription.cancelled_at = datetime.now(timezone.utc)
    db.add(DatasetSubscriptionEvent(
        subscription_id=subscription.id, action="cancelled", actor=actor
    ))
    db.commit()
    db.refresh(subscription)
    return subscription


def create_next_version(db, dataset: Dataset, change_description: str = None,
                        created_by: str = None):
    """生成下一个版本快照并推进数据集版本号。调用方与发布在同一事务内。"""
    next_number = (dataset.current_version or 1) + 1
    base_label = dataset.version or "1.0"
    parts = base_label.split(".")
    if len(parts) == 2 and parts[1].isdigit():
        next_label = f"{parts[0]}.{int(parts[1]) + 1}"
    else:
        next_label = str(next_number)

    version = DatasetVersion(
        dataset_id=dataset.id,
        version_number=next_number,
        version_label=next_label,
        change_description=change_description,
        created_by=created_by,
        total_items=dataset.total_items,
        success_count=dataset.success_count,
        failure_count=dataset.failure_count,
        annotation_complete_rate=dataset.annotation_complete_rate,
        average_quality_score=dataset.average_quality_score,
        data_grade=dataset.data_grade,
    )
    db.add(version)
    db.flush()

    dataset.current_version = next_number
    dataset.version = next_label
    return version


def generate_notifications(db, dataset: Dataset, version: DatasetVersion):
    """为当前有效订阅生成版本通知（仅在发布事务内调用，随版本一起提交）。

    取消的订阅不产生通知；恢复前发布的旧版本不会补发——每个版本只在发布瞬间
    快照一次有效订阅，因此重新订阅不会复活旧通知。
    """
    subscriptions = db.execute(
        select(DatasetSubscription)
        .where(
            DatasetSubscription.dataset_id == dataset.id,
            DatasetSubscription.status == "active",
            DatasetSubscription.notify_on_new_version.is_(True),
        )
        .with_for_update()
    ).scalars().all()

    notifications = []
    for sub in subscriptions:
        notifications.append(DatasetNotification(
            dataset_id=dataset.id,
            dataset_version_id=version.id,
            subscription_id=sub.id,
            subscriber_team=sub.subscriber_team,
            contact_person=sub.contact_person,
            version_label=version.version_label,
            message=f"数据集 '{dataset.name}' 已发布新版本 {version.version_label}",
        ))
    if notifications:
        db.add_all(notifications)
        db.flush()
    return notifications


def release_approved_dataset(db, dataset: Dataset, reviewer: str = None,
                             review_notes: str = None):
    """审核通过并发布：审核记录、版本快照、通知在同一事务落库。

    全程不提交，由调用方在同一事务边界内提交；任何一步失败整体回滚，
    满足“版本发布与通知落库共同成功或共同失败”。
    """
    from app.models import DatasetReview

    version = create_next_version(
        db, dataset,
        change_description=review_notes or "审核通过，发布新版本",
        created_by=reviewer,
    )
    dataset.is_published = True
    dataset.published_at = datetime.now(timezone.utc)

    review = DatasetReview(
        dataset_id=dataset.id,
        action="approve",
        reviewer=reviewer,
        review_notes=review_notes,
        dataset_version_id=version.id,
    )
    db.add(review)

    notifications = generate_notifications(db, dataset, version)
    db.flush()
    return review, version, notifications


def list_notifications(db, subscriber_team: str = None, dataset_id: int = None,
                       unread_only: bool = False, skip: int = 0, limit: int = 100):
    """查询通知；unread_only 用于“未读通知”查询，持久化存储因此重启后仍可查。"""
    stmt = select(DatasetNotification)
    if subscriber_team:
        stmt = stmt.where(DatasetNotification.subscriber_team == subscriber_team)
    if dataset_id is not None:
        stmt = stmt.where(DatasetNotification.dataset_id == dataset_id)
    if unread_only:
        stmt = stmt.where(DatasetNotification.is_read.is_(False))
    stmt = stmt.order_by(
        DatasetNotification.created_at.desc(), DatasetNotification.id.desc()
    ).offset(skip).limit(limit)
    return db.execute(stmt).scalars().all()


def mark_notification_read(db, notification_id: int, subscriber_team: str = None):
    notification = db.get(DatasetNotification, notification_id)
    if notification is None:
        return None
    if subscriber_team and notification.subscriber_team != subscriber_team:
        return None
    if not notification.is_read:
        notification.is_read = True
        notification.read_at = datetime.now(timezone.utc)
        db.commit()
        db.refresh(notification)
    return notification
