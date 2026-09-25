from typing import List, Optional
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import (
    Dataset, DatasetItem, DatasetReuse,
    DatasetVersion, DatasetReview, DatasetSubscription, DatasetNotification,
    OperationData, Annotation, RobotModel, Scene
)
from app.services.aggregation import compute_dataset_quality_stats
from app.schemas.dataset import (
    DatasetCreate, DatasetUpdate, DatasetResponse,
    DatasetItemAddRequest, DatasetItemRemoveRequest,
    DatasetReuseCreate, DatasetReuseResponse,
    DatasetVersionCreate, DatasetVersionResponse,
    DatasetReviewAction, DatasetReviewResponse,
    DatasetSubscriptionCreate, DatasetSubscriptionResponse,
    DatasetSubscriptionActionResponse, DatasetNotificationResponse
)

router = APIRouter()

VALID_REVIEW_ACTIONS = {"submit", "approve", "reject", "revoke"}

REVIEW_TRANSITIONS = {
    "submit": {"draft", "rejected"},
    "approve": {"pending_review"},
    "reject": {"pending_review"},
    "revoke": {"pending_review", "approved"},
}


def recalculate_dataset_stats(db: Session, dataset: Dataset):
    items = db.query(DatasetItem).filter(DatasetItem.dataset_id == dataset.id).all()
    op_ids = [item.operation_data_id for item in items]

    dataset.total_items = len(op_ids)

    if op_ids:
        stats = compute_dataset_quality_stats(db, op_ids)
        dataset.success_count = stats.success_count
        dataset.failure_count = stats.failure_count
        dataset.annotation_complete_rate = stats.annotation_complete_rate
        dataset.average_quality_score = stats.average_quality_score
        dataset.data_grade = stats.data_grade

    db.commit()


def _snapshot_version_stats(dataset: Dataset) -> dict:
    return {
        "total_items": dataset.total_items,
        "success_count": dataset.success_count,
        "failure_count": dataset.failure_count,
        "annotation_complete_rate": dataset.annotation_complete_rate,
        "average_quality_score": dataset.average_quality_score,
        "data_grade": dataset.data_grade,
    }


def _create_version_snapshot(db: Session, dataset: Dataset, change_description: str = None, created_by: str = None) -> DatasetVersion:
    # 版本号按数据集内已有快照递增，配合 (dataset_id, version_number) 唯一约束，
    # 保证并发创建时只有一个事务能成功
    max_number = db.query(func.max(DatasetVersion.version_number)).filter(
        DatasetVersion.dataset_id == dataset.id
    ).scalar() or 0
    stats = _snapshot_version_stats(dataset)

    version = DatasetVersion(
        dataset_id=dataset.id,
        version_number=max_number + 1,
        version_label=dataset.version,
        change_description=change_description,
        created_by=created_by,
        **stats
    )
    db.add(version)
    db.flush()
    return version


def _create_version_notifications(db: Session, dataset: Dataset, version: DatasetVersion) -> List[DatasetNotification]:
    """为当前有效订阅生成新版本通知并落库。

    只在调用方的事务内写入，不单独提交：通知与版本发布共同成功或共同失败。
    每个 (版本, 订阅) 最多一条通知，由唯一约束保证，重复发布不会产生重复通知。
    """
    subscriptions = db.query(DatasetSubscription).filter(
        DatasetSubscription.dataset_id == dataset.id,
        DatasetSubscription.status == "active",
        DatasetSubscription.notify_on_new_version == True
    ).all()

    notifications = []
    for sub in subscriptions:
        notification = DatasetNotification(
            dataset_id=dataset.id,
            dataset_version_id=version.id,
            subscription_id=sub.id,
            subscriber_team=sub.subscriber_team,
            contact_person=sub.contact_person,
            subscription_epoch=sub.epoch,
            version_label=version.version_label,
            message=f"数据集 '{dataset.name}' 已发布新版本 {version.version_label}",
            status="unread",
        )
        db.add(notification)
        notifications.append(notification)
    db.flush()
    return notifications


@router.get("/datasets", response_model=List[DatasetResponse], tags=["数据集管理"])
def list_datasets(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    robot_model_id: Optional[int] = Query(None, description="机型ID过滤"),
    scene_id: Optional[int] = Query(None, description="场景ID过滤"),
    skill_id: Optional[int] = Query(None, description="技能ID过滤"),
    is_published: Optional[bool] = Query(None, description="是否已发布"),
    review_status: Optional[str] = Query(None, description="审核状态过滤：draft/pending_review/approved/rejected"),
    owner_team: Optional[str] = Query(None, description="所属团队"),
    data_grade: Optional[str] = Query(None, description="数据等级"),
    keyword: Optional[str] = Query(None, description="搜索关键词"),
    db: Session = Depends(get_db)
):
    query = db.query(Dataset)
    if robot_model_id:
        query = query.filter(Dataset.robot_model_id == robot_model_id)
    if scene_id:
        query = query.filter(Dataset.scene_id == scene_id)
    if skill_id:
        query = query.filter(Dataset.skill_id == skill_id)
    if is_published is not None:
        query = query.filter(Dataset.is_published == is_published)
    if review_status:
        query = query.filter(Dataset.review_status == review_status)
    if owner_team:
        query = query.filter(Dataset.owner_team == owner_team)
    if data_grade:
        query = query.filter(Dataset.data_grade == data_grade)
    if keyword:
        query = query.filter(
            (Dataset.name.contains(keyword)) |
            (Dataset.description.contains(keyword))
        )
    return query.order_by(Dataset.created_at.desc()).offset(skip).limit(limit).all()


@router.get("/datasets/{dataset_id}", response_model=DatasetResponse, tags=["数据集管理"])
def get_dataset(dataset_id: int, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")
    return dataset


@router.post("/datasets", response_model=DatasetResponse, tags=["数据集管理"])
def create_dataset(data: DatasetCreate, db: Session = Depends(get_db)):
    robot_model = db.query(RobotModel).filter(RobotModel.id == data.robot_model_id).first()
    if not robot_model:
        raise HTTPException(status_code=400, detail="机型不存在")
    scene = db.query(Scene).filter(Scene.id == data.scene_id).first()
    if not scene:
        raise HTTPException(status_code=400, detail="场景不存在")

    dataset_data = data.model_dump(exclude={"operation_data_ids"})
    dataset = Dataset(**dataset_data)
    db.add(dataset)
    db.commit()
    db.refresh(dataset)

    _create_version_snapshot(db, dataset, change_description="初始版本")
    db.commit()
    db.refresh(dataset)

    if data.operation_data_ids:
        items = []
        for op_id in data.operation_data_ids:
            op = db.query(OperationData).filter(OperationData.id == op_id).first()
            if op:
                items.append(DatasetItem(dataset_id=dataset.id, operation_data_id=op_id))
        if items:
            db.bulk_save_objects(items)
            db.commit()
            db.refresh(dataset)
            recalculate_dataset_stats(db, dataset)

    return dataset


@router.put("/datasets/{dataset_id}", response_model=DatasetResponse, tags=["数据集管理"])
def update_dataset(dataset_id: int, data: DatasetUpdate, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")

    if dataset.review_status == "pending_review":
        raise HTTPException(status_code=400, detail="数据集正在审核中，无法修改")

    update_data = data.model_dump(exclude_unset=True)

    if dataset.review_status in ("approved", "published") and dataset.is_published:
        for field in ("name", "description", "robot_model_id", "scene_id", "skill_id", "tags", "license_info"):
            if field in update_data:
                raise HTTPException(status_code=400, detail="已发布的数据集需先提交新版本才能修改内容")

    if update_data.get("is_published") and not dataset.is_published:
        if dataset.review_status != "approved":
            raise HTTPException(status_code=400, detail="数据集尚未通过审核，无法发布")
        update_data["published_at"] = datetime.now(timezone.utc)

    for field, value in update_data.items():
        setattr(dataset, field, value)
    db.commit()
    db.refresh(dataset)
    return dataset


@router.delete("/datasets/{dataset_id}", tags=["数据集管理"])
def delete_dataset(dataset_id: int, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")
    if dataset.review_status == "pending_review":
        raise HTTPException(status_code=400, detail="数据集正在审核中，无法删除")
    db.delete(dataset)
    db.commit()
    return {"message": "删除成功"}


@router.get("/datasets/{dataset_id}/operation-ids", response_model=List[int], tags=["数据集管理"])
def get_dataset_operation_ids(dataset_id: int, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")
    items = db.query(DatasetItem).filter(DatasetItem.dataset_id == dataset_id).all()
    return [item.operation_data_id for item in items]


@router.post("/datasets/{dataset_id}/items", response_model=DatasetResponse, tags=["数据集管理"])
def add_items_to_dataset(dataset_id: int, req: DatasetItemAddRequest, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")

    if dataset.is_published and dataset.review_status in ("approved", "published"):
        raise HTTPException(status_code=400, detail="已发布的数据集需先创建新版本才能添加数据")

    existing_ids = set(
        db.query(DatasetItem.operation_data_id)
        .filter(DatasetItem.dataset_id == dataset_id)
        .all()
    )
    existing_ids = {x[0] for x in existing_ids}

    items = []
    for op_id in req.operation_data_ids:
        if op_id in existing_ids:
            continue
        op = db.query(OperationData).filter(OperationData.id == op_id).first()
        if op:
            items.append(DatasetItem(dataset_id=dataset_id, operation_data_id=op_id))

    if items:
        db.bulk_save_objects(items)
        db.commit()

    db.refresh(dataset)
    recalculate_dataset_stats(db, dataset)
    db.refresh(dataset)
    return dataset


@router.delete("/datasets/{dataset_id}/items", response_model=DatasetResponse, tags=["数据集管理"])
def remove_items_from_dataset(dataset_id: int, req: DatasetItemRemoveRequest, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")

    if dataset.is_published and dataset.review_status in ("approved", "published"):
        raise HTTPException(status_code=400, detail="已发布的数据集需先创建新版本才能移除数据")

    db.query(DatasetItem).filter(
        DatasetItem.dataset_id == dataset_id,
        DatasetItem.operation_data_id.in_(req.operation_data_ids)
    ).delete(synchronize_session=False)
    db.commit()

    db.refresh(dataset)
    recalculate_dataset_stats(db, dataset)
    db.refresh(dataset)
    return dataset


@router.post("/datasets/{dataset_id}/review", response_model=DatasetReviewResponse, tags=["数据集审核"])
def review_dataset(dataset_id: int, req: DatasetReviewAction, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")

    if req.action not in VALID_REVIEW_ACTIONS:
        raise HTTPException(status_code=400, detail=f"无效的审核动作，允许值：{', '.join(VALID_REVIEW_ACTIONS)}")

    if dataset.review_status not in REVIEW_TRANSITIONS.get(req.action, set()):
        raise HTTPException(
            status_code=400,
            detail=f"当前状态 '{dataset.review_status}' 不允许执行 '{req.action}' 操作"
        )

    if req.action == "submit":
        if dataset.total_items == 0:
            raise HTTPException(status_code=400, detail="数据集为空，无法提交审核")
        dataset.review_status = "pending_review"
        dataset.is_published = False

    elif req.action == "approve":
        # 条件更新作为原子比较并交换：并发的 approve 只有一个能把
        # pending_review 改为 approved，其余请求在此失败并回滚
        updated = db.query(Dataset).filter(
            Dataset.id == dataset.id,
            Dataset.review_status == "pending_review"
        ).update({Dataset.review_status: "approved"}, synchronize_session=False)
        if updated != 1:
            db.rollback()
            raise HTTPException(status_code=409, detail="数据集存在并发发布请求，本次操作已取消，请刷新后重试")
        dataset.review_status = "approved"

        review = DatasetReview(
            dataset_id=dataset.id,
            action="approve",
            reviewer=req.reviewer,
            review_notes=req.review_notes,
        )
        db.add(review)
        db.flush()

        version = _create_version_snapshot(
            db, dataset,
            change_description=req.review_notes or "审核通过，发布新版本",
            created_by=req.reviewer
        )
        dataset.current_version = version.version_number
        dataset.version = version.version_label

        dataset.is_published = True
        dataset.published_at = datetime.now(timezone.utc)

        review.dataset_version_id = version.id

        # 通知与版本发布在同一事务提交：共同成功或共同失败
        _create_version_notifications(db, dataset, version)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            raise HTTPException(status_code=409, detail="数据集发布冲突：版本或通知已存在，请刷新后重试")
        db.refresh(review)
        return review

    elif req.action == "reject":
        dataset.review_status = "rejected"
        dataset.is_published = False

    elif req.action == "revoke":
        dataset.review_status = "draft"
        dataset.is_published = False
        dataset.published_at = None

    review = DatasetReview(
        dataset_id=dataset.id,
        action=req.action,
        reviewer=req.reviewer,
        review_notes=req.review_notes,
    )
    db.add(review)
    db.commit()
    db.refresh(review)
    return review


@router.get("/datasets/{dataset_id}/reviews", response_model=List[DatasetReviewResponse], tags=["数据集审核"])
def list_dataset_reviews(dataset_id: int, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")
    return db.query(DatasetReview).filter(
        DatasetReview.dataset_id == dataset_id
    ).order_by(DatasetReview.created_at.desc()).all()


@router.post("/datasets/{dataset_id}/publish", response_model=DatasetResponse, tags=["数据集管理"])
def publish_dataset(dataset_id: int, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")
    if dataset.is_published:
        raise HTTPException(status_code=400, detail="数据集已发布")
    if dataset.review_status != "approved":
        raise HTTPException(status_code=400, detail="数据集尚未通过审核，无法发布")
    if dataset.total_items == 0:
        raise HTTPException(status_code=400, detail="数据集为空，无法发布")
    dataset.is_published = True
    dataset.published_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(dataset)
    return dataset


@router.post("/datasets/{dataset_id}/unpublish", response_model=DatasetResponse, tags=["数据集管理"])
def unpublish_dataset(dataset_id: int, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")
    dataset.is_published = False
    dataset.published_at = None
    dataset.review_status = "draft"
    db.commit()
    db.refresh(dataset)
    return dataset


@router.post("/datasets/{dataset_id}/versions", response_model=DatasetVersionResponse, tags=["数据集版本"])
def create_dataset_version(dataset_id: int, req: DatasetVersionCreate, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")

    if dataset.review_status == "pending_review":
        raise HTTPException(status_code=400, detail="数据集正在审核中，无法创建新版本")

    # 快照记录当前版本周期的最终状态，随后推进版本号进入下一周期
    version = _create_version_snapshot(db, dataset, change_description=req.change_description, created_by=req.created_by)
    dataset.current_version = version.version_number

    version_parts = dataset.version.split(".")
    if len(version_parts) == 2 and version_parts[1].isdigit():
        dataset.version = f"{version_parts[0]}.{int(version_parts[1]) + 1}"
    else:
        dataset.version = str(version.version_number + 1)
    dataset.review_status = "draft"
    dataset.is_published = False
    dataset.published_at = None

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="版本创建冲突：存在并发请求，请刷新后重试")
    db.refresh(version)
    return version


@router.get("/datasets/{dataset_id}/versions", response_model=List[DatasetVersionResponse], tags=["数据集版本"])
def list_dataset_versions(dataset_id: int, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")
    return db.query(DatasetVersion).filter(
        DatasetVersion.dataset_id == dataset_id
    ).order_by(DatasetVersion.version_number.desc()).all()


@router.get("/datasets/{dataset_id}/versions/{version_id}", response_model=DatasetVersionResponse, tags=["数据集版本"])
def get_dataset_version(dataset_id: int, version_id: int, db: Session = Depends(get_db)):
    version = db.query(DatasetVersion).filter(
        DatasetVersion.id == version_id,
        DatasetVersion.dataset_id == dataset_id
    ).first()
    if not version:
        raise HTTPException(status_code=404, detail="版本不存在")
    return version


def _restore_subscription(db: Session, subscription: DatasetSubscription, req: DatasetSubscriptionCreate) -> DatasetSubscription:
    """恢复已取消的订阅：复用原记录保留历史，周期递增，旧通知不随恢复复活。"""
    subscription.status = "active"
    subscription.epoch = (subscription.epoch or 0) + 1
    subscription.cancelled_at = None
    subscription.contact_person = req.contact_person
    subscription.notify_on_new_version = req.notify_on_new_version
    db.commit()
    db.refresh(subscription)
    return subscription


@router.post("/datasets/{dataset_id}/subscriptions", response_model=DatasetSubscriptionActionResponse, tags=["数据集订阅"])
def subscribe_dataset(dataset_id: int, req: DatasetSubscriptionCreate, response: Response, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")

    existing = db.query(DatasetSubscription).filter(
        DatasetSubscription.dataset_id == dataset_id,
        DatasetSubscription.subscriber_team == req.subscriber_team
    ).first()

    if existing and existing.status == "active":
        # 重复请求：幂等返回已有订阅，不修改状态
        return DatasetSubscriptionActionResponse(result="duplicate", subscription=existing)
    if existing:
        subscription = _restore_subscription(db, existing, req)
        return DatasetSubscriptionActionResponse(result="restored", subscription=subscription)

    subscription = DatasetSubscription(
        dataset_id=dataset_id,
        subscriber_team=req.subscriber_team,
        contact_person=req.contact_person,
        notify_on_new_version=req.notify_on_new_version,
        status="active",
        epoch=1,
    )
    db.add(subscription)
    try:
        db.commit()
    except IntegrityError:
        # 并发请求已抢先创建同一接收方的订阅，(dataset_id, subscriber_team)
        # 唯一约束保证任一时刻只有一条记录，这里回滚后按已有记录返回
        db.rollback()
        existing = db.query(DatasetSubscription).filter(
            DatasetSubscription.dataset_id == dataset_id,
            DatasetSubscription.subscriber_team == req.subscriber_team
        ).first()
        if existing is None:
            raise HTTPException(status_code=409, detail="订阅请求冲突，请重试")
        if existing.status == "active":
            return DatasetSubscriptionActionResponse(result="duplicate", subscription=existing)
        subscription = _restore_subscription(db, existing, req)
        return DatasetSubscriptionActionResponse(result="restored", subscription=subscription)

    db.refresh(subscription)
    response.status_code = 201
    return DatasetSubscriptionActionResponse(result="created", subscription=subscription)


@router.delete("/datasets/{dataset_id}/subscriptions/{subscription_id}", tags=["数据集订阅"])
def unsubscribe_dataset(dataset_id: int, subscription_id: int, db: Session = Depends(get_db)):
    subscription = db.query(DatasetSubscription).filter(
        DatasetSubscription.id == subscription_id,
        DatasetSubscription.dataset_id == dataset_id
    ).first()
    if not subscription:
        raise HTTPException(status_code=404, detail="订阅不存在")
    if subscription.status == "cancelled":
        return {"message": "订阅已处于取消状态", "subscription_id": subscription.id, "status": "cancelled"}
    # 软取消：保留订阅与通知历史，重新订阅时在同一记录上恢复
    subscription.status = "cancelled"
    subscription.cancelled_at = datetime.now(timezone.utc)
    db.commit()
    return {"message": "取消订阅成功", "subscription_id": subscription.id, "status": "cancelled"}


@router.get("/datasets/{dataset_id}/subscriptions", response_model=List[DatasetSubscriptionResponse], tags=["数据集订阅"])
def list_dataset_subscriptions(
    dataset_id: int,
    status: Optional[str] = Query(None, description="订阅状态过滤：active/cancelled"),
    db: Session = Depends(get_db)
):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")
    if status is not None and status not in ("active", "cancelled"):
        raise HTTPException(status_code=400, detail="无效的订阅状态，允许值：active/cancelled")
    query = db.query(DatasetSubscription).filter(DatasetSubscription.dataset_id == dataset_id)
    if status:
        query = query.filter(DatasetSubscription.status == status)
    return query.order_by(DatasetSubscription.created_at.desc()).all()


@router.get("/datasets/{dataset_id}/notifications", response_model=List[DatasetNotificationResponse], tags=["数据集通知"])
def list_dataset_notifications(
    dataset_id: int,
    status: Optional[str] = Query(None, description="通知状态过滤：unread/read"),
    db: Session = Depends(get_db)
):
    """数据集的通知历史：包含已取消订阅周期的历史通知，可追溯每条通知来自哪次订阅。"""
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")
    if status is not None and status not in ("unread", "read"):
        raise HTTPException(status_code=400, detail="无效的通知状态，允许值：unread/read")
    query = db.query(DatasetNotification).filter(DatasetNotification.dataset_id == dataset_id)
    if status:
        query = query.filter(DatasetNotification.status == status)
    return query.order_by(DatasetNotification.created_at.desc(), DatasetNotification.id.desc()).all()


@router.get("/notifications/unread", response_model=List[DatasetNotificationResponse], tags=["数据集通知"])
def list_unread_notifications(
    subscriber_team: str = Query(..., description="订阅团队"),
    db: Session = Depends(get_db)
):
    """接收方当前的未读通知。

    只统计仍处于有效状态订阅的当前订阅周期内产生的通知：
    取消订阅后旧通知不再出现，恢复订阅也不会复活取消前的通知。
    """
    return db.query(DatasetNotification).join(
        DatasetSubscription,
        DatasetNotification.subscription_id == DatasetSubscription.id
    ).filter(
        DatasetNotification.subscriber_team == subscriber_team,
        DatasetNotification.status == "unread",
        DatasetSubscription.status == "active",
        DatasetNotification.subscription_epoch == DatasetSubscription.epoch
    ).order_by(DatasetNotification.created_at.desc(), DatasetNotification.id.desc()).all()


@router.post("/notifications/{notification_id}/read", response_model=DatasetNotificationResponse, tags=["数据集通知"])
def mark_notification_read(notification_id: int, db: Session = Depends(get_db)):
    notification = db.query(DatasetNotification).filter(DatasetNotification.id == notification_id).first()
    if not notification:
        raise HTTPException(status_code=404, detail="通知不存在")
    if notification.status != "read":
        notification.status = "read"
        notification.read_at = datetime.now(timezone.utc)
        db.commit()
        db.refresh(notification)
    return notification


@router.get("/datasets/{dataset_id}/reuses", response_model=List[DatasetReuseResponse], tags=["数据集复用"])
def list_dataset_reuses(dataset_id: int, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")
    return db.query(DatasetReuse).filter(DatasetReuse.dataset_id == dataset_id).order_by(DatasetReuse.reuse_date.desc()).all()


@router.post("/dataset-reuses", response_model=DatasetReuseResponse, tags=["数据集复用"])
def create_dataset_reuse(data: DatasetReuseCreate, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == data.dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=400, detail="数据集不存在")
    if not dataset.is_published:
        raise HTTPException(status_code=400, detail="数据集尚未发布，无法复用")

    if data.dataset_version_id:
        version = db.query(DatasetVersion).filter(
            DatasetVersion.id == data.dataset_version_id,
            DatasetVersion.dataset_id == data.dataset_id
        ).first()
        if not version:
            raise HTTPException(status_code=400, detail="指定的数据集版本不存在")
    else:
        latest_version = db.query(DatasetVersion).filter(
            DatasetVersion.dataset_id == data.dataset_id
        ).order_by(DatasetVersion.version_number.desc()).first()
        if latest_version:
            data_dict = data.model_dump()
            data_dict["dataset_version_id"] = latest_version.id
            reuse = DatasetReuse(**data_dict)
        else:
            reuse = DatasetReuse(**data.model_dump())
        db.add(reuse)
        dataset.reuse_count = (dataset.reuse_count or 0) + 1
        db.commit()
        db.refresh(reuse)
        return reuse

    reuse = DatasetReuse(**data.model_dump())
    db.add(reuse)
    dataset.reuse_count = (dataset.reuse_count or 0) + 1
    db.commit()
    db.refresh(reuse)
    return reuse


@router.get("/dataset-reuses", response_model=List[DatasetReuseResponse], tags=["数据集复用"])
def list_all_dataset_reuses(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    dataset_id: Optional[int] = Query(None, description="数据集ID"),
    reusing_team: Optional[str] = Query(None, description="复用团队"),
    db: Session = Depends(get_db)
):
    query = db.query(DatasetReuse)
    if dataset_id:
        query = query.filter(DatasetReuse.dataset_id == dataset_id)
    if reusing_team:
        query = query.filter(DatasetReuse.reusing_team == reusing_team)
    return query.order_by(DatasetReuse.reuse_date.desc()).offset(skip).limit(limit).all()
