from typing import List, Optional
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import (
    Dataset, DatasetItem, DatasetReuse,
    DatasetVersion, DatasetReview, DatasetSubscription,
    DatasetSubscriptionEvent,
    OperationData, Annotation, RobotModel, Scene
)
from app.services.aggregation import compute_dataset_quality_stats
from app.services import subscription as subscription_service
from app.schemas.dataset import (
    DatasetCreate, DatasetUpdate, DatasetResponse,
    DatasetItemAddRequest, DatasetItemRemoveRequest,
    DatasetReuseCreate, DatasetReuseResponse,
    DatasetVersionCreate, DatasetVersionResponse,
    DatasetReviewAction, DatasetReviewResponse,
    DatasetSubscriptionCreate, DatasetSubscriptionResponse,
    DatasetSubscriptionResultResponse, DatasetSubscriptionEventResponse,
    DatasetNotificationResponse,
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

    version = DatasetVersion(
        dataset_id=dataset.id,
        version_number=1,
        version_label=dataset.version,
        change_description="初始版本",
        **_snapshot_version_stats(dataset)
    )
    db.add(version)
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
        db.add(DatasetReview(
            dataset_id=dataset.id,
            action="submit",
            reviewer=req.reviewer,
            review_notes=req.review_notes,
        ))
        db.commit()
        db.refresh(dataset)
        return db.query(DatasetReview).filter(
            DatasetReview.dataset_id == dataset.id
        ).order_by(DatasetReview.id.desc()).first()

    elif req.action == "approve":
        # 审核记录、版本快照、通知在同一事务内落库，共同成功或共同失败
        dataset.review_status = "approved"
        review, version, notifications = subscription_service.release_approved_dataset(
            db, dataset, reviewer=req.reviewer, review_notes=req.review_notes
        )
        try:
            db.commit()
        except Exception:
            db.rollback()
            raise
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

    # 仅生成草稿版本快照，不发布、不通知；通知只在审核通过发布时产生
    new_version = subscription_service.create_next_version(
        db, dataset,
        change_description=req.change_description,
        created_by=req.created_by,
    )
    dataset.review_status = "draft"
    dataset.is_published = False
    dataset.published_at = None

    db.commit()
    db.refresh(new_version)
    return new_version


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


@router.post("/datasets/{dataset_id}/subscriptions",
             response_model=DatasetSubscriptionResultResponse, tags=["数据集订阅"])
def subscribe_dataset(dataset_id: int, req: DatasetSubscriptionCreate, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")

    subscription, result = subscription_service.subscribe(
        db,
        dataset,
        subscriber_team=req.subscriber_team,
        contact_person=req.contact_person,
        notify_on_new_version=req.notify_on_new_version,
    )
    payload = DatasetSubscriptionResponse.model_validate(subscription).model_dump()
    payload["result"] = result
    return payload


@router.delete("/datasets/{dataset_id}/subscriptions/by-team/{subscriber_team}",
               tags=["数据集订阅"])
def unsubscribe_dataset_by_team(dataset_id: int, subscriber_team: str, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")

    subscription = subscription_service.unsubscribe(db, dataset_id, subscriber_team)
    if subscription is None:
        raise HTTPException(status_code=404, detail="有效订阅不存在")
    return {"message": "取消订阅成功", "subscription_id": subscription.id}


@router.delete("/datasets/{dataset_id}/subscriptions/{subscription_id}", tags=["数据集订阅"])
def unsubscribe_dataset(dataset_id: int, subscription_id: int, db: Session = Depends(get_db)):
    subscription = db.query(DatasetSubscription).filter(
        DatasetSubscription.id == subscription_id,
        DatasetSubscription.dataset_id == dataset_id
    ).first()
    if not subscription:
        raise HTTPException(status_code=404, detail="订阅不存在")

    cancelled = subscription_service.unsubscribe(
        db, dataset_id, subscription.subscriber_team
    )
    if cancelled is None:
        raise HTTPException(status_code=400, detail="订阅已取消")
    return {"message": "取消订阅成功", "subscription_id": cancelled.id}


@router.get("/datasets/{dataset_id}/subscriptions",
            response_model=List[DatasetSubscriptionResponse], tags=["数据集订阅"])
def list_dataset_subscriptions(
    dataset_id: int,
    active_only: bool = Query(False, description="仅返回有效订阅"),
    db: Session = Depends(get_db)
):
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")
    query = db.query(DatasetSubscription).filter(
        DatasetSubscription.dataset_id == dataset_id
    )
    if active_only:
        query = query.filter(DatasetSubscription.status == "active")
    return query.order_by(DatasetSubscription.created_at.desc()).all()


@router.get("/datasets/{dataset_id}/subscriptions/{subscription_id}/events",
            response_model=List[DatasetSubscriptionEventResponse], tags=["数据集订阅"])
def list_subscription_events(dataset_id: int, subscription_id: int, db: Session = Depends(get_db)):
    subscription = db.query(DatasetSubscription).filter(
        DatasetSubscription.id == subscription_id,
        DatasetSubscription.dataset_id == dataset_id
    ).first()
    if not subscription:
        raise HTTPException(status_code=404, detail="订阅不存在")
    return db.query(DatasetSubscriptionEvent).filter(
        DatasetSubscriptionEvent.subscription_id == subscription_id
    ).order_by(DatasetSubscriptionEvent.id.asc()).all()


@router.get("/notifications",
            response_model=List[DatasetNotificationResponse], tags=["通知"])
def list_notifications(
    subscriber_team: Optional[str] = Query(None, description="接收团队过滤"),
    dataset_id: Optional[int] = Query(None, description="数据集ID过滤"),
    unread_only: bool = Query(False, description="仅返回未读通知"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db)
):
    return subscription_service.list_notifications(
        db,
        subscriber_team=subscriber_team,
        dataset_id=dataset_id,
        unread_only=unread_only,
        skip=skip,
        limit=limit,
    )


@router.post("/notifications/{notification_id}/read",
             response_model=DatasetNotificationResponse, tags=["通知"])
def mark_notification_read(notification_id: int, db: Session = Depends(get_db)):
    notification = subscription_service.mark_notification_read(db, notification_id)
    if notification is None:
        raise HTTPException(status_code=404, detail="通知不存在")
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
