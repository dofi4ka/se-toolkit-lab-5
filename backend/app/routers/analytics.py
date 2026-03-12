"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, distinct, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


def _lab_title_filter(lab: str) -> str:
    """Convert lab id (e.g. 'lab-04') to title fragment for matching (e.g. 'Lab 04')."""
    s = lab.strip().lower()
    if s.startswith("lab-"):
        return "Lab " + s[4:].strip()
    return lab.strip()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    title_part = _lab_title_filter(lab)
    lab_stmt = (
        select(ItemRecord.id)
        .where(ItemRecord.type == "lab", ItemRecord.title.contains(title_part))
        .limit(1)
    )
    lab_result = await session.exec(lab_stmt)
    lab_row = lab_result.first()
    if not lab_row:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]
    lab_id = lab_row[0]

    task_ids_stmt = select(ItemRecord.id).where(
        ItemRecord.parent_id == lab_id, ItemRecord.type == "task"
    )
    task_ids_result = await session.exec(task_ids_stmt)
    task_ids = [r[0] for r in task_ids_result.all()]

    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    bucket_case = case(
        (InteractionLog.score <= 25, "0-25"),
        ((InteractionLog.score > 25) & (InteractionLog.score <= 50), "26-50"),
        ((InteractionLog.score > 50) & (InteractionLog.score <= 75), "51-75"),
        (InteractionLog.score > 75, "76-100"),
    )
    stmt = (
        select(bucket_case.label("bucket"), func.count().label("count"))
        .where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.isnot(None),
        )
        .group_by(bucket_case)
    )
    result = await session.exec(stmt)
    rows = {r.bucket: r.count for r in result.all()}

    return [
        {"bucket": "0-25", "count": rows.get("0-25", 0)},
        {"bucket": "26-50", "count": rows.get("26-50", 0)},
        {"bucket": "51-75", "count": rows.get("51-75", 0)},
        {"bucket": "76-100", "count": rows.get("76-100", 0)},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""
    title_part = _lab_title_filter(lab)
    lab_stmt = (
        select(ItemRecord.id)
        .where(ItemRecord.type == "lab", ItemRecord.title.contains(title_part))
        .limit(1)
    )
    lab_result = await session.exec(lab_stmt)
    lab_row = lab_result.first()
    if not lab_row:
        return []
    lab_id = lab_row[0]

    task_stmt = (
        select(ItemRecord.id, ItemRecord.title)
        .where(ItemRecord.parent_id == lab_id, ItemRecord.type == "task")
        .order_by(ItemRecord.title)
    )
    task_result = await session.exec(task_stmt)
    tasks = list(task_result.all())

    out = []
    for task_id, task_title in tasks:
        agg = (
            select(
                func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
                func.count().label("attempts"),
            )
            .where(
                InteractionLog.item_id == task_id,
                InteractionLog.score.isnot(None),
            )
        )
        r = await session.exec(agg)
        row = r.first()
        if row and row.attempts:
            out.append({
                "task": task_title,
                "avg_score": float(row.avg_score) if row.avg_score is not None else 0,
                "attempts": row.attempts,
            })
        else:
            out.append({"task": task_title, "avg_score": 0.0, "attempts": 0})
    return out


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    title_part = _lab_title_filter(lab)
    lab_stmt = (
        select(ItemRecord.id)
        .where(ItemRecord.type == "lab", ItemRecord.title.contains(title_part))
        .limit(1)
    )
    lab_result = await session.exec(lab_stmt)
    lab_row = lab_result.first()
    if not lab_row:
        return []
    lab_id = lab_row[0]

    task_ids_stmt = select(ItemRecord.id).where(
        ItemRecord.parent_id == lab_id, ItemRecord.type == "task"
    )
    task_ids_result = await session.exec(task_ids_stmt)
    task_ids = [r[0] for r in task_ids_result.all()]
    if not task_ids:
        return []

    stmt = (
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count().label("submissions"),
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )
    result = await session.exec(stmt)
    return [{"date": str(r.date), "submissions": r.submissions} for r in result.all()]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    title_part = _lab_title_filter(lab)
    lab_stmt = (
        select(ItemRecord.id)
        .where(ItemRecord.type == "lab", ItemRecord.title.contains(title_part))
        .limit(1)
    )
    lab_result = await session.exec(lab_stmt)
    lab_row = lab_result.first()
    if not lab_row:
        return []
    lab_id = lab_row[0]

    task_ids_stmt = select(ItemRecord.id).where(
        ItemRecord.parent_id == lab_id, ItemRecord.type == "task"
    )
    task_ids_result = await session.exec(task_ids_stmt)
    task_ids = [r[0] for r in task_ids_result.all()]
    if not task_ids:
        return []

    stmt = (
        select(
            Learner.student_group.label("group"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(distinct(InteractionLog.learner_id)).label("students"),
        )
        .join(InteractionLog, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )
    result = await session.exec(stmt)
    return [
        {
            "group": r.group,
            "avg_score": float(r.avg_score) if r.avg_score is not None else 0,
            "students": r.students,
        }
        for r in result.all()
    ]
