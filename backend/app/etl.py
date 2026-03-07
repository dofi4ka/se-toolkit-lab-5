"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime, timezone

import httpx
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    auth = (settings.autochecker_email, settings.autochecker_password)
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{settings.autochecker_api_url.rstrip('/')}/api/items",
            auth=auth,
        )
        r.raise_for_status()
        return r.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API."""
    auth = (settings.autochecker_email, settings.autochecker_password)
    all_logs: list[dict] = []
    since_param = since.isoformat() if since else None

    async with httpx.AsyncClient() as client:
        while True:
            params: dict = {"limit": 500}
            if since_param:
                params["since"] = since_param
            r = await client.get(
                f"{settings.autochecker_api_url.rstrip('/')}/api/logs",
                auth=auth,
                params=params,
            )
            r.raise_for_status()
            data = r.json()
            logs = data.get("logs", [])
            all_logs.extend(logs)
            if not data.get("has_more", False):
                break
            if not logs:
                break
            last = logs[-1]
            since_param = last.get("submitted_at")
            if not since_param:
                break

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database."""
    created = 0
    lab_by_short_id: dict[str, ItemRecord] = {}

    labs = [x for x in items if x.get("type") == "lab"]
    for lab in labs:
        title = lab.get("title", "")
        short_id = lab.get("lab", "")
        result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab", ItemRecord.title == title
            )
        )
        existing = result.first()
        if existing:
            lab_by_short_id[short_id] = existing
        else:
            new_lab = ItemRecord(type="lab", title=title)
            session.add(new_lab)
            await session.flush()
            lab_by_short_id[short_id] = new_lab
            created += 1

    tasks = [x for x in items if x.get("type") == "task"]
    for task in tasks:
        title = task.get("title", "")
        lab_short = task.get("lab", "")
        parent = lab_by_short_id.get(lab_short)
        if not parent or not parent.id:
            continue
        result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == title,
                ItemRecord.parent_id == parent.id,
            )
        )
        if result.first():
            continue
        session.add(
            ItemRecord(type="task", title=title, parent_id=parent.id)
        )
        created += 1

    await session.commit()
    return created


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database."""
    title_lookup: dict[tuple[str | None, str | None], str] = {}
    for it in items_catalog:
        lab_id = it.get("lab")
        task_id = it.get("task") if it.get("type") == "task" else None
        title_lookup[(lab_id, task_id)] = it.get("title", "")

    created = 0
    for log in logs:
        lab_short = log.get("lab")
        task_short = log.get("task")
        title = title_lookup.get((lab_short, task_short))
        if not title:
            continue

        if task_short is None:
            result = await session.exec(
                select(ItemRecord).where(
                    ItemRecord.type == "lab", ItemRecord.title == title
                )
            )
        else:
            lab_title = title_lookup.get((lab_short, None))
            if not lab_title:
                continue
            lab_result = await session.exec(
                select(ItemRecord).where(
                    ItemRecord.type == "lab", ItemRecord.title == lab_title
                )
            )
            lab_item = lab_result.first()
            if not lab_item or not lab_item.id:
                continue
            result = await session.exec(
                select(ItemRecord).where(
                    ItemRecord.type == "task",
                    ItemRecord.title == title,
                    ItemRecord.parent_id == lab_item.id,
                )
            )
        item = result.first()
        if not item or not item.id:
            continue

        external_id = log.get("id")
        if external_id is None:
            continue
        result = await session.exec(
            select(InteractionLog).where(InteractionLog.external_id == external_id)
        )
        if result.first():
            continue

        student_id = str(log.get("student_id", ""))
        result = await session.exec(
            select(Learner).where(Learner.external_id == student_id)
        )
        learner = result.first()
        if not learner:
            learner = Learner(
                external_id=student_id,
                student_group=str(log.get("group", "")),
            )
            session.add(learner)
            await session.flush()
        if not learner.id:
            continue

        submitted_at = log.get("submitted_at")
        if submitted_at:
            try:
                created_at = datetime.fromisoformat(
                    submitted_at.replace("Z", "+00:00")
                ).replace(tzinfo=None)
            except (ValueError, TypeError):
                created_at = datetime.now(timezone.utc).replace(tzinfo=None)
        else:
            created_at = datetime.now(timezone.utc).replace(tzinfo=None)

        session.add(
            InteractionLog(
                external_id=external_id,
                learner_id=learner.id,
                item_id=item.id,
                kind="attempt",
                score=log.get("score"),
                checks_passed=log.get("passed"),
                checks_total=log.get("total"),
                created_at=created_at,
            )
        )
        created += 1

    await session.commit()
    return created


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline."""
    items_raw = await fetch_items()
    await load_items(items_raw, session)

    result = await session.exec(
        select(InteractionLog).order_by(
            InteractionLog.created_at.desc()  # type: ignore[union-attr]
        ).limit(1)
    )
    last_row = result.first()
    last_ts = last_row.created_at if last_row else None
    since = last_ts if last_ts else None

    logs_raw = await fetch_logs(since=since)
    new_records = await load_logs(logs_raw, items_raw, session)

    count_result = await session.exec(select(InteractionLog))
    total_records = len(list(count_result.all()))

    return {
        "new_records": new_records,
        "total_records": total_records,
    }
