import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from edgar import run_full_refresh, run_incremental_refresh
from database import db_has_data

logger = logging.getLogger(__name__)


async def startup_refresh():
    if not db_has_data():
        logger.info("Database empty — running full refresh on startup (this takes 25-30 minutes)")
        await run_full_refresh()
    else:
        logger.info("Database has data — running incremental refresh")
        await run_incremental_refresh()


def create_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()
    scheduler.add_job(run_incremental_refresh, trigger="cron", hour=2, minute=0)
    return scheduler
