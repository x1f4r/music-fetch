from __future__ import annotations

from dataclasses import dataclass

from .config import Settings
from .db import Database
from .service import JobManager


@dataclass
class AppContext:
    settings: Settings
    db: Database
    manager: JobManager


def create_context(*, recover_orphans: bool = False) -> AppContext:
    settings = Settings()
    db = Database(settings.db_path)
    manager = JobManager(settings, db, recover_orphans=recover_orphans)
    return AppContext(settings=settings, db=db, manager=manager)
