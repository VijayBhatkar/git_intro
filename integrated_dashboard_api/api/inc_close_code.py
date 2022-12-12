from typing import List
from integrated_dashboard_api.db.DatabaseSession import DatabaseSession
from integrated_dashboard_api.db.DatabaseTables import SLARCACATEGORYLIST
from integrated_dashboard_api.core.config import config_db_details
from integrated_dashboard_api.core import cache

class CloseCode:
    def __init__(self) -> None:
        pass

    @cache.memcache
    def get_close_codes(self) -> List[str]:
        with DatabaseSession(**config_db_details) as session:
            result = session.query(SLARCACATEGORYLIST.CATEGORY_TEXT).distinct().all()
        return [row.CATEGORY_TEXT for row in result]
