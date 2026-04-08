"""Backfill SSOT payloads in monthly_metrics_normalized from persisted tables only.

This script does not parse PDFs. It rebuilds canonical/derived/instrument payloads by
running DataLoadingService.sync_normalized_for_account_year over existing account-years.
"""

from __future__ import annotations

from collections import defaultdict

from backend.db.models import Account, MonthlyClosing
from backend.db.session import get_session_factory
from backend.services.data_loading_service import DataLoadingService


def main() -> None:
    factory = get_session_factory()
    db = factory()
    try:
        pairs = db.query(MonthlyClosing.account_id, MonthlyClosing.year).distinct().all()
        years_by_account: dict[int, set[int]] = defaultdict(set)
        for account_id, year in pairs:
            if account_id is None or year is None:
                continue
            years_by_account[int(account_id)].add(int(year))

        loader = DataLoadingService(db)
        refreshed = 0
        for account_id, years in years_by_account.items():
            account = db.query(Account).filter(Account.id == account_id).first()
            if account is None:
                continue
            for year in sorted(years):
                loader.sync_normalized_for_account_year(account=account, year=year)
                refreshed += 1

        db.commit()
        print(f"SSOT backfill OK (no PDF parsing). account-year refreshed: {refreshed}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
