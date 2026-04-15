"""
Parser: Excel - Alternativos (NAV + Movimientos).
"""

from __future__ import annotations

import calendar
import re
from pathlib import Path

import pandas as pd

from parsers.base import BaseExcelParser, ParseResult, ParsedRow, ParserStatus


class AlternativesExcelParser(BaseExcelParser):
    BANK_CODE = "system"
    ACCOUNT_TYPE = "alternatives"
    VERSION = "1.0.1"
    DESCRIPTION = "Parser para Excel de alternativos agregado a nivel sociedad/clase/estrategia/moneda"
    SUPPORTED_EXTENSIONS = [".xlsx", ".xls"]

    _EXCLUDED_SOCIETIES = {"Ecoterra", "El Faro"}
    _ENTITY_NAME_ALIASES = {
        "Ect Intl": "Ecoterra Internacional",
        "Ect RE": "Ecoterra RE",
        "Ect RE II": "Ecoterra RE II",
        "Ect RE III": "Ecoterra RE III",
    }
    _GROUP_KEYS = ["entity_name", "asset_class", "strategy", "currency"]

    def get_expected_columns(self) -> list[str]:
        return [
            "entity_name",
            "asset_class",
            "strategy",
            "currency",
            "nemo_reference",
            "year",
            "month",
            "closing_date",
            "ending_value",
            "movements_net",
            "profit_period",
            "movements_ytd",
            "profit_ytd",
        ]

    def map_columns(self, raw_columns: list[str]) -> dict[str, str]:
        return {}

    def parse(self, filepath: Path) -> ParseResult:
        file_hash = self.compute_file_hash(filepath)
        try:
            nav_long = self._load_sheet_long(filepath, sheet_name="NAV", expected_label="NAV")
            mov_long = self._load_sheet_long(filepath, sheet_name="Movimientos", expected_label="Movimiento")

            monthly_nav = self._build_monthly_nav(nav_long)
            monthly_movements = self._build_monthly_movements(mov_long)
            monthly = monthly_nav.merge(
                monthly_movements,
                how="left",
                on=self._GROUP_KEYS + ["year", "month", "closing_date"],
            )
            monthly = monthly.merge(
                self._build_group_nemo_reference(nav_long),
                how="left",
                on=self._GROUP_KEYS,
            )
            monthly["movements_net"] = monthly["movements_net"].fillna(0.0)
            monthly = monthly.sort_values(self._GROUP_KEYS + ["closing_date"]).reset_index(drop=True)

            monthly["previous_ending"] = monthly.groupby(self._GROUP_KEYS)["ending_value"].shift(1).fillna(0.0)
            monthly["profit_period"] = (
                monthly["ending_value"] - monthly["previous_ending"] - monthly["movements_net"]
            )
            monthly["movements_ytd"] = monthly.groupby(self._GROUP_KEYS + ["year"])["movements_net"].cumsum()
            monthly["profit_ytd"] = monthly.groupby(self._GROUP_KEYS + ["year"])["profit_period"].cumsum()
            for column in ("movements_net", "profit_period", "movements_ytd", "profit_ytd"):
                monthly.loc[monthly[column].abs() < 1e-9, column] = 0.0

            rows = [
                ParsedRow(
                    row_number=idx + 2,
                    data={
                        "entity_name": row.entity_name,
                        "asset_class": row.asset_class,
                        "strategy": row.strategy,
                        "currency": row.currency,
                        "nemo_reference": row.nemo_reference,
                        "year": int(row.year),
                        "month": int(row.month),
                        "closing_date": row.closing_date.isoformat(),
                        "ending_value": round(float(row.ending_value), 4),
                        "movements_net": round(float(row.movements_net), 4),
                        "profit_period": round(float(row.profit_period), 4),
                        "movements_ytd": round(float(row.movements_ytd), 4),
                        "profit_ytd": round(float(row.profit_ytd), 4),
                    },
                )
                for idx, row in enumerate(monthly.itertuples(index=False))
            ]

            return ParseResult(
                status=ParserStatus.SUCCESS,
                parser_name=self.get_parser_name(),
                parser_version=self.VERSION,
                source_file_hash=file_hash,
                rows=rows,
                bank_code="alternativos",
                warnings=[
                    "Sociedades excluidas en parser de alternativos: Ecoterra, El Faro."
                ],
            )
        except Exception as exc:
            return ParseResult(
                status=ParserStatus.ERROR,
                parser_name=self.get_parser_name(),
                parser_version=self.VERSION,
                source_file_hash=file_hash,
                errors=[str(exc)],
            )

    def validate(self, result: ParseResult) -> list[str]:
        errors: list[str] = []
        for row in result.rows:
            data = row.data or {}
            for field in (
                "entity_name",
                "asset_class",
                "strategy",
                "currency",
                "year",
                "month",
                "closing_date",
                "ending_value",
                "movements_net",
                "profit_period",
            ):
                if data.get(field) in (None, ""):
                    errors.append(f"Fila {row.row_number}: {field} vacio")
        return errors

    def detect(self, filepath: Path) -> float:
        if filepath.suffix.lower() not in self.SUPPORTED_EXTENSIONS:
            return 0.0
        name = filepath.stem.lower()
        if "alternativo" in name:
            return 0.95
        return 0.2

    def _load_sheet_long(self, filepath: Path, *, sheet_name: str, expected_label: str) -> pd.DataFrame:
        raw = pd.read_excel(filepath, sheet_name=sheet_name, header=None)
        metadata = pd.DataFrame(
            {
                "col_idx": list(range(1, raw.shape[1])),
                "currency": raw.iloc[0, 1:].tolist(),
                "label": raw.iloc[1, 1:].tolist(),
                "strategy": raw.iloc[2, 1:].tolist(),
                "asset_class": raw.iloc[3, 1:].tolist(),
                "nemo": raw.iloc[4, 1:].tolist(),
                "entity_name": raw.iloc[6, 1:].tolist(),
            }
        )
        metadata = metadata[
            (metadata["label"] == expected_label)
            & metadata["entity_name"].notna()
            & metadata["nemo"].notna()
        ].copy()
        metadata = metadata[~metadata["entity_name"].isin(self._EXCLUDED_SOCIETIES)].copy()

        # Build a set of (nemo, entity) pairs that have a USD counterpart,
        # so any non-USD column with the same nemo+entity can be excluded.
        # This handles EUR columns adjacent to USD (original logic) and also
        # non-adjacent non-USD columns (e.g. GBP funds where GBP and USD
        # columns are not immediately consecutive in the sheet).
        usd_nemo_entity = set(
            zip(
                metadata.loc[metadata["currency"].str.upper() == "USD", "nemo"].astype(str),
                metadata.loc[metadata["currency"].str.upper() == "USD", "entity_name"].astype(str),
            )
        )
        keep_mask = []
        for row in metadata.itertuples(index=False):
            keep = True
            if str(row.currency).upper() != "USD":
                nemo_key = (str(row.nemo), str(row.entity_name))
                if nemo_key in usd_nemo_entity:
                    keep = False
            keep_mask.append(keep)
        metadata = metadata.loc[keep_mask].copy()

        values = raw.iloc[9:, [0] + metadata["col_idx"].tolist()].copy()
        values = values.rename(columns={0: "date"})
        values["date"] = pd.to_datetime(values["date"], errors="coerce")
        values = values[values["date"].notna()].copy()

        long_df = values.melt(id_vars=["date"], var_name="col_idx", value_name="value")
        long_df["col_idx"] = long_df["col_idx"].astype(int)
        long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce").fillna(0.0)
        long_df = long_df.merge(metadata, on="col_idx", how="inner")
        long_df["entity_name"] = long_df["entity_name"].map(self._normalize_entity_name)
        long_df["asset_class"] = long_df["asset_class"].fillna("Sin clase en Excel")
        long_df["strategy"] = long_df["strategy"].fillna("Sin estrategia en Excel")
        long_df["year"] = long_df["date"].dt.year.astype(int)
        long_df["month"] = long_df["date"].dt.month.astype(int)
        return long_df

    def _build_monthly_nav(self, nav_long: pd.DataFrame) -> pd.DataFrame:
        nav_sorted = nav_long.sort_values(["col_idx", "date"]).copy()
        last_rows = nav_sorted.groupby(["col_idx", "year", "month"], as_index=False).last()
        month_end = pd.to_datetime(
            {
                "year": last_rows["year"],
                "month": last_rows["month"],
                "day": 1,
            }
        ) + pd.offsets.MonthEnd(0)
        last_rows["closing_date"] = month_end.dt.date
        monthly = (
            last_rows.groupby(self._GROUP_KEYS + ["year", "month", "closing_date"], as_index=False)["value"]
            .sum()
            .rename(columns={"value": "ending_value"})
        )
        return monthly

    def _build_monthly_movements(self, mov_long: pd.DataFrame) -> pd.DataFrame:
        monthly = (
            mov_long.groupby(self._GROUP_KEYS + ["year", "month"], as_index=False)["value"]
            .sum()
            .rename(columns={"value": "movements_net"})
        )
        # La hoja Movimientos viene con signo de cashflow del inversionista.
        # Reporting usa contribucion neta al activo: calls/subscripciones positivas,
        # distribuciones/retiros negativas.
        monthly["movements_net"] = monthly["movements_net"] * -1
        monthly["closing_date"] = monthly.apply(
            lambda row: pd.Timestamp(
                int(row["year"]),
                int(row["month"]),
                calendar.monthrange(int(row["year"]), int(row["month"]))[1],
            ).date(),
            axis=1,
        )
        return monthly

    def _build_group_nemo_reference(self, nav_long: pd.DataFrame) -> pd.DataFrame:
        refs = (
            nav_long[self._GROUP_KEYS + ["nemo"]]
            .drop_duplicates()
            .assign(
                nemo_reference=lambda df: df["nemo"].map(self._normalize_nemo_reference)
            )
            .groupby(self._GROUP_KEYS, as_index=False)["nemo_reference"]
            .agg(
                lambda values: next(
                    iter(sorted({str(value).strip() for value in values if str(value).strip()})),
                    "",
                )
            )
        )
        return refs

    @staticmethod
    def _normalize_nemo_reference(value: object) -> str:
        text = re.sub(r"\s+", "", str(value or "").strip())
        text = re.sub(r"[^A-Za-z0-9]", "", text)
        return text[:5].upper()

    @classmethod
    def _normalize_entity_name(cls, value: object) -> str:
        text = str(value or "").strip()
        return cls._ENTITY_NAME_ALIASES.get(text, text)
