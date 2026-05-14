"""Schema mapper: applies YAML-defined column rename + type casting rules."""

from __future__ import annotations

from typing import Any

import pandas as pd

from ingestion_engine.utils.logger import get_logger

logger = get_logger(__name__)

# DuckDB/pandas type mapping
TYPE_MAP = {
    "varchar": "object",
    "string": "object",
    "str": "object",
    "text": "object",
    "integer": "Int64",
    "int": "Int64",
    "bigint": "Int64",
    "long": "Int64",
    "double": "float64",
    "float": "float64",
    "real": "float64",
    "boolean": "boolean",
    "bool": "boolean",
    "timestamp": "datetime64[ns]",
    "date": "object",
    "json": "object",
}


class SchemaMapper:
    """Applies schema mapping rules from YAML config to a DataFrame."""

    def __init__(self, mapping: dict[str, Any]):
        """
        Args:
            mapping: Dict of {source_col: {target, type, required, default, validation}}
        """
        self.mapping = mapping

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all mapping rules to the DataFrame."""
        if not self.mapping:
            return df

        df = df.copy()

        # 1. Rename columns
        rename_map = {}
        for src_col, rules in self.mapping.items():
            if src_col in df.columns:
                target = rules.get("target", src_col)
                if target != src_col:
                    rename_map[src_col] = target
            elif src_col.startswith("$."):
                # JSONPath flattened columns (from pd.json_normalize)
                flat_col = src_col.lstrip("$.").replace(".", ".")
                if flat_col in df.columns:
                    target = rules.get("target", flat_col)
                    rename_map[flat_col] = target

        if rename_map:
            df = df.rename(columns=rename_map)
            logger.debug(f"Renamed {len(rename_map)} columns")

        # 2. Fill defaults
        for src_col, rules in self.mapping.items():
            target = rules.get("target", src_col)
            default = rules.get("default")
            if target in df.columns and default is not None:
                df[target] = df[target].fillna(default)

        # 3. Cast types
        for src_col, rules in self.mapping.items():
            target = rules.get("target", src_col)
            col_type = rules.get("type", "").lower()
            if target in df.columns and col_type:
                df = self._cast_column(df, target, col_type)

        # 4. Validate ranges
        for src_col, rules in self.mapping.items():
            target = rules.get("target", src_col)
            validation = rules.get("validation", {})
            if target in df.columns and validation:
                df = self._apply_validation(df, target, validation, rules.get("required", False))

        return df

    @staticmethod
    def _cast_column(df: pd.DataFrame, col: str, col_type: str) -> pd.DataFrame:
        """Cast a column to the target type."""
        pandas_type = TYPE_MAP.get(col_type, col_type)
        try:
            if pandas_type == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            elif pandas_type in ("Int64",):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(pandas_type)
            elif pandas_type == "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            elif pandas_type == "boolean":
                df[col] = df[col].astype("boolean")
            else:
                df[col] = df[col].astype("object")
        except Exception as e:
            logger.warning(f"Type cast failed for column '{col}' to '{col_type}': {e}")
        return df

    @staticmethod
    def _apply_validation(
        df: pd.DataFrame,
        col: str,
        validation: dict[str, Any],
        required: bool,
    ) -> pd.DataFrame:
        """Apply range validation — clamp or nullify out-of-range values."""
        min_val = validation.get("min")
        max_val = validation.get("max")

        if min_val is not None:
            mask = pd.to_numeric(df[col], errors="coerce") < min_val
            if mask.any():
                logger.debug(f"Column '{col}': {mask.sum()} values below min={min_val} set to NaN")
                df.loc[mask, col] = None

        if max_val is not None:
            mask = pd.to_numeric(df[col], errors="coerce") > max_val
            if mask.any():
                logger.debug(f"Column '{col}': {mask.sum()} values above max={max_val} set to NaN")
                df.loc[mask, col] = None

        return df
