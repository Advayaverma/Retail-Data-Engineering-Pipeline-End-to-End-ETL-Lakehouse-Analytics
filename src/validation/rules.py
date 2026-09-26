"""
Data Quality Validation Rules for Retail Data Engineering Pipeline.
Implements reusable, atomic checks for nulls, ranges, duplicates, timestamps, and referential integrity.
"""

import re
from datetime import datetime
from abc import ABC, abstractmethod


class ValidationResult:
    """Encapsulates the result of a single validation check."""

    def __init__(self, is_valid: bool, error_code: str = None, error_message: str = None):
        self.is_valid = is_valid
        self.error_code = error_code
        self.error_message = error_message


class BaseRule(ABC):
    """Abstract base class for validation rules."""

    def __init__(self, field_name: str, error_code: str):
        self.field_name = field_name
        self.error_code = error_code

    @abstractmethod
    def evaluate(self, record: dict) -> ValidationResult:
        """Evaluate record and return ValidationResult."""
        pass


class NotNullOrEmptyRule(BaseRule):
    """Validates that a field is present, non-null, and non-empty."""

    def __init__(self, field_name: str, error_code: str = "ERR_NULL_VALUE"):
        super().__init__(field_name, error_code)

    def evaluate(self, record: dict) -> ValidationResult:
        val = record.get(self.field_name)
        if val is None:
            return ValidationResult(False, self.error_code, f"Field '{self.field_name}' is None/Null")
        if isinstance(val, str) and not val.strip():
            return ValidationResult(False, self.error_code, f"Field '{self.field_name}' is empty string")
        return ValidationResult(True)


class NumericRangeRule(BaseRule):
    """Validates that a numeric value is within [min_val, max_val] bounds."""

    def __init__(
        self,
        field_name: str,
        min_val: float = None,
        max_val: float = None,
        error_code: str = "ERR_NUMERIC_OUT_OF_RANGE"
    ):
        super().__init__(field_name, error_code)
        self.min_val = min_val
        self.max_val = max_val

    def evaluate(self, record: dict) -> ValidationResult:
        raw_val = record.get(self.field_name)
        try:
            num = float(raw_val)
        except (ValueError, TypeError):
            return ValidationResult(False, f"{self.error_code}_TYPE", f"Field '{self.field_name}' cannot be cast to float: '{raw_val}'")

        if self.min_val is not None and num < self.min_val:
            return ValidationResult(False, self.error_code, f"Field '{self.field_name}' value {num} < min bound {self.min_val}")
        if self.max_val is not None and num > self.max_val:
            return ValidationResult(False, self.error_code, f"Field '{self.field_name}' value {num} > max bound {self.max_val}")
        return ValidationResult(True)


class TimestampFormatRule(BaseRule):
    """Validates that a timestamp string matches expected ISO format and is a valid calendar date."""

    def __init__(self, field_name: str, expected_format: str = "%Y-%m-%d %H:%M:%S", error_code: str = "ERR_INVALID_TIMESTAMP"):
        super().__init__(field_name, error_code)
        self.expected_format = expected_format

    def evaluate(self, record: dict) -> ValidationResult:
        raw_ts = str(record.get(self.field_name, "")).strip()
        if not raw_ts:
            return ValidationResult(False, self.error_code, f"Field '{self.field_name}' is empty")
        try:
            datetime.strptime(raw_ts, self.expected_format)
            return ValidationResult(True)
        except (ValueError, TypeError):
            return ValidationResult(False, self.error_code, f"Field '{self.field_name}' invalid timestamp string: '{raw_ts}'")


class ReferentialIntegrityRule(BaseRule):
    """Validates that a foreign key exists within a set of valid reference primary keys."""

    def __init__(self, field_name: str, reference_set: set, reference_name: str, error_code: str = "ERR_REF_INTEGRITY"):
        super().__init__(field_name, error_code)
        self.reference_set = reference_set
        self.reference_name = reference_name

    def evaluate(self, record: dict) -> ValidationResult:
        fk_val = record.get(self.field_name)
        if not fk_val or str(fk_val).strip() not in self.reference_set:
            return ValidationResult(False, self.error_code, f"Field '{self.field_name}' foreign key '{fk_val}' not found in {self.reference_name}")
        return ValidationResult(True)


class DuplicateTracker:
    """Stateful duplicate detector tracking observed primary key combinations."""

    def __init__(self, key_field: str = "transaction_id"):
        self.key_field = key_field
        self.seen_keys = set()

    def check_duplicate(self, record: dict) -> ValidationResult:
        key_val = record.get(self.key_field)
        if key_val in self.seen_keys:
            return ValidationResult(False, "ERR_DUPLICATE_RECORD", f"Duplicate primary key detected: {key_val}")
        self.seen_keys.add(key_val)
        return ValidationResult(True)
