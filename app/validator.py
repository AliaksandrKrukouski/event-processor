from dataclasses import dataclass
from pathlib import Path
import json

from jsonschema import Draft202012Validator, FormatChecker

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schemas"


@dataclass
class ValidationResult:
    is_valid: bool
    errors: list[str]


def load_schema(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def validate_envelope(event: dict) -> list[str]:
    schema = load_schema(SCHEMA_DIR / "envelope.json")

    validator = Draft202012Validator(
        schema,
        format_checker=FormatChecker()
    )

    return [
        error.message
        for error in validator.iter_errors(event)
    ]


def validate_payload(event: dict) -> list[str]:
    event_type = event["event_type"]
    version = event["schema_version"]

    schema_path = SCHEMA_DIR / event_type / f"{version}.json"

    if not schema_path.is_file():
        return [
            f"Unsupported event type or schema version: "
            f"{event_type}/{version}"
        ]

    schema = load_schema(schema_path)

    validator = Draft202012Validator(schema)

    return [
        f"payload: {error.message}"
        for error in validator.iter_errors(event["payload"])
    ]


def validate_event(event: dict) -> ValidationResult:
    envelope_errors = validate_envelope(event)

    if envelope_errors:
        return ValidationResult(
            is_valid=False,
            errors=envelope_errors
        )

    payload_errors = validate_payload(event)

    return ValidationResult(
        is_valid=not payload_errors,
        errors=payload_errors
    )
