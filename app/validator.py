import json
from dataclasses import dataclass
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schemas"


@dataclass
class ValidationResult:
    is_valid: bool
    errors: list[str]


class EventValidator:
    def __init__(self, schema_dir: Path = SCHEMA_DIR):
        self.schema_dir = Path(schema_dir)
        self.format_checker = FormatChecker()

        # Loaded once at startup
        self.envelope_validator = self._load_validator(
            self.schema_dir / "envelope.json"
        )

        self.payload_validators = {}

        # Discover event-specific schemas: <event_type>/<schema_version>.json
        for path in sorted(self.schema_dir.glob("*/*.json")):
            event_type = path.parent.name
            version = path.stem

            key = (event_type, version)

            self.payload_validators[key] = (
                self._load_validator(path)
            )

        if not self.payload_validators:
            raise RuntimeError("No payload schemas found")

    def _load_validator(self, path: Path):
        with path.open(encoding="utf-8") as file:
            schema = json.load(file)

        # Check that the schema itself is valid
        Draft202012Validator.check_schema(schema)

        # Create reusable validator
        return Draft202012Validator(
            schema,
            format_checker=self.format_checker
        )

    def validate(self, event: dict) -> ValidationResult:
        # 1. Validate envelope
        errors = [
            error.message
            for error in self.envelope_validator.iter_errors(event)
        ]

        if errors:
            return ValidationResult(False, errors)

        # 2. Find payload schema
        key = (
            event["event_type"],
            event["schema_version"]
        )

        validator = self.payload_validators.get(key)

        if validator is None:
            return ValidationResult(
                False,
                [f"Unsupported event type/version: {key}"]
            )

        # 3. Validate payload
        errors = [
            f"payload: {error.message}"
            for error in validator.iter_errors(event["payload"])
        ]

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors
        )
