import json
import os
import shutil
import tempfile
from pathlib import Path

from requests import HTTPError


def _parse_dataset_ref(dataset_ref):
    normalized_ref = (dataset_ref or "").strip()
    if "/" not in normalized_ref:
        raise ValueError("BILLBOARD_URL must be in the format '<owner>/<dataset-slug>'.")

    owner, dataset_name = normalized_ref.split("/", 1)
    if not owner or not dataset_name:
        raise ValueError("BILLBOARD_URL must include both owner and dataset slug.")

    return normalized_ref, owner, dataset_name


def _extract_api_key(kaggle_api_token):
    token_value = (kaggle_api_token or "").strip()

    if not token_value:
        raise ValueError("KAGGLE_API_TOKEN is empty.")

    if token_value.startswith("{"):
        try:
            token_payload = json.loads(token_value)
        except json.JSONDecodeError as exc:
            raise ValueError("KAGGLE_API_TOKEN looks like JSON but is invalid.") from exc

        token_key = str(token_payload.get("key", "")).strip()
        if not token_key:
            raise ValueError("KAGGLE_API_TOKEN JSON must contain non-empty 'key'.")
        return token_key, "kaggle_api_token_json"

    return token_value, "kaggle_api_token_raw"


def upload_export_folder_to_kaggle(
    export_dir,
    dataset_ref,
    kaggle_api_token,
    version_notes=None,
):
    export_path = Path(export_dir)
    csv_files = sorted(export_path.glob("*.csv"))

    if not csv_files:
        raise ValueError(f"No CSV files found in {export_dir}")

    dataset_ref, dataset_owner, dataset_name = _parse_dataset_ref(dataset_ref)
    auth_username = dataset_owner
    auth_key, key_source = _extract_api_key(kaggle_api_token)

    os.environ["KAGGLE_USERNAME"] = auth_username
    os.environ["KAGGLE_KEY"] = auth_key

    from kaggle.api.kaggle_api_extended import KaggleApi

    with tempfile.TemporaryDirectory(prefix="billboard_kaggle_upload_") as staging_dir:
        staging_path = Path(staging_dir)

        for csv_file in csv_files:
            shutil.copy2(csv_file, staging_path / csv_file.name)

        metadata = {
            "id": dataset_ref,
            "title": dataset_name.replace("-", " ").replace("_", " ").title(),
            "licenses": [{"name": "CC0-1.0"}],
        }
        (staging_path / "dataset-metadata.json").write_text(
            json.dumps(metadata, indent=2),
            encoding="utf-8",
        )

        api = KaggleApi()
        try:
            api.authenticate()
            api.dataset_create_version(
                folder=str(staging_path),
                version_notes=version_notes or "Airflow automated update",
                convert_to_csv=False,
                delete_old_versions=False,
                quiet=False,
            )
        except HTTPError as exc:
            status_code = getattr(exc.response, "status_code", None)
            if status_code == 401:
                mismatch_hint = ""
                if auth_username != dataset_owner:
                    mismatch_hint = (
                        f" The auth user '{auth_username}' differs from dataset owner "
                        f"'{dataset_owner}'; ensure this user has write access."
                    )

                raise ValueError(
                    "Kaggle upload failed with 401 Unauthorized. "
                    "Username is derived from BILLBOARD_URL owner and key from KAGGLE_API_TOKEN. "
                    f"Key source: {key_source}."
                    + mismatch_hint
                ) from exc
            raise

    return [str(path) for path in csv_files]
