import os
import importlib
from pathlib import Path


def upload_export_folder_to_kaggle(export_dir, dataset_ref, kaggle_api_token, version_notes=None):
    export_path = Path(export_dir)

    if not export_path.exists():
        raise FileNotFoundError(f"Export folder not found: {export_path}")

    if not dataset_ref:
        raise ValueError("dataset_ref is required.")

    if not kaggle_api_token:
        raise ValueError("kaggle_api_token is required.")

    os.environ["KAGGLE_API_TOKEN"] = kaggle_api_token

    kagglehub = importlib.import_module("kagglehub")

    return kagglehub.dataset_upload(
        dataset_ref,
        str(export_path),
        version_notes=version_notes,
    )