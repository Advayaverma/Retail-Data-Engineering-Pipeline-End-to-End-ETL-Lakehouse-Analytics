"""
Databricks Runtime Abstraction & Compatibility Layer.

Detects runtime environment (Databricks vs. Local) and provides safe fallbacks
for dbutils, path resolution, and Unity Catalog volume routing.

Author: Retail Data Engineering Project
Target: Celebal Technologies Data Engineer Evaluation
"""

import os
import sys
from src.utils.logger import get_logger

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def is_running_on_databricks() -> bool:
    """Check if code is running inside a live Databricks runtime."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ or os.path.exists("/databricks")


def get_lakehouse_path(layer: str, table_name: str = "") -> str:
    """
    Resolve data path depending on environment:
    - In Databricks: Points to Unity Catalog Volume or DBFS mount (e.g. /Volumes/retail_catalog/...)
    - In Local: Points to local project data/{layer}/{table_name}
    """
    if is_running_on_databricks():
        catalog = os.environ.get("DATABRICKS_CATALOG", "retail_catalog")
        return f"/Volumes/{catalog}/{layer}/{table_name}"
    else:
        return os.path.join(PROJECT_ROOT, "data", layer, table_name)


class MockDBUtils:
    """Safe fallback for dbutils when running outside Databricks."""

    class FS:
        @staticmethod
        def ls(path: str):
            if os.path.isdir(path):
                return [{"path": os.path.join(path, f), "name": f} for f in os.listdir(path)]
            return []

        @staticmethod
        def cp(src: str, dst: str):
            import shutil
            shutil.copy(src, dst)

        @staticmethod
        def rm(path: str, recurse: bool = False):
            import shutil
            if os.path.isdir(path):
                shutil.rmtree(path) if recurse else os.rmdir(path)
            elif os.path.isfile(path):
                os.remove(path)

    class Widgets:
        def __init__(self):
            self._widgets = {}

        def text(self, name: str, defaultValue: str = "", label: str = ""):
            self._widgets[name] = defaultValue

        def get(self, name: str) -> str:
            return os.environ.get(name.upper(), self._widgets.get(name, ""))

    def __init__(self):
        self.fs = self.FS()
        self.widgets = self.Widgets()


def get_dbutils(spark=None):
    """
    Retrieve real dbutils if on Databricks, or MockDBUtils if running locally.
    """
    if is_running_on_databricks():
        try:
            import IPython
            return IPython.get_ipython().user_ns.get("dbutils")
        except Exception:
            pass
    return MockDBUtils()
