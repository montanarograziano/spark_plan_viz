import importlib.metadata

from spark_plan_viz.visualize_plan import visualize_plan as visualize_plan

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"  # Fallback for development mode
