import importlib.metadata

from spark_plan_viz._analyzer import analyze_plan as analyze_plan
from spark_plan_viz._renderer import visualize_plan as visualize_plan
from spark_plan_viz._rules import Severity as Severity, Suggestion as Suggestion
from spark_plan_viz._types import KeyInfo as KeyInfo, PlanNode as PlanNode

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"  # Fallback for development mode
