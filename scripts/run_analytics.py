import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import sys, uuid
from src.analytics.queries import calculate_analytics
from src.analytics.recommendations import generate_reorder_recommendations, sync_recommendations
rid = sys.argv[1] if len(sys.argv) > 1 else f"run-{uuid.uuid4().hex[:8]}"
for fn in (calculate_analytics, generate_reorder_recommendations, sync_recommendations):
    print(fn.__name__, fn(rid))