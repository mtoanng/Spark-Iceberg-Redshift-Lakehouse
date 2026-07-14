"""Compatibility entrypoint for the unified Spark ML recommendation job.

Training and recommendation generation now run together in
etl/ml/spark_recommendations.py so the AWS path stays simple.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from etl.ml.spark_recommendations import main


if __name__ == "__main__":
    sys.exit(main())
