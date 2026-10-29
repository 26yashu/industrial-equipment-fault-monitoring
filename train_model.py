"""Train an Isolation Forest on simulated NORMAL equipment readings."""
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from config import MODEL_PATH

FEATURES = ["temperature_c", "vibration_mm_s", "pressure_bar", "rpm"]

def main():
    rng = np.random.default_rng(42)
    n = 2500
    normal = pd.DataFrame({
        "temperature_c": rng.normal(65, 4, n).clip(45, 78),
        "vibration_mm_s": rng.normal(2.0, 0.45, n).clip(0.5, 3.5),
        "pressure_bar": rng.normal(6.0, 0.35, n).clip(4.8, 7.2),
        "rpm": rng.normal(1500, 55, n).clip(1300, 1700).astype(int),
    })
    model = IsolationForest(
        n_estimators=150,
        contamination=0.03,
        random_state=42
    )
    model.fit(normal[FEATURES])
    joblib.dump(model, MODEL_PATH)
    print(f"Model trained on {len(normal)} simulated normal readings.")
    print(f"Saved model to: {MODEL_PATH}")
    print("Note: this is a teaching/demo model trained on synthetic baseline data.")

if __name__ == "__main__":
    main()
