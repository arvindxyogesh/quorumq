"""
Inference script: loads a trained model and produces future-step forecasts.
"""
import logging
import os
import sys

import numpy as np
import pandas as pd
import pickle
import torch
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    with open("config/config.yaml") as f:
        config = yaml.safe_load(f)

    # Load scaler
    with open("artifacts/scaler.pkl", "rb") as f:
        scaler = pickle.load(f)

    # Load series
    df = pd.read_csv(config["data"]["path"], index_col=0, parse_dates=True)
    series = df[config["data"]["target_col"]].values.astype(np.float32)

    # Build and load model
    from src.models.lstm_model import build_model
    model = build_model(config)
    ckpt = torch.load("artifacts/model.pt", map_location="cpu")
    model.load_state_dict(ckpt["model_state"])
    model.eval()

    # Build input window from end of series
    seq_len = config["model"]["seq_len"]
    horizon = config["model"]["horizon"]

    window = series[-seq_len:]
    window_scaled = scaler.transform(window.reshape(-1, 1)).flatten()
    X = torch.tensor(window_scaled, dtype=torch.float32).unsqueeze(0).unsqueeze(-1)  # (1, seq_len, 1)

    with torch.no_grad():
        pred_scaled = model(X).numpy().flatten()

    predictions = scaler.inverse_transform(pred_scaled.reshape(-1, 1)).flatten()

    last_date = pd.to_datetime(df.index[-1])
    future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=horizon, freq="D")

    result = pd.DataFrame({"date": future_dates, "forecast": predictions.round(2)})
    print("\n========== FORECAST ==========")
    print(result.to_string(index=False))
    result.to_csv("artifacts/forecast.csv", index=False)
    logger.info("Forecast saved to artifacts/forecast.csv")


if __name__ == "__main__":
    main()
