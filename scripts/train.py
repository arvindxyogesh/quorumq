"""
Training script for deep learning time series forecasting.
Trains LSTM, GRU, or BiLSTM as specified in config/config.yaml.
"""
import json
import logging
import os
import sys

import numpy as np
import torch
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    with open("config/config.yaml") as f:
        config = yaml.safe_load(f)

    # ── Load data ──────────────────────────────────────────────────────────────
    data_path = config["data"]["path"]
    if not os.path.exists(data_path):
        logger.info("Generating synthetic energy data...")
        from data.generate_data import generate_energy_consumption
        generate_energy_consumption(output_path=data_path)

    import pandas as pd
    df = pd.read_csv(data_path, index_col=0, parse_dates=True)
    series = df[config["data"]["target_col"]].values.astype(np.float32)
    logger.info(f"Series length: {len(series)}")

    # ── Build data module ──────────────────────────────────────────────────────
    from src.data.dataset import TimeSeriesDataModule
    dm = TimeSeriesDataModule(config)
    train_loader, val_loader = dm.prepare(series)

    # ── Build model ────────────────────────────────────────────────────────────
    from src.models.lstm_model import build_model
    model = build_model(config)
    n_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    logger.info(f"Model: {config['model']['type'].upper()} | Params: {n_params:,}")

    # ── Train ──────────────────────────────────────────────────────────────────
    from src.training.trainer import Trainer
    trainer = Trainer(config, model)
    history = trainer.train(train_loader, val_loader)

    # ── Evaluate on test set ───────────────────────────────────────────────────
    from src.evaluation.metrics import evaluate_model
    metrics = evaluate_model(model, val_loader, dm, config)
    logger.info(f"Test metrics: {metrics}")

    # ── Save artifacts ─────────────────────────────────────────────────────────
    os.makedirs("artifacts", exist_ok=True)
    trainer.save("artifacts/model.pt")

    import pickle
    with open("artifacts/scaler.pkl", "wb") as f:
        pickle.dump(dm.scaler, f)

    report = {
        "model_type": config["model"]["type"],
        "n_params": n_params,
        "epochs_trained": len(history["train_loss"]),
        "best_val_loss": min(history["val_loss"]),
        "test_metrics": metrics,
    }
    os.makedirs("reports", exist_ok=True)
    with open("reports/training_report.json", "w") as f:
        json.dump(report, f, indent=2)

    print("\n========== RESULTS ==========")
    for k, v in metrics.items():
        print(f"  {k}: {v:.4f}")
    print(f"\nModel saved to artifacts/model.pt")


if __name__ == "__main__":
    main()
