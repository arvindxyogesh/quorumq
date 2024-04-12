"""Evaluation metrics for deep learning forecasting."""
import logging
from typing import Dict

import numpy as np
import torch
from torch.utils.data import DataLoader

logger = logging.getLogger(__name__)


def rmse(actual: np.ndarray, predicted: np.ndarray) -> float:
    return float(np.sqrt(np.mean((actual - predicted) ** 2)))


def mae(actual: np.ndarray, predicted: np.ndarray) -> float:
    return float(np.mean(np.abs(actual - predicted)))


def smape(actual: np.ndarray, predicted: np.ndarray, eps: float = 1e-8) -> float:
    denom = (np.abs(actual) + np.abs(predicted)) / 2 + eps
    return float(np.mean(np.abs(actual - predicted) / denom) * 100)


def evaluate_model(model, loader: DataLoader, data_module, config: dict) -> Dict[str, float]:
    """Run model on loader, inverse-transform, and compute metrics."""
    device = next(model.parameters()).device
    model.eval()
    actuals, preds = [], []

    with torch.no_grad():
        for X_batch, y_batch in loader:
            out = model(X_batch.to(device)).cpu().numpy()
            preds.append(out)
            actuals.append(y_batch.numpy())

    preds   = np.concatenate(preds,   axis=0).flatten()
    actuals = np.concatenate(actuals, axis=0).flatten()

    # Inverse-transform to original scale
    preds   = data_module.inverse_transform(preds)
    actuals = data_module.inverse_transform(actuals)

    return {
        "rmse":  round(rmse(actuals, preds), 4),
        "mae":   round(mae(actuals, preds), 4),
        "smape": round(smape(actuals, preds), 4),
    }
