"""
Sliding window dataset for PyTorch time series models.
Creates (input_seq, target) pairs from a univariate or multivariate series.
"""
import logging
from typing import Tuple

import numpy as np
import torch
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import MinMaxScaler

logger = logging.getLogger(__name__)


class SlidingWindowDataset(Dataset):
    """
    Converts a time series into supervised learning pairs.
    Each sample: (window of past `seq_len` steps) → (next `horizon` steps)
    """

    def __init__(self, data: np.ndarray, seq_len: int, horizon: int):
        self.seq_len = seq_len
        self.horizon = horizon
        self.X, self.y = self._build_windows(data)

    def _build_windows(self, data: np.ndarray) -> Tuple[torch.Tensor, torch.Tensor]:
        X, y = [], []
        for i in range(len(data) - self.seq_len - self.horizon + 1):
            X.append(data[i : i + self.seq_len])
            y.append(data[i + self.seq_len : i + self.seq_len + self.horizon])
        X = np.array(X, dtype=np.float32)
        y = np.array(y, dtype=np.float32)
        # Ensure shape: (N, seq_len, features) and (N, horizon)
        if X.ndim == 2:
            X = X[:, :, np.newaxis]
        return torch.from_numpy(X), torch.from_numpy(y)

    def __len__(self) -> int:
        return len(self.X)

    def __getitem__(self, idx: int) -> Tuple[torch.Tensor, torch.Tensor]:
        return self.X[idx], self.y[idx]


class TimeSeriesDataModule:
    """Handles scaling, splitting, and DataLoader creation."""

    def __init__(self, config: dict):
        self.config = config["data"]
        self.seq_len = config["model"]["seq_len"]
        self.horizon = config["model"]["horizon"]
        self.batch_size = config["training"]["batch_size"]
        self.scaler = MinMaxScaler(feature_range=(-1, 1))

    def prepare(self, series: np.ndarray) -> Tuple[DataLoader, DataLoader]:
        """Scale, split, wrap in DataLoaders."""
        n_test = self.config.get("n_test", 120)
        train_raw = series[:-n_test]
        test_raw  = series[-n_test - self.seq_len:]   # include lookback for test

        # Fit scaler on train only — no leakage
        train_scaled = self.scaler.fit_transform(train_raw.reshape(-1, 1)).flatten()
        test_scaled  = self.scaler.transform(test_raw.reshape(-1, 1)).flatten()

        train_ds = SlidingWindowDataset(train_scaled, self.seq_len, self.horizon)
        test_ds  = SlidingWindowDataset(test_scaled,  self.seq_len, self.horizon)

        train_loader = DataLoader(train_ds, batch_size=self.batch_size, shuffle=True,  num_workers=0)
        test_loader  = DataLoader(test_ds,  batch_size=self.batch_size, shuffle=False, num_workers=0)

        logger.info(f"Train windows: {len(train_ds)} | Test windows: {len(test_ds)}")
        return train_loader, test_loader

    def inverse_transform(self, scaled: np.ndarray) -> np.ndarray:
        return self.scaler.inverse_transform(scaled.reshape(-1, 1)).flatten()
