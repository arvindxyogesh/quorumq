"""
PyTorch training loop with early stopping and learning rate scheduling.
"""
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import torch
import torch.nn as nn
from torch.optim import Adam
from torch.optim.lr_scheduler import ReduceLROnPlateau
from torch.utils.data import DataLoader

logger = logging.getLogger(__name__)


class EarlyStopping:
    """Stop training when validation loss stops improving."""

    def __init__(self, patience: int = 10, min_delta: float = 1e-4):
        self.patience = patience
        self.min_delta = min_delta
        self.counter = 0
        self.best_loss = np.inf
        self.triggered = False

    def step(self, val_loss: float) -> bool:
        if val_loss < self.best_loss - self.min_delta:
            self.best_loss = val_loss
            self.counter = 0
        else:
            self.counter += 1
            if self.counter >= self.patience:
                self.triggered = True
        return self.triggered


class Trainer:
    """Training orchestrator for PyTorch time series models."""

    def __init__(self, config: dict, model: nn.Module):
        self.config = config["training"]
        self.model = model
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model.to(self.device)

        self.criterion = nn.HuberLoss()    # Huber: less sensitive to outliers than MSE
        self.optimizer = Adam(
            model.parameters(),
            lr=self.config.get("lr", 1e-3),
            weight_decay=self.config.get("weight_decay", 1e-4),
        )
        self.scheduler = ReduceLROnPlateau(
            self.optimizer, patience=5, factor=0.5, verbose=False
        )
        self.early_stopping = EarlyStopping(
            patience=self.config.get("patience", 15)
        )
        self.history: Dict[str, List[float]] = {"train_loss": [], "val_loss": []}

    def _run_epoch(self, loader: DataLoader, train: bool) -> float:
        self.model.train(train)
        total_loss = 0.0
        with torch.set_grad_enabled(train):
            for X_batch, y_batch in loader:
                X_batch = X_batch.to(self.device)
                y_batch = y_batch.to(self.device)
                preds = self.model(X_batch)
                loss = self.criterion(preds, y_batch)
                if train:
                    self.optimizer.zero_grad()
                    loss.backward()
                    nn.utils.clip_grad_norm_(self.model.parameters(), max_norm=1.0)
                    self.optimizer.step()
                total_loss += loss.item() * len(X_batch)
        return total_loss / len(loader.dataset)

    def train(
        self,
        train_loader: DataLoader,
        val_loader: DataLoader,
    ) -> Dict[str, List[float]]:
        """Full training loop."""
        n_epochs = self.config.get("epochs", 100)
        best_state = None

        logger.info(f"Training on {self.device} for up to {n_epochs} epochs")

        for epoch in range(1, n_epochs + 1):
            train_loss = self._run_epoch(train_loader, train=True)
            val_loss   = self._run_epoch(val_loader,   train=False)
            self.history["train_loss"].append(train_loss)
            self.history["val_loss"].append(val_loss)
            self.scheduler.step(val_loss)

            if val_loss <= self.early_stopping.best_loss:
                best_state = {k: v.clone() for k, v in self.model.state_dict().items()}

            if epoch % 10 == 0 or epoch == 1:
                lr = self.optimizer.param_groups[0]["lr"]
                logger.info(f"Epoch {epoch:>4}/{n_epochs}  train={train_loss:.4f}  val={val_loss:.4f}  lr={lr:.2e}")

            if self.early_stopping.step(val_loss):
                logger.info(f"Early stopping at epoch {epoch}")
                break

        if best_state is not None:
            self.model.load_state_dict(best_state)
            logger.info("Restored best model weights")

        return self.history

    def save(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        torch.save({
            "model_state": self.model.state_dict(),
            "history": self.history,
        }, path)
        logger.info(f"Model saved → {path}")
