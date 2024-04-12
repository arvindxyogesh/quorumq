"""
PyTorch LSTM and GRU forecasting models.
Three architectures: vanilla LSTM, vanilla GRU, and BiLSTM with attention.
"""
import torch
import torch.nn as nn
from typing import Optional


class LSTMForecaster(nn.Module):
    """
    Multi-layer LSTM for sequence-to-horizon forecasting.
    Architecture: LSTM stack → dropout → linear projection
    """

    def __init__(
        self,
        input_size: int,
        hidden_size: int,
        num_layers: int,
        horizon: int,
        dropout: float = 0.2,
    ):
        super().__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers

        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.dropout = nn.Dropout(dropout)
        self.fc = nn.Linear(hidden_size, horizon)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, input_size)
        out, _ = self.lstm(x)
        out = self.dropout(out[:, -1, :])   # take last time step
        return self.fc(out)                  # (batch, horizon)


class GRUForecaster(nn.Module):
    """
    Multi-layer GRU — faster to train than LSTM, comparable accuracy.
    """

    def __init__(
        self,
        input_size: int,
        hidden_size: int,
        num_layers: int,
        horizon: int,
        dropout: float = 0.2,
    ):
        super().__init__()
        self.gru = nn.GRU(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.dropout = nn.Dropout(dropout)
        self.fc = nn.Linear(hidden_size, horizon)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.gru(x)
        out = self.dropout(out[:, -1, :])
        return self.fc(out)


class BiLSTMForecaster(nn.Module):
    """
    Bidirectional LSTM with additive attention mechanism.
    Attention over the time axis helps focus on the most relevant past steps.
    """

    def __init__(
        self,
        input_size: int,
        hidden_size: int,
        num_layers: int,
        horizon: int,
        dropout: float = 0.2,
    ):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            bidirectional=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.dropout = nn.Dropout(dropout)
        # Attention: project hidden states to scalar scores
        self.attn = nn.Linear(hidden_size * 2, 1)
        self.fc = nn.Linear(hidden_size * 2, horizon)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)              # (batch, seq, hidden*2)
        attn_weights = torch.softmax(self.attn(out), dim=1)   # (batch, seq, 1)
        context = (attn_weights * out).sum(dim=1)              # (batch, hidden*2)
        context = self.dropout(context)
        return self.fc(context)


MODEL_REGISTRY = {
    "lstm":   LSTMForecaster,
    "gru":    GRUForecaster,
    "bilstm": BiLSTMForecaster,
}


def build_model(config: dict) -> nn.Module:
    """Instantiate a model from config."""
    model_type = config["model"]["type"]
    if model_type not in MODEL_REGISTRY:
        raise ValueError(f"Unknown model type '{model_type}'. Choose from {list(MODEL_REGISTRY.keys())}")
    return MODEL_REGISTRY[model_type](
        input_size=config["model"].get("input_size", 1),
        hidden_size=config["model"]["hidden_size"],
        num_layers=config["model"]["num_layers"],
        horizon=config["model"]["horizon"],
        dropout=config["model"].get("dropout", 0.2),
    )
