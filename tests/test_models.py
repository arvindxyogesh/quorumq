"""Tests for deep learning time series components."""
import numpy as np
import torch
import pytest


@pytest.fixture
def config():
    return {
        "model": {
            "type": "lstm",
            "input_size": 1,
            "hidden_size": 16,
            "num_layers": 1,
            "dropout": 0.0,
            "seq_len": 30,
            "horizon": 7,
        },
        "data": {"path": "data/energy.csv", "target_col": "consumption", "n_test": 60},
        "training": {"epochs": 5, "batch_size": 16, "lr": 1e-3, "weight_decay": 0, "patience": 5},
    }


def test_lstm_forward(config):
    from src.models.lstm_model import LSTMForecaster
    model = LSTMForecaster(input_size=1, hidden_size=16, num_layers=1, horizon=7)
    x = torch.randn(4, 30, 1)
    out = model(x)
    assert out.shape == (4, 7)


def test_gru_forward(config):
    from src.models.lstm_model import GRUForecaster
    model = GRUForecaster(input_size=1, hidden_size=16, num_layers=1, horizon=7)
    x = torch.randn(4, 30, 1)
    out = model(x)
    assert out.shape == (4, 7)


def test_bilstm_forward(config):
    from src.models.lstm_model import BiLSTMForecaster
    model = BiLSTMForecaster(input_size=1, hidden_size=16, num_layers=1, horizon=7)
    x = torch.randn(4, 30, 1)
    out = model(x)
    assert out.shape == (4, 7)


def test_sliding_window_dataset():
    from src.data.dataset import SlidingWindowDataset
    data = np.arange(100, dtype=np.float32)
    ds = SlidingWindowDataset(data, seq_len=10, horizon=5)
    assert len(ds) == 100 - 10 - 5 + 1
    X, y = ds[0]
    assert X.shape == (10, 1)
    assert y.shape == (5,)


def test_build_model(config):
    from src.models.lstm_model import build_model
    model = build_model(config)
    x = torch.randn(2, 30, 1)
    out = model(x)
    assert out.shape == (2, 7)
