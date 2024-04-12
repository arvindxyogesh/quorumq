# Deep Learning Time Series Forecasting

A production-grade **PyTorch** time series forecasting system supporting **LSTM**, **GRU**, and **BiLSTM with attention**. Features a proper sliding-window dataset, early stopping, Huber loss, gradient clipping, and MinMax scaling with no data leakage.

## Architecture

```
ts-deep-learning-forecasting/
├── config/config.yaml          # Model architecture, training hyper-params, data paths
├── data/generate_data.py       # Synthetic daily energy consumption (2-year, realistic)
├── src/
│   ├── data/
│   │   └── dataset.py          # SlidingWindowDataset + TimeSeriesDataModule (scaler, loaders)
│   ├── models/
│   │   └── lstm_model.py       # LSTMForecaster, GRUForecaster, BiLSTMForecaster (attention)
│   ├── training/
│   │   └── trainer.py          # Huber loss, Adam, ReduceLROnPlateau, EarlyStopping
│   └── evaluation/
│       └── metrics.py          # RMSE, MAE, SMAPE with inverse-transform
├── scripts/
│   ├── train.py                # Full training pipeline
│   └── predict.py              # Load model + produce n-step ahead forecast
├── artifacts/                  # model.pt, scaler.pkl, forecast.csv
└── reports/                    # training_report.json
```

## Model Pipeline

```
Raw Series (daily)
      │
      ▼
MinMax Scaling (fit on train only)
      │
      ▼
SlidingWindowDataset → (batch, seq_len, 1) windows
      │
      ▼
LSTM / GRU / BiLSTM+Attention
      │
      ▼
Huber Loss + Adam + ReduceLROnPlateau
      │
      ▼
EarlyStopping (restore best weights)
      │
      ▼
Inverse-Transform → RMSE / MAE / SMAPE
```

## Quick Start

```bash
pip install -r requirements.txt

# Train model (auto-generates data if missing)
python scripts/train.py

# Forecast future steps
python scripts/predict.py
```

## Switching Model Architecture

Edit `config/config.yaml`:

```yaml
model:
  type: bilstm     # lstm | gru | bilstm
  hidden_size: 256
  num_layers: 3
  seq_len: 90
  horizon: 14
```

## Engineering Highlights

| Feature | Implementation |
|---|---|
| **No leakage** | Scaler fit on training split only; test uses `transform()` |
| **Overlapping windows** | `SlidingWindowDataset` with configurable `seq_len` and `horizon` |
| **Gradient clipping** | `clip_grad_norm_(max=1.0)` prevents exploding gradients |
| **Huber loss** | Robust to outlier spikes in energy data |
| **Best-weight restore** | Trainer saves best checkpoint and restores after early stopping |
| **BiLSTM attention** | Per-timestep attention scores focus on most informative past steps |

## Running Tests

```bash
pytest tests/ -v
```
