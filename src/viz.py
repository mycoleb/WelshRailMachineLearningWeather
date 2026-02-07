from __future__ import annotations
import os
import numpy as np
import matplotlib.pyplot as plt

def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def plot_actual_vs_pred(y_true, y_pred, outpath: str) -> None:
    _ensure_dir(os.path.dirname(outpath))
    plt.figure()
    plt.scatter(y_true, y_pred, alpha=0.35)
    mn = float(min(np.min(y_true), np.min(y_pred)))
    mx = float(max(np.max(y_true), np.max(y_pred)))
    plt.plot([mn, mx], [mn, mx])
    plt.xlabel("Actual delay (min)")
    plt.ylabel("Predicted delay (min)")
    plt.title("Actual vs Predicted Delay")
    plt.tight_layout()
    plt.savefig(outpath, dpi=160)
    plt.close()

def plot_residuals(y_true, y_pred, outpath: str) -> None:
    _ensure_dir(os.path.dirname(outpath))
    resid = y_true - y_pred
    plt.figure()
    plt.hist(resid, bins=40)
    plt.xlabel("Residual (actual - predicted) minutes")
    plt.ylabel("Count")
    plt.title("Residual Distribution")
    plt.tight_layout()
    plt.savefig(outpath, dpi=160)
    plt.close()

def plot_feature_importance(model, feature_names, outpath: str, top_n: int = 18) -> None:
    _ensure_dir(os.path.dirname(outpath))
    imp = getattr(model, "feature_importances_", None)
    if imp is None:
        return
    order = np.argsort(imp)[::-1][:top_n]
    names = [feature_names[i] for i in order]
    vals = imp[order]

    plt.figure(figsize=(8, 5))
    plt.barh(range(len(names))[::-1], vals)
    plt.yticks(range(len(names))[::-1], names)
    plt.xlabel("Importance")
    plt.title("Top Feature Importances (Random Forest)")
    plt.tight_layout()
    plt.savefig(outpath, dpi=160)
    plt.close()

def plot_weather_sensitivity(df_joined, feature: str, target_col: str, outpath: str) -> None:
    """
    Quick "storytelling" plot: average delay by binned weather value.
    """
    _ensure_dir(os.path.dirname(outpath))
    if feature not in df_joined.columns:
        return

    s = df_joined[[feature, target_col]].dropna().copy()
    if s.empty:
        return

    # Bin into quantiles (robust for skew)
    s["bin"] = np.nan
    try:
        s["bin"] = pd.qcut(s[feature], q=10, duplicates="drop")
    except Exception:
        # fallback to fixed bins
        s["bin"] = pd.cut(s[feature], bins=10)

    g = s.groupby("bin")[target_col].mean().reset_index()
    plt.figure(figsize=(9, 4))
    plt.plot(range(len(g)), g[target_col].to_numpy(), marker="o")
    plt.xticks(range(len(g)), [str(b) for b in g["bin"]], rotation=45, ha="right")
    plt.ylabel("Mean delay (min)")
    plt.title(f"Delay sensitivity to {feature}")
    plt.tight_layout()
    plt.savefig(outpath, dpi=160)
    plt.close()
