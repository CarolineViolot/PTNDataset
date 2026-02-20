import json
import random
from pathlib import Path
from typing import Dict, List
import pandas as pd


def read_json_into_pandas(INPUT_JSON, col_oi=None):
    with open(INPUT_JSON) as f:
        posts = json.load(f)

    posts_df = []
    for channel_url in posts:
        df = pd.DataFrame(posts[channel_url])
        if col_oi:
            df = df[col_oi]
        df['channel_url'] = channel_url
        posts_df.append(df)
    posts_df = pd.concat(posts_df)
    return posts_df


def _build_channel_to_worker(channel_stats: pd.DataFrame, n_workers: int, random_state: int = 42) -> Dict[str, int]:
    """Greedy-pack channels into workers by descending byte weight."""
    weight_map = dict(zip(channel_stats["channel_handle"], channel_stats["sum_sizes_bytes"]))
    channels = sorted(weight_map, key=weight_map.get, reverse=True)
    random.Random(random_state).shuffle(channels)

    worker_load = [0] * n_workers
    channel_to_worker = {}
    for ch in channels:
        w = min(range(n_workers), key=lambda i: worker_load[i])
        channel_to_worker[ch] = w
        worker_load[w] += weight_map[ch]
    return channel_to_worker


def _write_attribution_files(df: pd.DataFrame, output_dir: Path, n_batches: int, n_apis: int, n_sessions: int) -> List[Dict]:
    """Write one JSON per (batch, api, session) and return metadata rows."""
    records = []
    for batch in range(n_batches):
        (output_dir / str(batch + 1)).mkdir(parents=True, exist_ok=True)
        for api in range(n_apis):
            for session in range(n_sessions):
                sub = df[(df.batch_idx == batch) & (df.api_idx == api) & (df.session_idx == session)]
                payload = {ch: grp[["id", "mime_type"]].to_dict("records") for ch, grp in sub.groupby("channel_url", sort=False)}

                path = output_dir / str(batch + 1) / f"manifest_api{api}_{session}.json"
                path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

                records.append({"batch": batch + 1, "api": api, "session": session,
                                 "filename": f"{batch + 1}/{path.name}",
                                 "n_channels": len(payload), "video_rows": len(sub),
                                 "sum_sizes_bytes": int(sub["size"].sum())})
    return records


def divide_dataset_collect_text_with_video_attribution(
    channel_handles, posts_df, n_apis, n_sessions, n_batches,
    output_dir="api_distribution_media/", random_state=42,
):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Clean and filter
    selected = {h.strip() for h in channel_handles if isinstance(h, str) and h.strip()}
    df = posts_df[posts_df["channel_url"].isin(selected)].copy()
    df = df.dropna(subset=["channel_url", "id", "mime_type", "size"])
    df["size"] = pd.to_numeric(df["size"], errors="coerce").dropna().astype("int64")
    df["channel_url"] = df["channel_url"].astype(str).str.strip()

    # Assign channels to (batch, api, session) workers
    channel_stats = df.groupby("channel_url").agg(channel_handle=("channel_url", "first"), sum_sizes_bytes=("size", "sum")).reset_index(drop=True)
    n_workers = n_apis * n_sessions * n_batches
    channel_to_worker = _build_channel_to_worker(channel_stats, n_workers, random_state)

    df["worker_idx"] = df["channel_url"].map(channel_to_worker)
    df["batch_idx"] = df["worker_idx"] // (n_apis * n_sessions)
    df["api_idx"] = (df["worker_idx"] % (n_apis * n_sessions)) // n_sessions
    df["session_idx"] = df["worker_idx"] % n_sessions

    # Write files and mapping
    records = _write_attribution_files(df, output_dir, n_batches, n_apis, n_sessions)
    mapping_path = output_dir / "dataset_mapping.csv"
    pd.DataFrame(records).to_csv(mapping_path, index=False)

    print(f"✓ {len(records)} JSONs written to {output_dir}, mapping at {mapping_path}")
    return records, df