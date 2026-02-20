import matplotlib
import matplotlib.pyplot as plt
import re
from pathlib import Path
from collections import OrderedDict, defaultdict
import statistics as stats

RUNTIME_RGX = re.compile(r"Job Runtime:\s*([0-9]+)\s*ms")

def parse_runtime_ms(log_path: Path) -> int | None:
    try:
        last_ms = None
        with log_path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                m = RUNTIME_RGX.search(line)
                if m:
                    last_ms = int(m.group(1))
        return last_ms
    except FileNotFoundError:
        return None

def _parse_worker_runs(worker_dir: Path) -> list[int]:
    runtimes = []
    logs_dir = worker_dir / "logs"
    if not logs_dir.is_dir():
        return runtimes
    
    for run_dir in sorted((p for p in logs_dir.iterdir() if p.is_dir() and p.name.isdigit()),
                          key=lambda p: int(p.name)):
        log_path = run_dir / "flink.log"
        ms = parse_runtime_ms(log_path)
        if ms is not None:
            runtimes.append(ms)
    return runtimes

def collect_all_deployments(root_dir: str):
    root = Path(root_dir).expanduser().resolve()
    
    aggregated_results = {
        "flink": defaultdict(list),
        "flink_no_psl": defaultdict(list)
    }

    for deployment_dir in root.iterdir():
        if not deployment_dir.is_dir(): continue
        experiments_dir = deployment_dir / "experiments"
        if not experiments_dir.exists(): continue

        for suite in ["flink", "flink_no_psl"]:
            suite_dir = experiments_dir / suite
            if not suite_dir.is_dir(): continue

            for d in sorted((p for p in suite_dir.iterdir() if p.is_dir() and p.name.isdigit()),
                            key=lambda p: int(p.name)):
                idx = int(d.name)
                workers = 2 ** idx
                runs_ms = _parse_worker_runs(d)
                if runs_ms:
                    aggregated_results[suite][workers].extend(runs_ms)

    final_maps = {"flink": {}, "flink_no_psl": {}}
    for suite in ["flink", "flink_no_psl"]:
        for workers, times in aggregated_results[suite].items():
            if times:
                final_maps[suite][workers] = stats.fmean(times)
        final_maps[suite] = OrderedDict(sorted(final_maps[suite].items()))

    return final_maps["flink"], final_maps["flink_no_psl"]

def plot_flink_compare(flink_map, flink_no_psl_map, output=None):
    matplotlib.rcParams.update({'font.size': 22})
    plt.gcf().set_size_inches(30, 12)

    for experiment_name, experiment in [("HarborMaster", flink_map), ("Unprotected", flink_no_psl_map)]:
        if not experiment: continue
        x = list(experiment.keys())
        y = list(experiment.values())
        y = [int(y)/1000 for y in y] 

        all_points = list(zip(x, y))
        all_points.sort(key=lambda x: x[0])
        x, y = zip(*all_points)
        
        marker = '^' if experiment_name == "HarborMaster" else 'o'
        plt.plot(x, y, marker=marker, linewidth=5, markersize=15, label=experiment_name)

    plt.xlabel("Number of Flink Workers")
    plt.ylabel("Completion Time (s)")
    plt.xscale('log')
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=2, fontsize=24)
    
    all_x = set(flink_map.keys()) | set(flink_no_psl_map.keys())
    if all_x:
        plt.xticks(sorted(list(all_x)), [str(int(val)) for val in sorted(list(all_x))])
    plt.grid(True)
    
    if output:
        plt.savefig(output, bbox_inches="tight")
        print(f"Plot saved to {output}")
    else:
        plt.show()

if __name__ == "__main__":
    base_directory = "vldb_rev" 
    flink_map, flink_no_psl_map = collect_all_deployments(base_directory)
    
    print("\n" + "="*75)
    print(f"{'Workers':<8} | {'HarborMaster':<12} | {'Unprotected':<12} | {'Slowdown (%)':<15}")
    print("="*75)

    common_workers = set(flink_map.keys()) & set(flink_no_psl_map.keys())
    target_range_slowdowns = [] # specifically for 1-4 workers

    for w in sorted(common_workers):
        t_psl = flink_map[w]
        t_base = flink_no_psl_map[w]
        
        if t_psl > 0:
            # Formula: (1 - (Baseline / HarborMaster)) * 100
            pct = (1 - (t_base / t_psl)) * 100
            
            # Print row
            print(f"{w:<8} | {t_psl/1000:.2f}s        | {t_base/1000:.2f}s        | {pct:.2f}%")
            
            # Collect stats for 1-4 range
            if 1 <= w <= 4:
                target_range_slowdowns.append(pct)

    print("-" * 75)
    
    if target_range_slowdowns:
        avg_target = stats.mean(target_range_slowdowns)
        print(f"AVERAGE SLOWDOWN (Workers 1-4 only): {avg_target:.2f}%")
    else:
        print("No data found for workers 1-4.")
    
    print("-" * 75)
    
    plot_flink_compare(flink_map, flink_no_psl_map, output="flink_experiment_combined.pdf")