"""Hai Platform task-test payload: Monte-Carlo PI with validation markers.

Run on a training node by the hai-platform worker image. Uses torch gloo backend
(distributed) so it works on a CPU-only cluster. Prints an unambiguous
`PI_RESULT <float>` line that the terraform module greps for, plus
`TASK_RUNNER:EXIT_OK` / `TASK_RUNNER:EXIT_ERR` so the poller knows the outcome
even if the log tail is noisy.
"""
import math
import os
import time
import torch

start_time = time.time()
print("start_time:", time.strftime("%Y-%m-%d %H:%M:%S"))

# Number of samples == quantifies the error bar. 5e8 per process gives an error
# of roughly 1/sqrt(N) ~ 4e-5, far inside the module's validation tolerance.
TOTAL_SAMPLES = int(os.environ.get("PI_TOTAL_SAMPLES", "500000000"))


def calculate_pi(num_samples, device):
    points = torch.rand(num_samples, 2, device=device)
    distances = torch.norm(points, p=2, dim=1)
    inside = torch.sum(distances <= 1.0).item()
    return 4.0 * inside / num_samples


def main():
    simulate = int(os.environ.get("HFAI_SIMULATE", 0))
    local_rank = int(os.environ.get("LOCAL_RANK", 0))
    rank = int(os.environ.get("RANK", 0))
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    if simulate or not all(k in os.environ for k in ("MASTER_IP", "MASTER_PORT", "WORLD_SIZE")):
        # single-process fallback (no distributed env)
        local_pi = calculate_pi(TOTAL_SAMPLES, device)
        final_pi = local_pi
        world_size = 1
    else:
        ip = os.environ["MASTER_IP"]
        port = os.environ["MASTER_PORT"]
        hosts = int(os.environ["WORLD_SIZE"])
        rank = int(os.environ["RANK"])
        backend = "gloo"  # CPU-only cluster
        if torch.cuda.is_available():
            gpus = torch.cuda.device_count()
            world_size = hosts * gpus
            global_rank = rank * gpus + local_rank
        else:
            world_size = hosts
            global_rank = rank
        dist = __import__("torch.distributed", fromlist=["init_process_group", "all_gather", "destroy_process_group"])
        dist.init_process_group(
            backend=backend, init_method=f"tcp://{ip}:{port}",
            world_size=world_size, rank=global_rank,
        )
        local_samples = TOTAL_SAMPLES // world_size
        local_pi = calculate_pi(local_samples, device)
        pi_tensor = torch.tensor(local_pi, device=device).unsqueeze(0)
        gathered = [torch.zeros(1, device=device) for _ in range(world_size)]
        dist.all_gather(gathered, pi_tensor)
        final_pi = 0.0 if rank != 0 else sum(float(p.item()) for p in gathered) / world_size
        dist.destroy_process_group()

    if rank == 0:
        err = abs(final_pi - math.pi)
        print(f"PI_RESULT {final_pi:.10f}")
        print(f"PI_ERROR {err:.10f}")
        print(f"PI_WORLD_SIZE {world_size}")
        print(f"PI_TOTAL_SAMPLES {TOTAL_SAMPLES}")
        ok = err < 0.0005
        print(f"TASK_RUNNER:EXIT_OK" if ok else "TASK_RUNNER:EXIT_ERR")
    print(f"elapsed_seconds {(time.time() - start_time):.2f}")


if __name__ == "__main__":
    main()