import subprocess
import time

from preprocessing.dask.dask_cluster import DaskCluster
from preprocessing.utils.general import timeit
from dask.distributed import Client


class LocalDaskCluster(DaskCluster):

    def __init__(self, compose_file: str, services: tuple[str, str], timeout: int):
        self.compose_file = compose_file
        self.services = services
        self.timeout = timeout

    def get_cluster_url(self):
        return "tcp://localhost:8786"

    @classmethod
    def build(cls, num_workers: int, max_retries: int = 3, compose_file: str = "dask/docker-compose.yml",
              services: tuple[str, str] = ("redis", "scheduler", "worker"),
              timeout: int = 40
              ):

        s = 4

        cls.run_docker_compose(
            compose_file=compose_file,
            services=services,
            num_workers=num_workers,
            timeout=timeout,
            max_retries=max_retries
        )
        return cls(
            compose_file=compose_file,
            services=services,
            timeout=timeout
        )

    @classmethod
    def run_docker_compose(cls, compose_file: str, services: tuple[str, str], num_workers: int, max_retries: int,
                           timeout: int = 40):
        try:
            # Start all services, dynamically scaling workers
            subprocess.run([
                "docker", "compose", "-f", compose_file, "up", "-d", "--scale", f"worker={num_workers}"
            ], check=True)

            result = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True)
            print(f"🚀 Docker Compose started with {num_workers} workers.")
        except subprocess.CalledProcessError as e:
            raise RuntimeError("❌ Failed to start Docker Compose.") from e

        for attempt in range(1, max_retries + 1):
            print(f"\n🔁 Health check attempt {attempt}/{max_retries}...")
            unhealthy = cls.wait_for_healthy_services(compose_file, services, timeout)

            if not unhealthy:
                print("✅ All services are healthy!")
                return True

            if attempt < max_retries:
                print(f"⚠️ Unhealthy services: {unhealthy}. Restarting them...")
                for service in unhealthy:
                    try:
                        subprocess.run(["docker", "compose", "-f", compose_file, "restart", service], check=True)
                        print(f"🔄 Restarted {service}")
                    except subprocess.CalledProcessError:
                        print(f"❌ Failed to restart {service}")
            else:
                raise TimeoutError(f"⛔ Services still unhealthy after {max_retries} retries: {unhealthy}")

    @classmethod
    def wait_for_healthy_services(cls, compose_file: str, services: tuple[str, str], timeout: int) -> list:
        """Wait for services to become healthy. Returns list of unhealthy services."""
        start_time = time.time()
        unhealthy = []

        while time.time() - start_time < timeout:
            unhealthy = []
            for service in services:
                container_ids = cls.get_container_ids(compose_file, service)
                for container_id in container_ids:
                    health = cls.get_container_health(container_id)
                    print(f"🩺 {service} ({container_id}) → {health}")
                    if health != "healthy":
                        unhealthy.append(service)

            if not unhealthy:
                return []

            time.sleep(timeout / 2)

        return unhealthy

    @staticmethod
    def get_container_ids(compose_file: str, service: str) -> list[str]:
        """Get container IDs for a service (could be multiple workers)."""
        result = subprocess.run(
            ["docker", "compose", "-f", compose_file, "ps", "-q", service],
            capture_output=True, text=True, check=True
        )
        return [line.strip() for line in result.stdout.strip().splitlines() if line.strip()]

    @staticmethod
    def get_container_health(container_id: str) -> str:
        """Check container health using docker inspect."""
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Health.Status}}", container_id],
            capture_output=True, text=True
        )
        return result.stdout.strip() if result.returncode == 0 else "unknown"

    @timeit
    def delete_dask_cluster(self):
        try:
            # Step 1: Stop the cluster
            subprocess.run(["docker", "compose", "-f", self.compose_file, "down"], check=True)
            print("🧹 Docker Compose shutting down...")
        except subprocess.CalledProcessError as e:
            print("❌ Failed to stop Docker Compose:", e)
            raise

        # Step 2: Wait until containers are gone
        start_time = time.time()
        while time.time() - start_time < self.timeout:
            running = self.get_running_service_containers(self.compose_file, self.services)
            if not running:
                print("✅ All services are stopped and removed.")
                return
            print(f"⏳ Still stopping: {running}")
            time.sleep(5)

    @staticmethod
    def get_running_service_containers(compose_file: str, services: tuple[str, str]) -> list:
        """Return running container names matching compose service names."""
        result = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True)
        all_running = result.stdout.strip().splitlines()
        return [name for name in all_running if any(service in name for service in services)]

    def get_workers_count(self):
        client = Client(self.get_cluster_url())
        workers = client.scheduler_info()["workers"]
        return len(workers)