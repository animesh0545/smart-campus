import pathlib
import subprocess
import sys

SERVICES = pathlib.Path(__file__).resolve().parents[1]
ROOT = SERVICES.parent
GEN = SERVICES / ".gen"

sys.path.insert(0, str(SERVICES))
sys.path.insert(0, str(GEN))

# Generate the gRPC stubs the same way the Dockerfile does
if not (GEN / "campus_pb2.py").exists():
    GEN.mkdir(exist_ok=True)
    subprocess.check_call([sys.executable, "-m", "grpc_tools.protoc", f"-I{ROOT / 'proto'}",
                           f"--python_out={GEN}", f"--grpc_python_out={GEN}",
                           str(ROOT / "proto" / "campus.proto")])
