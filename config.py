import os
import sys
import random
import logging


# Whether to included DEBUG level on the logs
DEBUG = bool(os.environ.get("DEBUG", False))
# Number of seconds for a connection to time out.
SOCKET_TIMEOUT_SECONDS = int(os.environ.get("SOCKET_TIMEOUT_SECONDS", 3))
# Max age of a peer in seconds to make it into the work list.
MAX_PEER_AGE_SECONDS = int(
    os.environ.get("MAX_PEER_AGE_SECONDS", 60 * 60)
)  # 1 hour
# Number of search rounds. Depends on number of open sockets. Determined experimentally.
# TODO: we could do this smarter, ie "stop once we stop finding a lot of new nodes"
MAX_SEARCH_ROUNDS = int(os.environ.get("MAX_SEARCH_ROUNDS", 45))
# Max number of open sockets. Put this too high and you'll see a lot of timeouts and errors.
MAX_OPEN_SOCKETS = int(os.environ.get("MAX_OPEN_SOCKETS", 128))
# timeout = factor * socket_timeout. Timeout defines how long we wait for all
# currently running greenlets. This needs to be relative to MAX_HF_VERSION - MIN_HF_VERSION.
GREENLET_TIMEOUT_FACTOR = int(os.environ.get("GREENLET_TIMEOUT_FACTOR", 20))
# MAX and MIN HF_VERSION define the boundaries of the hardfork versions
# we pretend to be at
MAX_HF_VERSION = int(os.environ.get("MAX_HF_VERSION", 17))
MIN_HF_VERSION = int(os.environ.get("MIN_HF_VERSION", 14))
# We need to pretend to be at block 2e6, so the peers disconnect when our
# hardfork network doesn't match theirs
FAKE_BLOCK_HEIGHT = int(10e6)


# We start from the official seed nodes.
# https://github.com/monero-project/monero/blob/release-v0.18/src/p2p/net_node.inl
SEED_NODES = {
    "176.9.0.187:18080",
    "88.198.163.90:18080",
    "66.85.74.134:18080",
    "88.99.173.38:18080",
    "51.79.173.165:18080",
    "192.99.8.110:18080",
    "37.187.74.171:18080",
    "18.169.212.248:18080",  # Rino's community public node (London).
    "95.217.25.101:18080",  # Guy's own node in the cloud.
}


# We chose a random peer id that stays constant over the course of the scan to avoid confusing the network.
PEER_ID = random.getrandbits(64)


# AWS Lambda sets some default log handlers that prevent our logging
# from working. See this issue for context:
# https://stackoverflow.com/questions/37703609/using-python-logging-with-aws-lambda
root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)
logging.basicConfig(
    level=logging.INFO if not DEBUG else logging.DEBUG,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
LOGGER = logging.getLogger()
