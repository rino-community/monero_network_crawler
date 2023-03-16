# -*- coding: utf-8 -*-
import collections
import sys
import os
import socket
from base64 import b64decode
from datetime import datetime
from typing import Tuple, Optional
from multiprocessing.pool import ThreadPool

import boto3
import psycopg2


import geoip2.database

import config
from p2p import try_to_get_info


logger = config.LOGGER

ssm = boto3.client("ssm")


# Simple wrapper to retrieve a secret from SSM Parameter Store
def get_secret_from_ssm(key):
    resp = ssm.get_parameter(Name=key, WithDecryption=True)

    return resp["Parameter"]["Value"]


def is_connected():
    try:
        socket.create_connection(("www.google.com", 80))
        return True
    except OSError:
        pass

    return False


def init_env():
    db_url = get_secret_from_ssm(os.environ["DB_URL_SECRET"])
    db_host, db_port = db_url.split(":")

    db_user = get_secret_from_ssm(os.environ["DB_USER_SECRET"])

    db_psw = get_secret_from_ssm(os.environ["DB_PASSWORD_SECRET"])

    db_database = os.environ["DB_DATABASE"]

    return {
        "db_host": db_host,
        "db_port": db_port,
        "db_user": db_user,
        "db_psw": db_psw,
        "db_database": db_database,
    }


def lambda_handler(event, context):
    nodes_todo = {node for node in config.SEED_NODES}
    # ensure that we have all env needed for db connection
    env = init_env()

    nodes_ok = {}
    nodes_error = {}
    nodes_data = {}

    logger.info(
        f"Starting Monero P2P crawler with {len(nodes_todo)} nodes in seed list."
    )
    logger.debug(f"Using peer id {config.PEER_ID}")

    # prepare data to save to crawler run table
    crawler_run_data = {"timestamp": datetime.now()}

    # This loop does search rounds over the nodes left todo.
    search_round = 0
    while len(nodes_todo) and search_round < config.MAX_SEARCH_ROUNDS:
        search_round += 1
        logger.info(
            f"[Round {search_round}] Nodes unscanned {len(nodes_todo):4}  /  reachable {len(nodes_ok):4}  /  "
            f"unreachable {len(nodes_error):4}  |  Scanning ..."
        )

        # Spawn up to MAX_OPEN_SOCKETS threads that try to connect to nodes in parallel.
        round_args = []
        while len(nodes_todo) and len(round_args) < config.MAX_OPEN_SOCKETS:

            current_node = nodes_todo.pop()
            logger.debug(f"Queueing node {current_node}")
            ip, port_str = current_node.split(":")
            round_args.append((ip, int(port_str)))

        with ThreadPool(config.MAX_OPEN_SOCKETS) as pool:
            results = pool.map(try_to_get_info, round_args)

        # Process data from all finished threads.
        for success, info, peers in results:
            data_to_store = {}

            current_node = f"{info['ip']}:{info['port']}"

            # collect data to store them to database

            data_to_store.update(
                {
                    "port": info.get("port", 18080),
                    "height": info.get("height", None),
                    "hf_version": info.get("hf_version", None),
                    "tip_hash": info.get("tip_hash", None),
                    "timestamp": info.get("timestamp", None),
                    "success": success,
                }
            )
            nodes_data.update({info["ip"]: data_to_store})

            if success:
                nodes_ok[current_node] = info
                logger.debug(f"Got {len(peers)} peers from {current_node}")
                for peer in peers:
                    if (
                        peer not in nodes_ok
                        and peer not in nodes_error
                        and current_node not in nodes_error
                        and current_node not in nodes_todo
                    ):
                        nodes_todo.add(peer)

            else:
                nodes_error[current_node] = info

    print(
        f"Nodes unscanned {len(nodes_todo):4}  /  reachable {len(nodes_ok):4}  /  unreachable {len(nodes_error):4}"
    )
    print("Finished scanning.")
    print(f"{len(nodes_ok)} reachable nodes.")
    print(
        f"{len(nodes_error)} unreachable nodes. (These are up and connected, "
        f"but are not accessible through their p2p port.)"
    )

    # Very stupid statistics, just for testing
    hf_versions = []
    heights = []
    for peer in nodes_ok.values():
        hf_versions.append(peer["hf_version"])
        heights.append(peer["height"])

    # noinspection PyArgumentList
    hf_versions_counter = collections.Counter(hf_versions)
    # noinspection PyArgumentList
    heights_counter = collections.Counter(heights)

    print("Top 5 HF versions:")
    for k, v in hf_versions_counter.most_common(5):
        print(f"  {k:2}: {v:4}")
    print("Top 5 heights:")
    for k, v in heights_counter.most_common(5):
        print(f"  {k:8}: {v:4}")

    # save data to database

    db_save = save_data(env, nodes_data, crawler_run_data)

    if db_save is True:
        print("Data has been saved successfully.")
        print("Bye")
    else:
        print("!!!!Data has not been saved!!!!")
        sys.exit(1)


Coordinates = Tuple[Optional[float], Optional[float]]


class IPGeolocator:
    """
    Contains logic to geolocate IP addresses
    """

    def __init__(self, db_path: str):
        self.db = geoip2.database.Reader(db_path)

    def get_coordinates(self, ip_addr: str) -> Coordinates:
        """
        Tries to obtain latitude and longitude of an IP address
        """
        try:
            city = self.db.city(ip_addr)
        except:
            logger.warning(
                f"Unable to geolocalize {ip_addr}. Placing it in the North Pole"
            )
            return 90.0, 135.0

        return city.location.latitude, city.location.longitude


def save_data(env, data, crawler_run_data):

    conn = psycopg2.connect(
        host=env.get("db_host"),
        port=env.get("db_port"),
        user=env.get("db_user"),
        password=env.get("db_psw"),
        database=env.get("db_database"),
    )

    success = True

    geoloc = IPGeolocator("./GeoLite2-City.mmdb")

    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO crawler_run (timestamp) VALUES(%s) RETURNING id""",
            [crawler_run_data["timestamp"]],
        )
        id = cur.fetchone()[0]
        cur.close()
        for k, v in data.items():
            lat, lon = geoloc.get_coordinates(k)
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO crawler_data (ip, port, height, hf_version, tip_hash, lat, long, success, crawler_run_id, last_access) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    k,
                    v["port"],
                    v["height"],
                    v["hf_version"],
                    v["tip_hash"],
                    lat,
                    lon,
                    v["success"],
                    id,
                    v["timestamp"],
                ),
            )
            cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        print(
            "Error in transaction, reverting all operation in transaction ... %s"
            % e
        )
        conn.rollback()
        success = False
    finally:
        if conn:
            cur.close()
            conn.close()

    return success


if __name__ == "__main__":
    ### STANDALONE TEST ###
    event = None
    context = None
    t = "Error"
    try:
        lambda_handler(event, context)
    except Exception as e:
        print(str(e))

    print(t)
