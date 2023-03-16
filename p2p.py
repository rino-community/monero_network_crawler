# -*- coding: utf-8 -*-
import binascii
from datetime import datetime
import socket

from levin.bucket import Bucket
from levin.section import Section
from levin.constants import (
    c_int64,
    c_uint32,
    P2P_COMMAND_HANDSHAKE,
    P2P_COMMANDS,
    LEVIN_SIGNATURE,
)
from levin.utils import int2ip

import config

logger = config.LOGGER


class EchoNodeException(Exception):
    """
    To be raised and handled when an "echo node" is encountered
    """


class ExtendedBucket(Bucket):
    @staticmethod
    def create_handshake_request(
        my_port: int = 0,
        network_id: bytes = None,
        peer_id: bytes = b"\x41\x41\x41\x41\x41\x41\x41\x41",
        hf_version: int = 1,
        block_height: int = 1,
    ):
        """
        Creates a handshake request whose Hardfork Version and Block Height can be
        forged.
        """
        _hf_version = int(hf_version)
        _block_height = int(block_height)
        handshake_section = Section.handshake_request(
            peer_id=peer_id, network_id=network_id, my_port=my_port
        )
        handshake_section.entries["payload_data"].entries[
            "top_version"
        ] = c_int64(_hf_version)
        handshake_section.entries["payload_data"].entries[
            "current_height"
        ] = c_int64(_block_height)
        bucket = Bucket.create_request(
            P2P_COMMAND_HANDSHAKE.value, section=handshake_section
        )

        logger.debug(">> created packet '%s'" % P2P_COMMANDS[bucket.command])
        return bucket


def try_to_get_info(ip_and_port):
    """
    Tries to get the info of a node, by pretending to be nodes with different
    hardfork versions.

    When one of the hardfork versions matches the one of the peer, it should
    return its information.
    """
    node_ip, node_port = ip_and_port
    key = f"{node_ip}:{node_port}"
    for hf_version in reversed(
        range(config.MIN_HF_VERSION, config.MAX_HF_VERSION + 1)
    ):
        logger.debug(
            f"{key} Pretending to be at hardfork version {hf_version}"
        )
        got_info, node_info, node_peers = get_info_from_node(
            node_ip, node_port, hf_version
        )
        if got_info:
            if hf_version == config.MAX_HF_VERSION:
                # Ignore "echo nodes". Check this issue for more info:
                # https://gitlab.cryptosphere-systems.com/side-projects/network_health_infra/issues/6
                logger.debug(f'{key} is an "echo node"')
                got_info = False
                break

            logger.debug(
                f"{key} Got node info when pretending to be at hardfork version {hf_version}"
            )
            break
        if got_info is None:  # network problem (timeout most likely)
            break

    return got_info, node_info, node_peers


def get_info_from_node(node_ip, node_port, hf_version):
    """
    Tries to connect to the indicated node.
    If successful, retrieves node info (height, hardfork version and tip hash) as well as the list of peers.
    :param node_ip: IP of node.
    :type node_ip: str
    :param node_port: Port of node.
    :type node_port: int
    :return: True if successful, dict of node info, dict of node's peers
    :rtype: (boolean, dict, dict)
    """
    got_info = False
    node_info = {"ip": node_ip, "port": node_port, "timestamp": datetime.now()}
    node_peers = []
    key = f"{node_ip}:{node_port}"

    logger.debug(f"{key} Trying connection")

    sock = None
    try:
        logger.debug(f"{key} Opening socket")
        sock = socket.socket()
        sock.settimeout(config.SOCKET_TIMEOUT_SECONDS)
        sock.connect((node_ip, node_port))

        logger.debug(f"{key} Initiating handshake")
        bucket = ExtendedBucket.create_handshake_request(
            peer_id=config.PEER_ID,
            hf_version=hf_version,
            block_height=config.FAKE_BLOCK_HEIGHT,
        )
        sock.send(bucket.header())
        sock.send(bucket.payload())

        while True:
            logger.debug(
                f"{key} >> sent packet '{P2P_COMMANDS[bucket.command]}'"
            )

            buffer = sock.recv(8)

            if len(buffer) == 0:
                logger.warning(f"{key} Node closed connection")
                break

            if not buffer.startswith(bytes(LEVIN_SIGNATURE)):
                logger.warning(f"{key} Received invalid data: {buffer}")
                break

            bucket = ExtendedBucket.from_buffer(signature=buffer, sock=sock)

            if bucket.command != P2P_COMMAND_HANDSHAKE:
                continue

            logger.debug(
                f"{key} Payload entries: {bucket.payload_section.entries.keys()}"
            )
            logger.debug(
                f"{key} Node data entries: {bucket.payload_section.entries['node_data'].entries.keys()}"
            )
            logger.debug(
                f"{key} Payload data entries: "
                f"{bucket.payload_section.entries['payload_data'].entries.keys()}"
            )

            payload_data = bucket.payload_section.entries["payload_data"]

            # Not all nodes expose these entries.
            if (
                "current_height" in payload_data.entries
                and "top_id" in payload_data.entries
            ):
                node_info["height"] = payload_data.entries[
                    "current_height"
                ].value
                node_info["hf_version"] = hf_version
                node_info["tip_hash"] = binascii.b2a_hex(
                    payload_data.entries["top_id"]
                ).decode("utf-8")
                got_info = True

            # Some nodes don't announce their peerlist.
            if "local_peerlist_new" not in bucket.payload_section.entries:
                logger.warning(f"{key} Node doesn't announce peerlist.")
                break

            # Parse peerlist and add it to result if peer was seen recently enough.
            for peer_entry in [
                e.entries
                for e in bucket.payload_section.entries["local_peerlist_new"]
            ]:
                # Some versions don't seem to have this.
                if "addr" not in peer_entry["adr"].entries:
                    continue

                addr = peer_entry["adr"].entries["addr"].entries
                last_seen, m_ip, m_port = (
                    peer_entry.get("last_seen"),
                    addr["m_ip"],
                    addr["m_port"],
                )
                ip_str = str(
                    int2ip(
                        c_uint32.from_buffer(
                            buffer=m_ip.__bytes__(), endian="big"
                        ).value
                    )
                )
                #
                # It looks like last_seen is not reported for most (any?) nodes.
                # We'll start logging when the last_seen of a node is reported,
                # to check whether any last_seen is reported at all.
                if last_seen:
                    logger.info(f"last_seen reported for {ip_str}")
                    seen_delta = datetime.now() - datetime.fromtimestamp(
                        last_seen.value
                    )
                    if seen_delta.seconds > config.MAX_PEER_AGE_SECONDS:
                        logger.debug(
                            f"{key}: reported inactive peer {ip_str}:{m_port.value}"
                        )
                        continue
                logger.debug(
                    f"{key} Found potentially active peer {ip_str}:{m_port.value}"
                )
                node_peers.append(f"{ip_str}:{m_port.value}")
            break
    except (socket.error, socket.timeout) as e:
        logger.warning(f"{key} Unable to connect: {e}")
        return None, node_info, node_peers
    except Exception as e:
        logger.error(f"Failed to process {key}: {e}")

    if sock:
        sock.close()

    return got_info, node_info, node_peers
