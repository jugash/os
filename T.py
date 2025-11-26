#!/usr/bin/env python3

import os
import hvac
from hvac import exceptions
import argparse
import logging
import sys
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def verify_cluster_state(client, expected_state, timeout=60):
    """
    Verifies the state of a cluster.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        status = get_replication_status(client)
        if status and status.get('data', {}).get('mode') == expected_state:
            logging.info(f"Cluster {client.url} is in the expected state: {expected_state}")
            return True
        time.sleep(5)
    logging.error(f"Cluster {client.url} did not reach the expected state of {expected_state} within {timeout} seconds.")
    return False

def get_vault_client(vault_addr, vault_token):
    """
    Creates and returns a Vault client.
    """
    return hvac.Client(url=vault_addr, token=vault_token)

def get_replication_status(client):
    """
    Gets the replication status of a Vault cluster.
    """
    try:
        response = client.sys.read_replication_status()
        return response
    except exceptions.VaultError as e:
        logging.error(f"Error getting replication status from {client.url}: {e}")
        return None

def promote_dr_primary(client):
    """
    Promotes a DR primary to primary.
    """
    try:
        client.sys.promote()
        logging.info("DR primary promoted to primary.")
        return True
    except exceptions.VaultError as e:
        logging.error(f"Error promoting DR primary on {client.url}: {e}")
        return False

def demote_primary(client):
    """
    Demotes a primary to a DR secondary.
    """
    try:
        client.sys.demote()
        logging.info("Primary demoted to DR secondary.")
        return True
    except exceptions.VaultError as e:
        logging.error(f"Error demoting primary on {client.url}: {e}")
        return False

def disable_performance_replication(client):
    """
    Disables performance replication on a cluster.
    """
    try:
        client.sys.disable_performance_replication()
        logging.info(f"Performance replication disabled on {client.url}")
        return True
    except exceptions.VaultError as e:
        logging.error(f"Error disabling performance replication on {client.url}: {e}")
        return False

def generate_performance_secondary_token(client):
    """
    Generates a secondary activation token for a performance replica.
    """
    try:
        response = client.sys.generate_secondary_activation_token(secondary_type='performance')
        return response['data']['token']
    except exceptions.VaultError as e:
        logging.error(f"Error generating secondary activation token on {client.url}: {e}")
        return None

def enable_performance_replication(client, token):
    """
    Enables performance replication on a cluster.
    """
    try:
        client.sys.enable_performance_replication(token=token)
        logging.info(f"Performance replication enabled on {client.url}")
        return True
    except exceptions.VaultError as e:
        logging.error(f"Error enabling performance replication on {client.url}: {e}")
        return False

def generate_dr_secondary_token(client):
    """
    Generates a secondary activation token for a DR replica.
    """
    try:
        response = client.sys.generate_secondary_activation_token(secondary_type='dr')
        return response['data']['token']
    except exceptions.VaultError as e:
        logging.error(f"Error generating secondary activation token on {client.url}: {e}")
        return None

def enable_dr_replication(client, token):
    """
    Enables DR replication on a cluster.
    """
    try:
        client.sys.enable_dr_replication(token=token)
        logging.info(f"DR replication enabled on {client.url}")
        return True
    except exceptions.VaultError as e:
        logging.error(f"Error enabling DR replication on {client.url}: {e}")
        return False

def preflight_checks(clients):
    """
    Verifies connectivity and authentication for all clusters.
    """
    for client in clients:
        try:
            client.sys.read_health_status()
            logging.info(f"Successfully connected to {client.url}")
        except exceptions.VaultError as e:
            logging.error(f"Failed to connect to {client.url}: {e}")
            return False
    return True

def switch_primary(new_primary_client, old_primary_client, perf_replica_clients, dry_run=False, resilient=False):
    """
    Switches the primary cluster.
    """
    if dry_run:
        logging.info(f"DRY-RUN: Promoting {new_primary_client.url} to primary")
        logging.info(f"DRY-RUN: Reconfiguring performance replicas to replicate from {new_primary_client.url}")
        logging.info(f"DRY-RUN: Demoting {old_primary_client.url} and re-establishing DR replication from {new_primary_client.url}")
        return True

    logging.info(f"Promoting {new_primary_client.url} to primary...")
    if not promote_dr_primary(new_primary_client):
        return False
    if not verify_cluster_state(new_primary_client, "primary"):
        return False

    logging.info("Reconfiguring performance replicas...")
    token = generate_performance_secondary_token(new_primary_client)
    if not token:
        return False
    for client in perf_replica_clients:
        if not disable_performance_replication(client):
            if resilient:
                logging.warning(f"Could not disable performance replication on {client.url}. This may be because the cluster is unreachable.")
            else:
                return False
        
        if not enable_performance_replication(client, token):
            if resilient:
                logging.warning(f"Could not enable performance replication on {client.url}. This may be because the cluster is unreachable.")
            else:
                return False

    logging.info("Re-establishing DR replication on old primary...")
    if not demote_primary(old_primary_client):
        if resilient:
            logging.warning(f"Could not demote old primary at {old_primary_client.url}. This may be because the cluster is unreachable.")
        else:
            return False
    
    dr_token = generate_dr_secondary_token(new_primary_client)
    if not dr_token or not enable_dr_replication(old_primary_client, dr_token):
        if resilient:
            logging.warning(f"Could not re-establish DR replication on old primary at {old_primary_client.url}. This may be because the cluster is unreachable.")
        else:
            return False
    elif not verify_cluster_state(old_primary_client, "secondary"):
        if resilient:
            logging.warning(f"Old primary at {old_primary_client.url} did not reach the expected state of 'secondary'.")
        else:
            return False
    
    return True

def main():
    """
    Main function to handle failover and failback.
    """
    parser = argparse.ArgumentParser(description="Vault Enterprise Failover and Failback Script")
    parser.add_argument("command", choices=["status", "failover", "failback"], help="The command to execute")
    parser.add_argument("--dry-run", action="store_true", help="Print the sequence of actions without executing them")
    args = parser.parse_args()

    dc1_cluster_addr = os.environ.get("DC1_CLUSTER_ADDR")
    dc1_cluster_token = os.environ.get("DC1_CLUSTER_TOKEN")
    dc1_perf_replica_addr = os.environ.get("DC1_PERF_REPLICA_ADDR")
    dc1_perf_replica_token = os.environ.get("DC1_PERF_REPLICA_TOKEN")
    dc2_cluster_addr = os.environ.get("DC2_CLUSTER_ADDR")
    dc2_cluster_token = os.environ.get("DC2_CLUSTER_TOKEN")
    dc2_perf_replica_addr = os.environ.get("DC2_PERF_REPLICA_ADDR")
    dc2_perf_replica_token = os.environ.get("DC2_PERF_REPLICA_TOKEN")

    if not all([dc1_cluster_addr, dc1_cluster_token, dc1_perf_replica_addr, dc1_perf_replica_token, dc2_cluster_addr, dc2_cluster_token, dc2_perf_replica_addr, dc2_perf_replica_token]):
        logging.error("Please set all the required environment variables for the four clusters.")
        return

    dc1_cluster_client = get_vault_client(dc1_cluster_addr, dc1_cluster_token)
    dc1_perf_replica_client = get_vault_client(dc1_perf_replica_addr, dc1_perf_replica_token)
    dc2_cluster_client = get_vault_client(dc2_cluster_addr, dc2_cluster_token)
    dc2_perf_replica_client = get_vault_client(dc2_perf_replica_addr, dc2_perf_replica_token)

    all_clients = [dc1_cluster_client, dc1_perf_replica_client, dc2_cluster_client, dc2_perf_replica_client]
    if not preflight_checks(all_clients):
        sys.exit(1)

    try:
        if args.command == "status":
            logging.info("Getting cluster statuses...")
            
            for name, client in [("DC1 Cluster", dc1_cluster_client), 
                                 ("DC1 Perf Replica", dc1_perf_replica_client), 
                                 ("DC2 Cluster", dc2_cluster_client), 
                                 ("DC2 Perf Replica", dc2_perf_replica_client)]:
                status = get_replication_status(client)
                if status:
                    role = status.get('data', {}).get('mode')
                    logging.info(f"{name} Role: {role}")

        elif args.command == "failover":
            logging.info("Performing failover...")
            dc2_cluster_status = get_replication_status(dc2_cluster_client)

            if dc2_cluster_status:
                dc2_role = dc2_cluster_status.get('data', {}).get('mode')

                if dc2_role == 'secondary':
                    if not switch_primary(dc2_cluster_client, dc1_cluster_client, [dc1_perf_replica_client, dc2_perf_replica_client], args.dry_run, resilient=True):
                        sys.exit(1)
                else:
                    logging.warning("DC2 cluster is not in a secondary state.")
        elif args.command == "failback":
            logging.info("Performing failback...")
            dc1_cluster_status = get_replication_status(dc1_cluster_client)
            dc2_cluster_status = get_replication_status(dc2_cluster_client)

            if dc1_cluster_status and dc2_cluster_status:
                dc1_role = dc1_cluster_status.get('data', {}).get('mode')
                dc2_role = dc2_cluster_status.get('data', {}).get('mode')

                if dc2_role == 'primary' and dc1_role == 'secondary':
                    if not switch_primary(dc1_cluster_client, dc2_cluster_client, [dc1_perf_replica_client, dc2_perf_replica_client], args.dry_run):
                        sys.exit(1)
                else:
                    logging.warning("Clusters are not in a failback-ready state.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
