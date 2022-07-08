from contextlib import closing, contextmanager
import psycopg2.extras
import pytest
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder
from fixtures.utils import lsn_from_hex, lsn_to_hex
from fixtures.log_helper import log
import time
from fixtures.neon_fixtures import Postgres
import threading
import timeit

pytest_plugins = ("fixtures.neon_fixtures")


@contextmanager
def pg_cur(pg):
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            yield cur


# Periodically check that all backpressure lags are below the configured threshold,
# assert if they are not.
# If the check query fails, stop the thread. Main thread should notice that and stop the test.
def check_backpressure(pg: Postgres, stop_event: threading.Event, polling_interval=5):
    log.info("checks started")

    with pg_cur(pg) as cur:
        cur.execute("CREATE EXTENSION neon")  # TODO move it to neon_fixtures?

        cur.execute("select pg_size_bytes(current_setting('max_replication_write_lag'))")
        res = cur.fetchone()
        max_replication_write_lag_bytes = res[0]
        log.info(f"max_replication_write_lag: {max_replication_write_lag_bytes} bytes")

        cur.execute("select pg_size_bytes(current_setting('max_replication_flush_lag'))")
        res = cur.fetchone()
        max_replication_flush_lag_bytes = res[0]
        log.info(f"max_replication_flush_lag: {max_replication_flush_lag_bytes} bytes")

        cur.execute("select pg_size_bytes(current_setting('max_replication_apply_lag'))")
        res = cur.fetchone()
        max_replication_apply_lag_bytes = res[0]
        log.info(f"max_replication_apply_lag: {max_replication_apply_lag_bytes} bytes")

    with pg_cur(pg) as cur:
        while not stop_event.is_set():
            try:
                cur.execute('''
                select pg_wal_lsn_diff(pg_current_wal_flush_lsn(),received_lsn) as received_lsn_lag,
                pg_wal_lsn_diff(pg_current_wal_flush_lsn(),disk_consistent_lsn) as disk_consistent_lsn_lag,
                pg_wal_lsn_diff(pg_current_wal_flush_lsn(),remote_consistent_lsn) as remote_consistent_lsn_lag,
                pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_flush_lsn(),received_lsn)),
                pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_flush_lsn(),disk_consistent_lsn)),
                pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_flush_lsn(),remote_consistent_lsn))
                from backpressure_lsns();
                ''')

                res = cur.fetchone()
                received_lsn_lag = res[0]
                disk_consistent_lsn_lag = res[1]
                remote_consistent_lsn_lag = res[2]

                log.info(f"received_lsn_lag = {received_lsn_lag} ({res[3]}), "
                         f"disk_consistent_lsn_lag = {disk_consistent_lsn_lag} ({res[4]}), "
                         f"remote_consistent_lsn_lag = {remote_consistent_lsn_lag} ({res[5]})")

                # Since feedback from pageserver is not immediate, we should allow some lag overflow
                lag_overflow = 5 * 1024 * 1024  # 5MB

                if max_replication_write_lag_bytes > 0:
                    assert received_lsn_lag < max_replication_write_lag_bytes + lag_overflow
                if max_replication_flush_lag_bytes > 0:
                    assert disk_consistent_lsn_lag < max_replication_flush_lag_bytes + lag_overflow
                if max_replication_apply_lag_bytes > 0:
                    assert remote_consistent_lsn_lag < max_replication_apply_lag_bytes + lag_overflow

                time.sleep(polling_interval)

            except Exception as e:
                log.info(f"backpressure check query failed: {e}")
                stop_event.set()

    log.info('check thread stopped')


# This test illustrates how to tune backpressure to control the lag
# between the WAL flushed on compute node and WAL digested by pageserver.
#
# To test it, throttle walreceiver ingest using failpoint and run heavy write load.
# If backpressure is disabled or not tuned properly, the query will timeout, because the walreceiver cannot keep up.
# If backpressure is enabled and tuned properly, insertion will be throttled, but the query will not timeout.


@pytest.mark.skip("See https://github.com/neondatabase/neon/issues/1587")
def test_backpressure_received_lsn_lag(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    # Create a branch for us
    env.neon_cli.create_branch('test_backpressure')

    pg = env.postgres.create_start('test_backpressure',
                                   config_lines=['max_replication_write_lag=30MB'])
    log.info("postgres is running on 'test_backpressure' branch")

    # setup check thread
    check_stop_event = threading.Event()
    check_thread = threading.Thread(target=check_backpressure, args=(pg, check_stop_event))
    check_thread.start()

    # Configure failpoint to slow down walreceiver ingest
    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor(cursor_factory=psycopg2.extras.DictCursor) as pscur:
            pscur.execute("failpoints walreceiver-after-ingest=sleep(20)")

    # FIXME
    # Wait for the check thread to start
    #
    # Now if load starts too soon,
    # check thread cannot auth, because it is not able to connect to the database
    # because of the lag and waiting for lsn to replay to arrive.
    time.sleep(2)

    with pg_cur(pg) as cur:
        # Create and initialize test table
        cur.execute("CREATE TABLE foo(x bigint)")

        inserts_to_do = 2000000
        rows_inserted = 0

        while check_thread.is_alive() and rows_inserted < inserts_to_do:
            try:
                cur.execute("INSERT INTO foo select from generate_series(1, 100000)")
                rows_inserted += 100000
            except Exception as e:
                if check_thread.is_alive():
                    log.info('stopping check thread')
                    check_stop_event.set()
                    check_thread.join()
                    assert False, f"Exception {e} while inserting rows, but WAL lag is within configured threshold. That means backpressure is not tuned properly"
                else:
                    assert False, f"Exception {e} while inserting rows and WAL lag overflowed configured threshold. That means backpressure doesn't work."

        log.info(f"inserted {rows_inserted} rows")

    if check_thread.is_alive():
        log.info('stopping check thread')
        check_stop_event.set()
        check_thread.join()
        log.info('check thread stopped')
    else:
        assert False, "WAL lag overflowed configured threshold. That means backpressure doesn't work."


def test_restart_delay(neon_simple_env: NeonEnv):
    # See discussion at https://github.com/neondatabase/neon/issues/2023
    env = neon_simple_env

    tenant_id = env.initial_tenant
    timeline_id = env.neon_cli.create_branch('test_restart_delay')
    pg = env.postgres.create('test_restart_delay')
    pg.start()

    # Generate enough WALs to exceed the maximum write lag
    pg.safe_psql_many(queries=[
        'CREATE TABLE foo(key int primary key)',
        'INSERT INTO foo SELECT generate_series(1, 100000)',
    ])

    def wait_pageserver_sync():
        log.info("Waiting for the pageserver to catch up with the compute")
        for _ in range(20):  # At most 20s
            # Query the pageserver first, so we don't care how stale its response is:
            pageserver_last_record_lsn = lsn_from_hex(env.pageserver.http_client().timeline_detail(
                tenant_id, timeline_id)['local']['last_record_lsn'])
            last_wal = lsn_from_hex(pg.safe_psql('SELECT pg_current_wal_insert_lsn()')[0][0])
            log.info(
                f"pageserver is at {lsn_to_hex(pageserver_last_record_lsn)}, last_wal is {lsn_to_hex(last_wal)}"
            )
            assert pageserver_last_record_lsn <= last_wal
            if pageserver_last_record_lsn >= last_wal:
                break
            time.sleep(1)
        else:
            assert False, "Timeout: the pageserver did not caught up with the compute"

    wait_pageserver_sync()
    log.info("Restarting compute node so it does not send LogStandbySnapshot right away")
    pg.stop()
    pg.start()

    # We hope that the following operations take significantly less
    # than LOG_SNAPSHOT_INTERVAL_MS (15s), so if the query is blocked by backpressure,
    # it's not unblocked by LogStandbySnapshot sending new WAL.
    wait_pageserver_sync()
    log.info("Restarting pageserver and safekeeper")
    env.pageserver.stop()
    env.safekeepers[0].stop()
    env.safekeepers[0].start()
    env.pageserver.start()

    timer = timeit.default_timer()
    pg.safe_psql('INSERT INTO foo SELECT generate_series(100001, 100100)')
    duration = timeit.default_timer() - timer
    # insert 100 rows should not take too much time
    log.info(f"The query took {duration}s")
    assert duration < 2.0


#TODO test_backpressure_disk_consistent_lsn_lag. Play with pageserver's checkpoint settings
#TODO test_backpressure_remote_consistent_lsn_lag
