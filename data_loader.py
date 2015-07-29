__author__ = 'marcselwan'
from multiprocessing import Process, Queue
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.concurrent import execute_concurrent
import cassandra
import sys
import random
import string
import getopt
import time
import datetime
import collections

KEYSPACE = 'test'
CONSISTENCY=ConsistencyLevel.LOCAL_ONE
SEED_NODES = ['localhost']
DATACENTER = None


def connect(seeds, keyspace, datacenter=None, port=9042):
    from cassandra.io.libevreactor import LibevConnection
    from cassandra.cluster import Cluster
    from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy, ExponentialReconnectionPolicy

    class CustomRetryPolicy(RetryPolicy):

        def on_write_timeout(self, query, consistency, write_type,
                             required_responses, received_responses, retry_num):

            # retry at most 5 times regardless of query type
            if retry_num >= 5:
                return (self.RETHROW, None)

            return (self.RETRY, consistency)


    load_balancing_policy = None
    if datacenter:
        # If you are using multiple datacenters it's important to use
        # the DCAwareRoundRobinPolicy. If not then the client will
        # make cross DC connections. This defaults to round robin
        # which means round robin across all nodes irrespective of
        # data center.
        load_balancing_policy = DCAwareRoundRobinPolicy(local_dc=datacenter)

    cluster = Cluster(contact_points=seeds,
                      port=port,
                      default_retry_policy=CustomRetryPolicy(),
                      reconnection_policy=ExponentialReconnectionPolicy(1, 60),
                      load_balancing_policy=load_balancing_policy)

    cluster.connection_class = LibevConnection
    cluster.connection_class = LibevConnection
    cluster.control_connection_timeout = 10.0
    cluster.compression = False
    session = cluster.connect(keyspace)
    return session

def worker(ninserts, threadnum, queue):
    # get connection
    connection = connect(SEED_NODES, keyspace=KEYSPACE, datacenter=DATACENTER)
    preparedstmt_albertsons = connection.prepare("""INSERT INTO tx_details_raw (super_chain, chain, merchant, tx_time, settle_time,"
                                      amount, terminal_id) VALUES (?, ?, ?, ?, ?, ?, ?)""")
    preparedstmt_aldi = connection.prepare("""INSERT INTO tx_details_raw (super_chain, chain, merchant, tx_time, settle_time,"
                                      amount, terminal_id) VALUES (?, ?, ?, ?, ?, ?, ?)""")
    preparedstmt_kroger = connection.prepare("""INSERT INTO tx_details_raw (super_chain, chain, merchant, tx_time, settle_time,"
                                      amount, terminal_id) VALUES (?, ?, ?, ?, ?, ?, ?)""")
    preparedstmt_albertsons.consistency_level=CONSISTENCY
    preparedstmt_aldi.consistency_level=CONSISTENCY
    preparedstmt_kroger.consistency_level=CONSISTENCY

    inserts = 0
    total_insert_time = 0.0
    chain_albertsons= ['Albertsons', 'Safeway', 'Lucky', 'United']
    chain_aldi= ['Aldi', 'Traders Joes']
    chain_kroger= ['Bakers', 'Food4Less', 'Dhillon']
    merchant=['Marc', 'Ankur', 'Guy', 'Phil', 'The Pizza Guy']
    while (inserts < ninserts):
        # make a unique and incremental serial number across threads
        sernum = 'SN%05X%07X' %(threadnum, inserts)
        # make 2 to 9 inserts for this sernum
        ni = random.randint(2,9)
        for i in xrange(ni):
            start_ins_time = datetime.datetime.now()
            connection.execute(preparedstmt_albertsons, ('Albertsons', random.choice(chain_albertsons), random.choice(merchant), datetime.datetime.utcnow(),
                                                         datetime.datetime.utcnow(), random.random((.1, 10000), 2), random.randint(1,100000000)))
            connection.execute(preparedstmt_aldi, ('Aldi', random.choice(chain_albertsons), random.choice(merchant), datetime.datetime.utcnow(),
                                                         datetime.datetime.utcnow(), random.random((.1, 10000), 2), random.randint(1,100000000)))
            connection.execute(preparedstmt_kroger, ('Kroger', random.choice(chain_albertsons), random.choice(merchant), datetime.datetime.utcnow(),
                                                         datetime.datetime.utcnow(), random.random((.1, 10000), 2), random.randint(1,100000000)))
            stop_ins_time = datetime.datetime.now()
            insert_time = (stop_ins_time - start_ins_time).total_seconds()
            total_insert_time += insert_time
            inserts += 1
            if (inserts >= ninserts): break

    print 'Thread %d, performed %d inserts in %f secs (%f inserts/sec)' %(threadnum, ninserts, total_insert_time, inserts / total_insert_time)
    connection.shutdown()
    # save all the thread specific data
    queue.put([total_insert_time, inserts, inserts / total_insert_time])


def concurrent_worker(ninserts, threadnum, queue):
    # get connection
    connection = connect(SEED_NODES, keyspace=KEYSPACE, datacenter=DATACENTER)
    preparedstmt_albertsons = connection.prepare("""INSERT INTO tx_details_raw (super_chain, chain, merchant, tx_time, settle_time,"
                                      amount, terminal_id) VALUES (?, ?, ?, ?, ?, ?, ?)""")
    preparedstmt_aldi = connection.prepare("""INSERT INTO tx_details_raw (super_chain, chain, merchant, tx_time, settle_time,"
                                      amount, terminal_id) VALUES (?, ?, ?, ?, ?, ?, ?)""")
    preparedstmt_kroger = connection.prepare("""INSERT INTO tx_details_raw (super_chain, chain, merchant, tx_time, settle_time,"
                                      amount, terminal_id) VALUES (?, ?, ?, ?, ?, ?, ?)""")

    preparedstmt_albertsons.consistency_level=CONSISTENCY
    preparedstmt_aldi.consistency_level=CONSISTENCY
    preparedstmt_kroger.consistency_level=CONSISTENCY



    inserts = 0
    total_insert_time = 0.0
    while (inserts < ninserts):
        # make a unique and incremental serial number across threads
        chain_albertsons= ['Albertsons', 'Safeway', 'Lucky', 'United']
        chain_aldi= ['Aldi', 'Traders Joes']
        chain_kroger= ['Bakers', 'Food4Less', 'Dhillon']
        merchant=['Marc', 'Ankur', 'Guy', 'Phil', 'The Pizza Guy']
        # make 2 to 9 inserts for this sernum
        statements_and_params = []
        for i in xrange(random.randint(2,9)):
            statements_and_params.append([preparedstmt_albertsons, ('Albertsons', random.choice(chain_albertsons), random.choice(merchant), datetime.datetime.utcnow(),
                                                         datetime.datetime.utcnow(), random.random((.1, 10000), 2), random.randint(1,100000000))])
            statements_and_params.append([preparedstmt_aldi, ('Aldi', random.choice(chain_aldi), random.choice(merchant), datetime.datetime.utcnow(),
                                                         datetime.datetime.utcnow(), random.random((.1, 10000), 2), random.randint(1,100000000))])
            statements_and_params.append([preparedstmt_kroger, ('Kroger', random.choice(chain_kroger), random.choice(merchant), datetime.datetime.utcnow(),
                                                         datetime.datetime.utcnow(), random.random((.1, 10000), 2), random.randint(1,100000000))])

        start_ins_time = datetime.datetime.now()
        execute_concurrent(connection, statements_and_params)
        stop_ins_time = datetime.datetime.now()
        insert_time = (stop_ins_time - start_ins_time).total_seconds()
        total_insert_time += insert_time
        inserts += len(statements_and_params)

    print 'Thread %d, performed %d inserts in %f secs (%f inserts/sec)' %(threadnum, ninserts, total_insert_time, inserts / total_insert_time)
    connection.shutdown()
    # save all the thread specific data
    queue.put([total_insert_time, inserts, inserts / total_insert_time])

def main(nclients, ninserts):
    print "Starting....\n"
    start = datetime.datetime.now()
    threads = []
    thdata = Queue()
    for x in xrange(nclients):
        t = Process(target=concurrent_worker, args=(ninserts, x, thdata))
        threads.append(t)
        t.start()

    for thread in threads:
        thread.join()

    end = datetime.datetime.now()
    # gather statistics
    total_insert_time = 0.0
    total_inserts = 0
    total_rate = 0
    while not thdata.empty():
        [insert_time, inserts, rate] = thdata.get()
        total_insert_time += insert_time
        total_inserts += inserts
        total_rate += rate

    # because all threads running concurrently
    average_run_time = total_insert_time / nclients

    print
    print 'Average insert time per record (using %d threads %d inserts) is %f millisecs' % (nclients, ninserts, (total_insert_time * 1000) / total_inserts)
    print
    print 'Combined insert rate: %d per second' % (total_rate)
    print
    print 'Whole test ran for %f seconds' % ((end - start).total_seconds())
    print
    print "Finished!!\n"

def print_help():
    print '''cassandra_test1.py clients inserts reads

    --clients=    no of clients
    --inserts=    no of inserts/client
    --help        print this help

    -c        same as --clients
    -i        same as --inserts
    -h        same as --help

    '''

if __name__ == "__main__":
    try:
        options, args = getopt.getopt(
            sys.argv[1:], 'hc:i:', ['clients=',
                                    'inserts=',
                                    'help'])
    except getopt.GetoptError, err:
        print str(err)
        print_help()
        sys.exit(2)

    for opt, arg in options:
        if opt in ('--clients'):
            nclients = int(arg)
        elif opt in ('--inserts'):
            ninserts = int(arg)
        elif opt in ('--help'):
            print_help()
            sys.exit(2)

    # default values in case of missing command line arguments
    try:
        nclients
    except NameError:
        nclients = 1
        #print "No of clients need to be specified."
        #print_help()
        #sys.exit(2)

    try:
        ninserts
    except NameError:
        ninserts = 1000000

    main(nclients, ninserts)
