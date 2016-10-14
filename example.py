import rados, sys


def main():
    try:
        cluster = rados.Rados(conffile='/home/ubuntu/python-librados/ceph.conf')
    except TypeError as e :
        print 'Argument validation error: ', e
        raise e

    print "Created cluster handle."

    try:
        cluster.connect()
    except Exception as e:
        print "connectoin error: ", e
        raise e
    finally:
        print "\nConnected to the cluster.\n"
        return cluster


def cluster_shutdown(cluster):
    print "\n\nShutting down th handle."
    cluster.shutdown


def cluster_info(cluster):
    print("librados version: {version}\nwill attempt to connect to: {mon_init_member}\nCluster ID: {fsid}".format(
        version = str(cluster.version()),
        mon_init_member = str(cluster.conf_get('mon initial members')),
        fsid = cluster.get_fsid()
    ))


def cluster_stats(cluster):
  stats = cluster.get_cluster_stats()
  for key, value in stats.iteritems():
    print str(key) + " -> " + str(value)


def pool_list(cluster):
    print "Available Pools"
    print "---------------"
    pools = cluster.list_pools()

    for pool in pools:
        print pool


def pool_create(cluster, pool_name):
    if cluster.pool_exists(pool_name):
        print "Pool carete fail. Pool name is exist."
    else:
        print("Create {name} Pool".format(
            name = pool_name
        ))
        cluster.create_pool(pool_name)


def pool_delete(cluster, pool_name):
    if cluster.pool_exists(pool_name):
        print("Delete {name} Pool".format(
            name = pool_name
        ))
        cluster.delete_pool(pool_name)
    else:
        print "Pool delete fail. Pool name is not exist."


if __name__ == '__main__':
    # connect cluster
    cluster = main()

    # cluster_info(cluster)
    # cluster_stats(cluster)
    # pool_list(cluster)
    # pool_create(cluster, 'test')
    # pool_delete(cluster, 'test')
    # pool_list(cluster)

    # shutdown connect
    cluster_shutdown(cluster)

