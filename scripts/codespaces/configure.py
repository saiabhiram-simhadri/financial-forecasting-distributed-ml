"""Generate a resource-limited single-user Hadoop/Hive configuration."""
import argparse
import os
from pathlib import Path
import shlex
import shutil
import xml.etree.ElementTree as ET


def write_xml(path, properties):
    root = ET.Element('configuration')
    for name, value in properties.items():
        prop = ET.SubElement(root, 'property')
        ET.SubElement(prop, 'name').text = name
        ET.SubElement(prop, 'value').text = str(value)
    ET.indent(root)
    ET.ElementTree(root).write(path, encoding='utf-8', xml_declaration=True)


def configure(state):
    state = Path(state).resolve()
    tools = state / 'tools'
    hadoop = tools / 'hadoop'
    hive = tools / 'hive'
    conf = state / 'conf' / 'hadoop'
    hive_conf = state / 'conf' / 'hive'
    for folder in (conf, hive_conf, state / 'logs' / 'hadoop', state / 'logs' / 'hive',
                   state / 'pids', state / 'hdfs' / 'namenode', state / 'hdfs' / 'datanode',
                   state / 'metastore', state / 'events', state / 'tmp' / 'hive',
                   state / 'yarn-local', state / 'yarn-logs'):
        folder.mkdir(parents=True, exist_ok=True)
    # Retain distribution logging defaults when using a separate persistent config.
    for name in ('log4j.properties', 'hadoop-metrics2.properties', 'hadoop-policy.xml'):
        source = hadoop / 'etc' / 'hadoop' / name
        if source.is_file():
            shutil.copy2(source, conf / name)
    for name in ('hive-log4j2.properties', 'beeline-log4j2.properties'):
        source = hive / 'conf' / (name + '.template')
        if source.is_file():
            shutil.copy2(source, hive_conf / name)
    common = {'fs.defaultFS': 'hdfs://127.0.0.1:9000',
              'hadoop.tmp.dir': state / 'tmp' / 'hadoop',
              'hadoop.http.staticuser.user': os.environ.get('USER', 'vscode')}
    hdfs = {
        'dfs.replication': 1,
        'dfs.namenode.name.dir': (state / 'hdfs' / 'namenode').as_uri(),
        'dfs.datanode.data.dir': (state / 'hdfs' / 'datanode').as_uri(),
        'dfs.namenode.rpc-address': '127.0.0.1:9000',
        'dfs.namenode.http-address': '127.0.0.1:9870',
        'dfs.datanode.address': '127.0.0.1:9866',
        'dfs.datanode.http.address': '127.0.0.1:9864',
        'dfs.datanode.ipc.address': '127.0.0.1:9867',
        'dfs.datanode.hostname': 'localhost',
        'dfs.client.use.datanode.hostname': 'true',
        'dfs.permissions.enabled': 'false',
    }
    classpath = ','.join(str(hadoop / 'share' / 'hadoop' / component / glob)
                         for component in ('common', 'hdfs', 'mapreduce', 'yarn')
                         for glob in ('*', 'lib/*'))
    mapred = {
        'mapreduce.framework.name': 'yarn',
        'mapreduce.application.classpath': classpath,
        'mapreduce.map.memory.mb': 512, 'mapreduce.reduce.memory.mb': 512,
        'mapreduce.map.java.opts': '-Xmx384m',
        'mapreduce.reduce.java.opts': '-Xmx384m',
        'yarn.app.mapreduce.am.resource.mb': 768,
        'yarn.app.mapreduce.am.command-opts': '-Xmx512m',
        'mapreduce.job.running.map.limit': 1,
        'mapreduce.job.running.reduce.limit': 1,
        'mapreduce.job.reduces': 1,
        'mapreduce.map.maxattempts': 1,
        'mapreduce.reduce.maxattempts': 1,
        'mapreduce.task.io.sort.mb': 64,
    }
    yarn = {
        'yarn.resourcemanager.hostname': 'localhost',
        'yarn.resourcemanager.webapp.address': '127.0.0.1:8088',
        'yarn.nodemanager.hostname': 'localhost',
        'yarn.nodemanager.local-dirs': state / 'yarn-local',
        'yarn.nodemanager.log-dirs': state / 'yarn-logs',
        'yarn.nodemanager.aux-services': 'mapreduce_shuffle',
        'yarn.nodemanager.aux-services.mapreduce_shuffle.class': 'org.apache.hadoop.mapred.ShuffleHandler',
        'yarn.nodemanager.resource.memory-mb': 3072,
        'yarn.nodemanager.resource.cpu-vcores': 2,
        'yarn.scheduler.minimum-allocation-mb': 256,
        'yarn.scheduler.maximum-allocation-mb': 3072,
        'yarn.resourcemanager.scheduler.class': 'org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler',
        'yarn.nodemanager.vmem-check-enabled': 'false',
        'yarn.nodemanager.pmem-check-enabled': 'false',
        'yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage': 95,
        'yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb': 1024,
        'yarn.nodemanager.env-whitelist':
            'JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME,PATH,PYTHONPATH,LANG',
    }
    for name, properties in [('core-site.xml', common), ('hdfs-site.xml', hdfs),
                             ('mapred-site.xml', mapred), ('yarn-site.xml', yarn)]:
        write_xml(conf / name, properties)
        write_xml(hive_conf / name, properties)
    java8 = tools / 'java8'
    env_lines = {
        'JAVA_HOME': java8,
        'HADOOP_PID_DIR': state / 'pids', 'HADOOP_LOG_DIR': state / 'logs' / 'hadoop',
        'HDFS_NAMENODE_OPTS': '-Xmx256m', 'HDFS_DATANODE_OPTS': '-Xmx256m',
        'YARN_RESOURCEMANAGER_HEAPSIZE': '256', 'YARN_NODEMANAGER_HEAPSIZE': '384',
    }
    (conf / 'hadoop-env.sh').write_text(
        '\n'.join(f'export {k}={shlex.quote(str(v))}' for k, v in env_lines.items()) + '\n' +
        'export HADOOP_HEAPSIZE_MAX="${HADOOP_HEAPSIZE_MAX:-384}"\n' +
        'export HADOOP_HEAPSIZE_MIN="${HADOOP_HEAPSIZE_MIN:-64}"\n',
        encoding='utf-8')
    (conf / 'yarn-env.sh').write_text(
        'export YARN_RESOURCEMANAGER_HEAPSIZE=256\nexport YARN_NODEMANAGER_HEAPSIZE=384\n',
        encoding='utf-8')
    write_xml(hive_conf / 'hive-site.xml', {
        'javax.jdo.option.ConnectionURL': f'jdbc:derby:;databaseName={state / "metastore" / "db"};create=true',
        'javax.jdo.option.ConnectionDriverName': 'org.apache.derby.jdbc.EmbeddedDriver',
        'javax.jdo.option.ConnectionUserName': 'APP',
        'javax.jdo.option.ConnectionPassword': 'mine',
        'hive.metastore.schema.verification': 'true',
        'datanucleus.schema.autoCreateAll': 'false',
        'hive.metastore.warehouse.dir': 'hdfs://127.0.0.1:9000/warehouse/managed',
        'hive.metastore.warehouse.external.dir': 'hdfs://127.0.0.1:9000/warehouse/external',
        'hive.execution.engine': 'mr',
        'hive.exec.mode.local.auto': 'false',
        'hive.exec.parallel': 'false',
        'hive.exec.reducers.max': 1,
        'hive.exec.scratchdir': '/tmp/hive',
        'hive.exec.local.scratchdir': state / 'tmp' / 'hive',
        'hive.server2.thrift.bind.host': '127.0.0.1',
        'hive.server2.thrift.port': 10000,
        'hive.server2.authentication': 'NONE',
        'hive.server2.enable.doAs': 'false',
        'hive.server2.thrift.min.worker.threads': 2,
        'hive.server2.thrift.max.worker.threads': 10,
        'hive.server2.webui.host': '127.0.0.1',
        'hive.server2.webui.port': 10002,
        'hive.server2.logging.operation.log.location': state / 'logs' / 'hive' / 'operations',
        'hive.security.authorization.enabled': 'false',
        'hive.support.concurrency': 'false',
        'hive.txn.manager': 'org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager',
        'hive.strict.managed.tables': 'false',
        'hive.create.as.insert.only': 'false',
        'hive.server2.materializedviews.registry.impl': 'DUMMY',
        'hive.stats.autogather': 'false',
        'hive.stats.column.autogather': 'false',
    })


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--state', required=True)
    args = parser.parse_args()
    expected = Path(__file__).resolve().parents[2] / '.codespaces-state'
    if os.environ.get('CODESPACES') != 'true' or Path(args.state).resolve() != expected:
        parser.error('Use the Codespaces setup script and its repository-owned state path')
    configure(expected)
    print('Single-user Hadoop/Hive configuration generated. No services started.')


if __name__ == '__main__':
    main()
