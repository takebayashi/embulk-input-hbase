package org.embulk.input;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.embulk.config.CommitReport;
import org.embulk.config.Config;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.exec.PooledBufferAllocator;
import org.embulk.spi.*;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

public class HBaseInputPlugin
        implements InputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("zookeeper")
        public String getZooKeeperHosts();

        @Config("table")
        public String getTableName();

        @Config("start_row")
        public String getStartRow();

        @Config("stop_row")
        public String getStopRow();

        @Config("columns")
        public SchemaConfig getColumns();
    }

    HConnection connect(PluginTask task) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", task.getZooKeeperHosts());
        return HConnectionManager.createConnection(config);
    }

    List<byte[]> regionIds;

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        Schema schema = task.getColumns().toSchema();
        try (HConnection connection = connect(task)) {
            HTable table = (HTable) connection.getTable(task.getTableName());
            List<HRegionLocation> locations = table.getRegionsInRange(Bytes.toBytes(task.getStartRow()), Bytes.toBytes(task.getStopRow()));
            regionIds = locations.stream().map(l -> l.getRegionInfo().getRegionName()).collect(Collectors.toList());
            return resume(task.dump(), schema, regionIds.size(), control);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<CommitReport> successCommitReports)
    {
    }

    @Override
    public CommitReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        try (HConnection connection = connect(task);
             PageBuilder pageBuilder = new PageBuilder(new PooledBufferAllocator(), schema, output);
             HTableInterface table = connection.getTable(task.getTableName())) {
            Scan scan = new Scan();

            // row range
            HRegionInfo regionInfo = connection.locateRegion(regionIds.get(taskIndex)).getRegionInfo();
            byte[] startRow = Bytes.toBytes(task.getStartRow());
            if (Bytes.compareTo(startRow, regionInfo.getStartKey()) < 0) {
                startRow = regionInfo.getStartKey();
            }
            byte[] stopRow = Bytes.toBytes(task.getStopRow());
            if (Bytes.compareTo(regionInfo.getEndKey(), stopRow) < 0) {
                stopRow = regionInfo.getEndKey();
            }
            scan.setStartRow(startRow);
            scan.setStopRow(stopRow);

            for (Result result : table.getScanner(scan)) {
                for (Column column : schema.getColumns()) {
                    String[] token = column.getName().split(":", 2);
                    byte[] family = Bytes.toBytes(token[0]);
                    byte[] qualifier = Bytes.toBytes(token[1]);
                    byte[] value;
                    if (result.containsColumn(family, qualifier)) {
                        value = CellUtil.cloneValue(result.getColumnLatestCell(family, qualifier));
                        Type type = column.getType();
                        if (type.equals(Types.BOOLEAN)) {
                            pageBuilder.setBoolean(column, Bytes.toBoolean(value));
                        }
                        else if (type.equals(Types.DOUBLE)) {
                            pageBuilder.setDouble(column, Bytes.toDouble(value));
                        }
                        else if (type.equals(Types.LONG)) {
                            pageBuilder.setLong(column, Bytes.toLong(value));
                        }
                        else if (type.equals(Types.STRING)) {
                            pageBuilder.setString(column, Bytes.toString(value));
                        }
                    }
                }
                pageBuilder.addRecord();
            }
            pageBuilder.finish();
            return Exec.newCommitReport().setAll(taskSource);
        } catch (IOException e) {
            Exec.getLogger(this.getClass()).error("failed to connect to HBase", e);
            return null;
        }
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        // TODO
        throw new UnsupportedOperationException("'hbase' input plugin does not support guessing.");
        //ConfigDiff diff = Exec.newConfigDiff();
        //diff.set("property1", "value");
        //return diff;
    }
}
