package org.embulk.input;

import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.embulk.config.*;
import org.embulk.spi.*;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

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
        @ConfigDefault("null")
        public Optional<String> getStartRow();

        @Config("stop_row")
        @ConfigDefault("null")
        public Optional<String> getStopRow();

        @Config("columns")
        public SchemaConfig getColumns();

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    byte[] getStartRowKey(PluginTask task) {
        if (task.getStartRow().isPresent()) {
            return Bytes.toBytes(task.getStartRow().get());
        }
        return HConstants.EMPTY_START_ROW;
    }

    byte[] getStopRowKey(PluginTask task) {
        if (task.getStopRow().isPresent()) {
            return Bytes.toBytes(task.getStopRow().get());
        }
        return HConstants.EMPTY_END_ROW;
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
            List<HRegionLocation> locations = table.getRegionsInRange(getStartRowKey(task), getStopRowKey(task));
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
             PageBuilder pageBuilder = new PageBuilder(task.getBufferAllocator(), schema, output);
             HTableInterface table = connection.getTable(task.getTableName())) {
            Scan scan = new Scan();

            // row range
            HRegionInfo regionInfo = connection.locateRegion(regionIds.get(taskIndex)).getRegionInfo();
            byte[] startRow = getStartRowKey(task);
            if (Bytes.compareTo(startRow, regionInfo.getStartKey()) < 0) {
                startRow = regionInfo.getStartKey();
            }
            byte[] stopRow = getStopRowKey(task);
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
