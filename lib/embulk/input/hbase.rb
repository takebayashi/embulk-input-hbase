Embulk::JavaPlugin.register_input(
  "hbase", "org.embulk.input.HbaseInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
