Embulk::JavaPlugin.register_input(
  "hbase", "org.embulk.input.HBaseInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
