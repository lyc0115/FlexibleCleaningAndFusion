# FlexibleCleaningandFusion
项目描述：
利用科技大数据共性关键技术和科技资源统一数据模型将陕西省各科技资源数据，如科技论文、发明专利、科研团队与机构、科技项目、科研人才、科技成果、科学仪器等相关数据通过数据采集，数据清洗，数据融合，数据关联，数据可视化等阶段处理最终形成科技资源知识图谱，为下一阶段
的科技知识搜索和推荐做数据支撑。 
项目内容：
利用可变清洗规则框架基于MR计算引擎对数据进行清洗、融合和关联，将处理好的中间数据存储于Hbase中，利用中间数据处理后生成的各数据画像文件，通过拼接 Neo4jCQL导入Neo4j中，构建科技资源知识图谱。
大数据平台：
CDH5.14.4（10台）
技术栈：
Sqoop
flume
kafka
habse
hadoop
mr
zookeeper
yarn