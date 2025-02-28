#!/bin/bash

echo "初始化"

hdfs dfs -rm -r /LAB2/KMedoidsResult
hdfs dfs -rm -r /LAB2/KMedoidsCentroids

echo "开始KMedoids聚类"

# 输入路径 输出路径 K值 最大迭代次数 收敛阈值 PAM抽样个数（如果按照比例抽的话，尝试了10%、1%、0.1%以及0.05%，还是难以在hadoop认为任务超时之前完成计算）
hadoop jar ~/LAB2/BigDataAnalysis.LAB2-4.0-FinalVersion.jar org.KMedoids.KMedoidsJob hdfs:///LAB2/聚类数据.txt hdfs:///LAB2/KMedoidsResult 2 5 0.1 1000

echo "KMedoids聚类完成"

hdfs dfs -ls /LAB2

hdfs dfs -get /LAB2/KMedoidsCentroids ~/LAB2/KMedoids
hdfs dfs -get /LAB2/KMedoidsResult ~/LAB2/KMedoids

# yarn logs -applicationId application_1733723983930_0001 > temp1.log