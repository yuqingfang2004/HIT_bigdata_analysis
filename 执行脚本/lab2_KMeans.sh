#!/bin/bash

echo "初始化"

hdfs dfs -rm -r /LAB2/KMeansCentroids
hdfs dfs -rm -r /LAB2/KMeansResult

echo "开始KMeans聚类"

# KMeans算法参数：输入文件路径，输出文件路径，K值，最大迭代次数，收敛阈值
hadoop jar ~/LAB2/BigDataAnalysis.LAB2-4.0-FinalVersion.jar org.KMeans.KMeansJob hdfs:///LAB2/聚类数据.txt hdfs:///LAB2/KMeansResult 2 10 0.1

echo "KMeans聚类已完成，即将获取结果"

hdfs dfs -ls /LAB2

hdfs dfs -get /LAB2/KMeansCentroids ~/LAB2/KMeans
hdfs dfs -get /LAB2/KMeansResult ~/LAB2/KMeans

# yarn logs -applicationId application_1728958313375_0004 > temp1.log
