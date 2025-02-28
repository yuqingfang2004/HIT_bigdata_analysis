#!/bin/bash

# 执行的jar包名称：BigDataAnalysis.LAB1-4.0-FinalVersion.jar

hdfs dfs -rm -r /LAB1/D_Sample
hdfs dfs -rm -r /LAB1/D_Filter
hdfs dfs -rm -r /LAB1/D_Filter1
hdfs dfs -rm -r /LAB1/D_Done

echo "==================================="
echo "即将开始执行 数据抽样"
echo "==================================="
hadoop jar ~/LAB1/BigDataAnalysis.LAB1-4.0-FinalVersion.jar org.Stratified_Sampling.Stratified_Sampling_Job hdfs:///LAB1/data.txt hdfs:///LAB1/D_Sample

echo "==================================="
echo "即将开始执行 数据过滤 以及 数据格式转换"
echo "==================================="
hadoop jar ~/LAB1/BigDataAnalysis.LAB1-4.0-FinalVersion.jar org.CombineFilterAndWashing.CombineFilterAndWashing_Job hdfs:///LAB1/D_Sample/part-r-00000 hdfs:///LAB1/D_Filter


echo "==================================="
echo "即将开始执行 数据清洗"
echo "==================================="
hadoop jar ~/LAB1/BigDataAnalysis.LAB1-4.0-FinalVersion.jar org.Missing_Value_Imputation.Missing_Value_Imputation_Job hdfs:///LAB1/D_Filter/part-r-00000 hdfs:///LAB1/D_Done

echo "==================================="
echo "数据预处理全部完毕，正在下载结果"
echo "==================================="

hdfs dfs -get /LAB1/D_Sample ~/LAB1
hdfs dfs -get /LAB1/D_Filter ~/LAB1
hdfs dfs -get /LAB1/D_Done ~/LAB1