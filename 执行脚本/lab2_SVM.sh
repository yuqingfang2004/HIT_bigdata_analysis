echo "初始化"

# hdfs dfs -rm -r /LAB2/SVMTrain
hdfs dfs -rm -r /LAB2/SVMValidationResult
hdfs dfs -rm -r /LAB2/SVMTestResult

hdfs dfs -ls /LAB2

# echo "开始训练SVM"

# hadoop jar ~/LAB2/BigDataAnalysis.LAB2-4.0-FinalVersion.jar org.SVM.SVMJob hdfs:///LAB2/训练数据.txt hdfs:///LAB2/SVMTrain train 0.005 100

# echo "SVM训练完成"

echo "开始验证SVM"

hadoop jar ~/LAB2/BigDataAnalysis.LAB2-4.0-FinalVersion.jar org.SVM.SVMJob hdfs:///LAB2/验证数据.txt hdfs:///LAB2/SVMValidationResult validate 0.001 10

echo "SVM验证完成"

echo "开始测试SVM"

hadoop jar ~/LAB2/BigDataAnalysis.LAB2-4.0-FinalVersion.jar org.SVM.SVMJob hdfs:///LAB2/测试数据.txt hdfs:///LAB2/SVMTestResult test 0.001 10

echo "SVM测试完成"

echo "正在获取结果"

hdfs dfs -ls /LAB2

hdfs dfs -get /LAB2/SVMTrain ~/LAB2/SVM

hdfs dfs -get /LAB2/SVMValidationResult ~/LAB2/SVM

hdfs dfs -get /LAB2/SVMTestResult ~/LAB2/SVM


# 日志调试调取(调试用)
# yarn logs -applicationId application_1734055325122_0003 > temp1.log