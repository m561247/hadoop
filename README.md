# 操作方式
1. 放入指定的 center : hadoop fs -put pm25.center.txt pm25.center.txt
2. 放入指定的資料集 : hadoop fs -put pm25.txt pm25.txt
3. 執行 : hadoop jar output.jar Kmeans pm25.txt pm25.cluster.center.conf.txt kmeans 3
# 執行參數 : 執行程式 資料集 center 結果名稱 執行次數
