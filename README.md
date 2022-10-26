# 操作方式
1. 放入指定的 center : hadoop fs -put pm25.center.txt pm25.center.txt
2. 放入指定的資料集 : hadoop fs -put pm25.txt pm25.txt
3. 執行 : hadoop jar output.jar Kmeans pm25.txt pm25.center.txt kmeans 3
- 執行參數 : 執行程式 資料集 center 結果名稱 執行次數
# Mapper
- 計算各點與中心之歐式距離
- 以中心為 key，value 為原 text
# Reducer
- 利用 reduce 以中心為 key 將記錄合併
- 計算平均值，求出新的中心
# Setup function
- 需要
- 抓出中心點並設置群數


