# weatherforecast2bq
- Qiitaに記事投稿済み
[GCP Cloud Functionsで気象庁天気予報情報を定期的にスクレイピングして、BigQueryへinsertする方法](https://qiita.com/shinkoizumi0033/items/a193512912162883c065)
GCP Cloud Functionsで、気象庁が公開している天気予報（東京地域）を、requests/BeautifulSoupを利用して取得/スクレイピングして、取得したデータをBigQueryに格納するコード。
気象庁の天気予報は、時間帯によって、フォームが変わるので、翌日以降を予報するタイミング（前日の11時以降程度）に実行する想定で作成。

1. pubsub経由でFunctionsを実行
2. Functionsからrequestsで、気象庁の天気予報htmlを取得(東京地域)
3. Functionsで、BeautifulSoup4を利用してスクレイピング実施
4. FunctionsからBigQueryへデータをinsert
