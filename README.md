# 概要
TensorFlow TransformとApache Beamを使ったKubeflowコンポーネントの検証用リポジトリである。


# 注意
基本はpipenvによるパッケージのバージョン管理を行う。
パイプラインの起動時に`pip freeze > requrements.txt`を行ってからパイプライン、コンポーネントを起動する。

# Kubeflowからの起動
TBD

memo  
DataflowPythonJobOpは、GCSにDataflowのPythonコードとrequirements.txtファイルを置いておくことが前提になっている。  
そのため実際には、コードが複数ファイルにまたがる場合には利用できず、setup.pyを利用する場合にも使うことができないように思われる。  
対応方法は以下になる。
- Dataflowのコンポーネントを独自に作成する。（他のコンポーネントと同様にコンテナ化）
- kfpからはコンテナを指定して使うようにする。
- [gcp_resources](https://cloud.google.com/vertex-ai/docs/pipelines/build-own-components?hl=ja)を利用する。（パイプラインキャンセル時にDataflowもキャンセルさせるため）


# Dataflowコンポーネントの起動方法
componentsの中にはApache Beamで書かれたコードが存在する。
これらを直接起動するコマンドを記載する。
#TODO それぞれコンテナ化し、コンポーネントとして利用できるようにする。

## wordcount
プロジェクト名、バケット名は適宜変更すること
```
python components/wordcount/wc.py --output gs://ca-pubtex-ai-verification-dataflow/output_wordcount --runner DataflowRunner --project ca-pubtex-ai-verification --region us-central1 --staging_location gs://ca-pubtex-ai-verification-dataflow/staging --temp_location gs://ca-pubtex-ai-verification-dataflow/tmp --job_name=wordcount-job --requirements_file requirements.txt
```

## transform-tft
プロジェクト名、バケット名は適宜変更すること
```
python components/transform-tft/simple_sample.py --output_dir gs://ca-pubtex-ai-verification-dataflow/output_tft --runner DataflowRunner --project ca-pubtex-ai-verification --region us-central1 --staging_location gs://ca-pubtex-ai-verification-dataflow/staging --temp_location gs://ca-pubtex-ai-verification-dataflow/tmp --setup_file components/transform-tft/setup.py
```

