FROM  python:3.9.12-buster 

# パッケージの追加とタイムゾーンの設定
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    tzdata \
&&  ln -sf /usr/share/zoneinfo/Asia/Tokyo /etc/localtime \
&&  apt-get clean \
&&  rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Tokyo

# ワーキングディレクトリの作成とコピー
WORKDIR /kubeflow-tft-sample
COPY ./ ./


# パッケージのinstall
RUN pip install --upgrade pip && \
    pip install pipenv --no-cache-dir && \
    pipenv install