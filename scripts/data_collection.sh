url="https://disk.yandex.ru/d/xj1Pyfkzko9R_w"

wget "$(yadisk-direct $url)" -O data/data.zip

unzip data/data.zip -d data/
cp data/bg_dataset/*.csv data/
rm -rf data/bg_dataset
rm data/data.zip