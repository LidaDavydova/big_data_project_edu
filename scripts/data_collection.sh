#!/bin/bash
curl -L -o data/data.zip\
  https://www.kaggle.com/api/v1/datasets/download/sturarods/anac-national-civil-aviation-agency-2000-2025

unzip data/data.zip -d data/

rm data/data.zip
