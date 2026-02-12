#!/usr/bin/env sh

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=auto
ENDPOINT="http://localhost:9000"

ENDPOINT="http://localhost:9000"
aws s3 mb s3://base --endpoint-url $ENDPOINT

cat > public-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PublicReadGetObject",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::base/*"
    }
  ]
}
EOF

aws s3api put-bucket-policy \
  --bucket base \
  --policy file://public-policy.json \
  --endpoint-url $ENDPOINT

aws s3 cp --recursive "./0000000000/0000000000-0000573739-cd076ab0/" "s3://base/0000000000/0000000000-0000573739-cd076ab0/" --endpoint-url $ENDPOINT
aws s3 cp --recursive "./0000000000/0000573740-0001146939-e632b81c/" "s3://base/0000000000/0000573740-0001146939-e632b81c/" --endpoint-url $ENDPOINT
aws s3 cp --recursive "./0000000000/0001146940-0001543239-148c34fa/" "s3://base/0000000000/0001146940-0001543239-148c34fa/" --endpoint-url $ENDPOINT
aws s3 cp --recursive "./0000000000/0001543240-0001850659-1c5d2eaf/" "s3://base/0000000000/0001543240-0001850659-1c5d2eaf/" --endpoint-url $ENDPOINT
aws s3 cp --recursive "./0000000000/0001850660-0001911641-cf44aa39/" "s3://base/0000000000/0001850660-0001911641-cf44aa39/" --endpoint-url $ENDPOINT
aws s3 cp --recursive "./0001911642/0001911642-0001960839-f7626146/" "s3://base/0001911642/0001911642-0001960839-f7626146/" --endpoint-url $ENDPOINT
aws s3 cp --recursive "./0001911642/0001960840-0001967559-bd2f9293/" "s3://base/0001911642/0001960840-0001967559-bd2f9293/" --endpoint-url $ENDPOINT
aws s3 cp --recursive "./0001911642/0001967560-0001973459-3b5f276f/" "s3://base/0001911642/0001967560-0001973459-3b5f276f/" --endpoint-url $ENDPOINT
aws s3 cp --recursive "./0001911642/0001973460-0001982819-565c28a3/" "s3://base/0001911642/0001973460-0001982819-565c28a3/" --endpoint-url $ENDPOINT
