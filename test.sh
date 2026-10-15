#!/bin/bash
# Deploy a Java example as a Managed Service for Apache Flink application on
# LocalStack and check it produces output:
#   S3Sink          generated stock prices land as JSON lines in the sink bucket
#   GettingStarted  records put on the input stream come out of the output stream
# Build the example first, e.g. `mvn -B package -f java/S3Sink/pom.xml`.
# Usage: ./test.sh S3Sink|GettingStarted
set -euo pipefail

EXAMPLE=${1:?usage: ./test.sh S3Sink|GettingStarted}
APP=$(echo "ci-$EXAMPLE" | tr '[:upper:]' '[:lower:]')
REGION=${AWS_DEFAULT_REGION:-us-east-1}
CODE_BUCKET=$APP-code
ROLE=$APP-role
MAX_WAIT_SECONDS=${MAX_WAIT_SECONDS:-600}
# AWS endpoint as seen from the Flink containers LocalStack starts
FLINK_AWS_ENDPOINT=${FLINK_AWS_ENDPOINT:-https://localhost.localstack.cloud:4566}

JAR=$(ls java/"$EXAMPLE"/target/*.jar 2>/dev/null | grep -v '/original-' | head -1 || true)
if [ -z "$JAR" ]; then
  echo "Test failed: no jar in java/$EXAMPLE/target, run mvn package first"
  exit 1
fi

cleanup() {
  created=$(lstk aws kinesisanalyticsv2 describe-application --application-name "$APP" \
    --query ApplicationDetail.CreateTimestamp --output text 2>/dev/null || true)
  if [ -n "$created" ] && [ "$created" != "None" ]; then
    lstk aws kinesisanalyticsv2 stop-application --application-name "$APP" --force > /dev/null 2>&1 || true
    lstk aws kinesisanalyticsv2 delete-application --application-name "$APP" --create-timestamp "$created" > /dev/null 2>&1 || true
  fi
}
# Clean up only after a pass, so a failed run leaves the Flink containers to inspect
trap 'rc=$?; [ "$rc" = 0 ] && cleanup; exit "$rc"' EXIT

echo "Uploading $JAR"
lstk aws s3 mb "s3://$CODE_BUCKET" > /dev/null
lstk aws s3 cp "$JAR" "s3://$CODE_BUCKET/job.jar" > /dev/null

lstk aws iam create-role --role-name "$ROLE" --assume-role-policy-document '{
  "Version": "2012-10-17",
  "Statement": [{"Effect": "Allow", "Principal": {"Service": "kinesisanalytics.amazonaws.com"}, "Action": "sts:AssumeRole"}]
}' > /dev/null
lstk aws iam put-role-policy --role-name "$ROLE" --policy-name "$APP-policy" --policy-document '{
  "Version": "2012-10-17",
  "Statement": [{"Effect": "Allow", "Action": ["s3:*", "kinesis:*"], "Resource": "*"}]
}'

case "$EXAMPLE" in
  S3Sink)
    SINK_BUCKET=$APP-sink
    lstk aws s3 mb "s3://$SINK_BUCKET" > /dev/null
    PROPERTIES="[{\"PropertyGroupId\": \"bucket\", \"PropertyMap\": {\"name\": \"$SINK_BUCKET\"}}]"
    ;;
  GettingStarted)
    INPUT=$APP-input
    OUTPUT=$APP-output
    for stream in "$INPUT" "$OUTPUT"; do
      lstk aws kinesis create-stream --stream-name "$stream" --shard-count 1
      lstk aws kinesis wait stream-exists --stream-name "$stream"
    done
    # The Kinesis connectors call kinesis.<region>.amazonaws.com over HTTPS by default.
    # LocalStack's certificate does not cover that name, so point them at LocalStack.
    # TRIM_HORIZON, so records put before the job is fully up are still read.
    PROPERTIES="[
      {\"PropertyGroupId\": \"InputStream0\", \"PropertyMap\": {\"stream.name\": \"$INPUT\", \"aws.region\": \"$REGION\", \"aws.endpoint\": \"$FLINK_AWS_ENDPOINT\", \"flink.stream.initpos\": \"TRIM_HORIZON\"}},
      {\"PropertyGroupId\": \"OutputStream0\", \"PropertyMap\": {\"stream.name\": \"$OUTPUT\", \"aws.region\": \"$REGION\", \"aws.endpoint\": \"$FLINK_AWS_ENDPOINT\"}}
    ]"
    ;;
  *)
    echo "Unknown example $EXAMPLE, expected S3Sink or GettingStarted"
    exit 1
    ;;
esac

lstk aws kinesisanalyticsv2 create-application \
  --application-name "$APP" \
  --runtime-environment FLINK-1_20 \
  --application-mode STREAMING \
  --service-execution-role "arn:aws:iam::000000000000:role/$ROLE" \
  --application-configuration "{
    \"ApplicationCodeConfiguration\": {
      \"CodeContent\": {\"S3ContentLocation\": {\"BucketARN\": \"arn:aws:s3:::$CODE_BUCKET\", \"FileKey\": \"job.jar\"}},
      \"CodeContentType\": \"ZIPFILE\"
    },
    \"EnvironmentProperties\": {\"PropertyGroups\": $PROPERTIES}
  }" > /dev/null
lstk aws kinesisanalyticsv2 start-application --application-name "$APP"
echo "Started application $APP"

status=""
waited=0
until [ "$status" = "RUNNING" ]; do
  if (( waited >= MAX_WAIT_SECONDS )); then
    echo "Test failed: application $APP is $status after ${MAX_WAIT_SECONDS}s, expected RUNNING"
    exit 1
  fi
  sleep 10
  waited=$((waited + 10))
  status=$(lstk aws kinesisanalyticsv2 describe-application --application-name "$APP" \
    --query ApplicationDetail.ApplicationStatus --output text)
done
echo "PASS: application $APP is RUNNING after ${waited}s"

if [ "$EXAMPLE" = "S3Sink" ]; then
  # The FileSink commits part files on checkpoints, every 20s on LocalStack
  key=""
  waited=0
  until [ -n "$key" ] && [ "$key" != "None" ]; do
    if (( waited >= MAX_WAIT_SECONDS )); then
      echo "Test failed: no output in s3://$SINK_BUCKET/output/ after ${MAX_WAIT_SECONDS}s"
      exit 1
    fi
    sleep 10
    waited=$((waited + 10))
    key=$(lstk aws s3api list-objects-v2 --bucket "$SINK_BUCKET" --prefix output/ \
      --query 'Contents[?Size > `0`] | [0].Key' --output text)
  done
  echo "PASS: the job wrote s3://$SINK_BUCKET/$key"

  records=$(lstk aws s3 cp "s3://$SINK_BUCKET/$key" -)
  count=$(echo "$records" | grep -c . || true)
  if ! echo "$records" | jq -e -s 'length > 0 and all(.[];
      (.ticker | type == "string") and (.price | type == "number") and (.eventTime | type == "number"))' > /dev/null; then
    echo "Test failed: $key does not hold stock price JSON lines:"
    echo "$records" | head -5
    exit 1
  fi
  echo "PASS: $key holds $count stock price records, e.g. $(echo "$records" | head -1)"
  echo "Test passed: the S3Sink application writes generated records to S3"
  exit 0
fi

# GettingStarted: the job copies every input record to the output stream unchanged
MARKER="ci-$(date +%s)"
for i in 1 2 3 4 5; do
  lstk aws kinesis put-record --stream-name "$INPUT" --partition-key "$i" \
    --cli-binary-format raw-in-base64-out --data "{\"marker\": \"$MARKER\", \"seq\": $i}" > /dev/null
done
echo "Put 5 records with marker $MARKER on $INPUT"

shard=$(lstk aws kinesis list-shards --stream-name "$OUTPUT" --query 'Shards[0].ShardId' --output text)
iterator=$(lstk aws kinesis get-shard-iterator --stream-name "$OUTPUT" --shard-id "$shard" \
  --shard-iterator-type TRIM_HORIZON --query ShardIterator --output text)
seen=""
waited=0
while :; do
  response=$(lstk aws kinesis get-records --shard-iterator "$iterator" --output json)
  iterator=$(echo "$response" | jq -r .NextShardIterator)
  for data in $(echo "$response" | jq -r '.Records[].Data'); do
    seen="$seen$(echo "$data" | base64 --decode)"$'\n'
  done
  found=$(echo "$seen" | jq -c --arg m "$MARKER" 'select(.marker == $m) | .seq' 2>/dev/null | sort -un | wc -l | tr -d ' ' || true)
  [ "$found" = "5" ] && break
  if (( waited >= MAX_WAIT_SECONDS )); then
    echo "Test failed: $found of 5 records reached $OUTPUT after ${MAX_WAIT_SECONDS}s"
    exit 1
  fi
  sleep 5
  waited=$((waited + 5))
done
echo "PASS: all 5 records reached $OUTPUT unchanged, e.g. $(echo "$seen" | grep "$MARKER" | head -1)"
echo "Test passed: the GettingStarted application copies records from the input to the output stream"
