# Amazon Managed Service for Apache Flink Examples

This repository contains sample applications for [Amazon Managed Service for Apache Flink](https://docs.localstack.cloud/aws/services/kinesisanalyticsv2/), organized by language and operational use cases.

## Prerequisites

- A valid [LocalStack for AWS license](https://localstack.cloud/pricing), which provides a [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/aws/getting-started/auth-token/) required to run these samples with Managed Flink in LocalStack.
- [Docker](https://docs.docker.com/get-docker/) for running LocalStack.
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) (required by `lstk aws`) and [`lstk`](https://docs.localstack.cloud/aws/developer-tools/running-localstack/lstk/), the LocalStack CLI.
- [Java](https://adoptium.net/) and [Maven](https://maven.apache.org/) for Java-based examples.
- [Python](https://www.python.org/downloads/) and `pip` for Python-based examples.

Run the following command before starting LocalStack with your preferred method:

```bash
export LOCALSTACK_AUTH_TOKEN=<your-auth-token>
```

## Samples

- [Java S3 Sink sample](java/S3Sink/README.md)
- [Python Getting Started sample](python/GettingStarted/README.md)
- [All Java examples](java/README.md)
- [All Python examples](python/README.md)
- [Scala getting started example](scala/GettingStarted/README.md)
- [Infrastructure utilities](infrastructure/README.md)

## Testing on LocalStack

`test.sh` deploys a Java example as a Managed Flink application on LocalStack and checks its output. CI runs it for two examples:

- [S3Sink](java/S3Sink/README.md), the example from the [LocalStack Managed Flink guide](https://docs.localstack.cloud/aws/services/kinesisanalyticsv2/): generated stock prices must land as JSON lines in the sink bucket.
- [GettingStarted](java/GettingStarted/README.md): records put on the input Kinesis stream must come out of the output stream unchanged.

```bash
lstk start
mvn -B package -DskipTests -f java/S3Sink/pom.xml
./test.sh S3Sink
```

Applications that use the Kinesis connectors need two extra runtime properties on LocalStack, which `test.sh` sets for GettingStarted:

- `aws.endpoint` set to `https://localhost.localstack.cloud:4566` on each stream's property group. By default the connectors call `kinesis.<region>.amazonaws.com`, and LocalStack's TLS certificate does not cover that name.
- `aws.http.protocol.version` set to `HTTP1_1` on the sink's property group. The sink's AWS SDK v2 client uses HTTP/2 by default, which fails against LocalStack.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This sample code is made available under the MIT-0 license. See [LICENSE](LICENSE).
