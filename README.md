# Amazon Managed Service for Apache Flink Examples

This repository contains sample applications for Amazon Managed Service for Apache Flink, organized by language and operational use cases.

## LocalStack Managed Flink Notes

These samples are intended to run with LocalStack's Managed Service for Apache Flink emulation. Review service docs and current limitations before running:

- https://docs.localstack.cloud/aws/services/kinesisanalyticsv2/
- https://docs.localstack.cloud/aws/services/kinesisanalyticsv2/#limitations

## Prerequisites

- A valid [LocalStack for AWS license](https://localstack.cloud/pricing), which provides a [`LOCALSTACK_AUTH_TOKEN`](https://docs.localstack.cloud/getting-started/auth-token/) required to run these samples with Managed Flink in LocalStack.
- [Docker](https://docs.docker.com/get-docker/) for running LocalStack.
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and [LocalStack CLI](https://docs.localstack.cloud/user-guide/tools/localstack-cli/) (`awslocal`).
- [Java](https://adoptium.net/) and [Maven](https://maven.apache.org/) for Java-based examples.
- [Python](https://www.python.org/downloads/) and `pip` for Python-based examples.

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

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This sample code is made available under the MIT-0 license. See [LICENSE](LICENSE).
