# IAM permissions

`watcher sync` (the long-running daemon) and `watcher setup` (one-time provisioning)
intentionally need different, non-overlapping permissions. Attach each to a separate
role/principal if you can -- the daemon never needs to create or modify AWS resources.

## `watcher sync` -- read/receive only

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ssm:GetParametersByPath",
        "ssm:GetParameters",
        "ssm:GetParameter"
      ],
      "Resource": "arn:aws:ssm:REGION:ACCOUNT:parameter/myapp/prod/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:REGION:ACCOUNT:watcher-ssm-events"
    }
  ]
}
```

**Easy to forget:** if `provider.parameter_store.decrypt = true` (the default), the
daemon also needs `kms:Decrypt` on whichever KMS key encrypts your SecureString
parameters -- including `alias/aws/ssm`, the AWS-managed default, if that's what you use.
Without it, `sync` works fine until it hits its first SecureString parameter, then fails
that fetch. Add:

```json
{
  "Effect": "Allow",
  "Action": "kms:Decrypt",
  "Resource": "arn:aws:kms:REGION:ACCOUNT:key/KEY_ID"
}
```

## `watcher setup` -- provisioning only

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:CreateQueue",
        "sqs:SetQueueAttributes",
        "sqs:GetQueueUrl",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:REGION:ACCOUNT:watcher-ssm-events*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "events:PutRule",
        "events:PutTargets",
        "events:DescribeRule",
        "events:ListTargetsByRule"
      ],
      "Resource": "arn:aws:events:REGION:ACCOUNT:rule/watcher-ssm-parameter-change"
    }
  ]
}
```

The `watcher-ssm-events*` resource pattern covers both the main queue and the optional
`-dlq` queue if `setup.dlq.enabled = true`; narrow it to the two exact queue ARNs if you
prefer to avoid the wildcard.

`watcher setup` is meant to be run by an operator (or a one-off CI/deploy step), not by
the daemon itself -- that's the whole reason provisioning is a separate subcommand.
